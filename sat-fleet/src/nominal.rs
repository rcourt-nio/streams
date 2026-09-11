use crate::config::Preset;
use crate::satgen::SatelliteConfig;
use anyhow::{Context, Result, anyhow};
use conjure_runtime_rustls_platform_verifier::PlatformVerifierClient;
use futures::StreamExt;
use nominal_streaming::api as napi;
use nominal_streaming::client::async_conjure_client;
use nominal_streaming::client::conjure::http::client::AsyncService;
use nominal_streaming::client::conjure::object::SafeLong;
use nominal_streaming::prelude::{BearerToken, ResourceIdentifier};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use url::Url;

use napi::api::rids::DatasetRid;
use napi::api::{Label, PropertyName, PropertyValue, TagName, TagValue, Token};
use napi::scout::RunServiceAsyncClient;
use napi::scout::api::DataSourceRefName;
use napi::scout::asset::api::{
    AssetSortField, AssetSortOptions, CreateAssetDataScope, CreateAssetRequest,
    SearchAssetsQuery, SearchAssetsRequest,
};
use napi::scout::assets::AssetServiceAsyncClient;
use napi::scout::rids::api::{AssetRid, PropertiesFilter};
use napi::scout::run::api::{
    CreateRunRequest, DataSource, RunRid, UpdateRunRequest, UtcTimestamp,
};

const DATA_SCOPE_NAME: &str = "telemetry";
const BATCH_PROPERTY: &str = "batch";
const CREATE_CONCURRENCY: usize = 24;
const SEARCH_PAGE_SIZE: i32 = 500;

pub struct EnvConfig {
    pub token: String,
    pub dataset: String,
    pub base_url: String,
}

impl EnvConfig {
    pub fn from_env() -> Result<Self> {
        let get = |key: &str| {
            std::env::var(key).with_context(|| format!("missing env var {key} (set it in .env)"))
        };
        Ok(Self {
            token: get("NOMINAL_TOKEN")?,
            dataset: get("NOMINAL_DATASET")?,
            base_url: get("NOMINAL_URL")?,
        })
    }
}

#[derive(Clone)]
pub struct NominalApi {
    token: BearerToken,
    dataset_rid: ResourceIdentifier,
    assets: AssetServiceAsyncClient<PlatformVerifierClient>,
    runs: RunServiceAsyncClient<PlatformVerifierClient>,
}

fn conj_err(error: impl std::fmt::Debug) -> anyhow::Error {
    anyhow!("{error:?}")
}

impl NominalApi {
    pub fn new(env: &EnvConfig) -> Result<Self> {
        let token = BearerToken::new(&env.token).context("invalid NOMINAL_TOKEN")?;
        let dataset_rid = ResourceIdentifier::new(&env.dataset)
            .with_context(|| format!("NOMINAL_DATASET is not a valid rid: {}", env.dataset))?;
        let url: Url = env.base_url.parse().context("invalid NOMINAL_URL")?;
        let client = async_conjure_client("sat-fleet", url).map_err(conj_err)?;
        Ok(Self {
            token,
            dataset_rid,
            assets: AssetServiceAsyncClient::new(client.clone()),
            runs: RunServiceAsyncClient::new(client),
        })
    }

    /// Finds existing assets for the batch and creates any that are missing.
    /// Idempotent: re-running provisions only the gap. Never touches assets
    /// belonging to other batches.
    pub async fn provision_assets(
        &self,
        preset: &Preset,
        common_label: &str,
        sats: &[SatelliteConfig],
        progress: impl Fn(ProvisionProgress) + Send + Sync,
    ) -> Result<ProvisionOutcome> {
        let mut name_to_rid = self.search_batch_assets(&preset.name).await?;
        let missing: Vec<&SatelliteConfig> = sats
            .iter()
            .filter(|s| !name_to_rid.contains_key(&s.name))
            .collect();
        let existing = sats.len() - missing.len();

        progress(ProvisionProgress {
            total: sats.len(),
            existing,
            created: 0,
            failed: 0,
        });

        let created_count = AtomicUsize::new(0);
        let failed_count = AtomicUsize::new(0);
        let create_futures: Vec<_> = missing
            .iter()
            .map(|sat| {
                let sat = (*sat).clone();
                let created_count = &created_count;
                let failed_count = &failed_count;
                let progress = &progress;
                let preset = &preset;
                async move {
                    let result = self
                        .create_asset(&preset.name, &preset.label, common_label, &sat)
                        .await
                        .map(|rid| (sat.name.clone(), rid))
                        .map_err(|e| format!("{}: {e:#}", sat.name));
                    match &result {
                        Ok(_) => created_count.fetch_add(1, Ordering::Relaxed),
                        Err(_) => failed_count.fetch_add(1, Ordering::Relaxed),
                    };
                    progress(ProvisionProgress {
                        total: sats.len(),
                        existing,
                        created: created_count.load(Ordering::Relaxed),
                        failed: failed_count.load(Ordering::Relaxed),
                    });
                    result
                }
            })
            .collect();
        let results: Vec<Result<(String, AssetRid), String>> =
            futures::stream::iter(create_futures)
                .buffer_unordered(CREATE_CONCURRENCY)
                .collect()
                .await;

        let mut created = 0;
        let mut failed = 0;
        let mut first_error = None;
        for result in results {
            match result {
                Ok((name, rid)) => {
                    created += 1;
                    name_to_rid.insert(name, rid);
                }
                Err(e) => {
                    failed += 1;
                    first_error.get_or_insert(e);
                }
            }
        }

        // Asset rids in satellite order, for the run's asset list.
        let mut asset_rids: Vec<AssetRid> = sats
            .iter()
            .filter_map(|s| name_to_rid.get(&s.name).cloned())
            .collect();

        // Per-batch logs asset: log.system entries are untagged (the logs
        // write API has no tag support), so they can't fall inside any
        // satellite's tag-filtered data scope. This asset exposes the
        // dataset unfiltered, making logs reachable from the run — runs
        // reject raw data sources alongside assets
        // (Scout:BothAssetAndDataSourcesSpecified).
        let logs_title = format!("{} logs", preset.sat_prefix);
        match name_to_rid.get(&logs_title) {
            Some(rid) => asset_rids.push(rid.clone()),
            None => match self
                .create_logs_asset(&preset.name, &preset.label, common_label, &logs_title)
                .await
            {
                Ok(rid) => {
                    created += 1;
                    asset_rids.push(rid);
                }
                Err(e) => {
                    failed += 1;
                    first_error.get_or_insert(format!("{logs_title}: {e:#}"));
                }
            },
        }

        Ok(ProvisionOutcome {
            asset_rids,
            existing,
            created,
            failed,
            first_error,
        })
    }

    async fn search_batch_assets(&self, batch: &str) -> Result<BTreeMap<String, AssetRid>> {
        let mut out = BTreeMap::new();
        let mut page_token: Option<Token> = None;
        loop {
            let query = SearchAssetsQuery::Properties(
                PropertiesFilter::builder()
                    .name(PropertyName(BATCH_PROPERTY.to_string()))
                    .values([PropertyValue(batch.to_string())])
                    .build(),
            );
            let mut builder = SearchAssetsRequest::builder()
                .sort(
                    AssetSortOptions::builder()
                        .is_descending(false)
                        .field(AssetSortField::CreatedAt)
                        .build(),
                )
                .query(query)
                .page_size(SEARCH_PAGE_SIZE);
            if let Some(token) = &page_token {
                builder = builder.next_page_token(token.clone());
            }
            let response = self
                .assets
                .search_assets(&self.token, &builder.build())
                .await
                .map_err(conj_err)
                .context("searching batch assets")?;

            for asset in response.results() {
                out.insert(asset.title().to_string(), asset.rid().clone());
            }
            match response.next_page_token() {
                Some(token) => page_token = Some(token.clone()),
                None => break,
            }
        }
        Ok(out)
    }

    async fn create_asset(
        &self,
        batch: &str,
        batch_label: &str,
        common_label: &str,
        sat: &SatelliteConfig,
    ) -> Result<AssetRid> {
        let scope = CreateAssetDataScope::builder()
            .data_scope_name(DataSourceRefName(DATA_SCOPE_NAME.to_string()))
            .data_source(DataSource::Dataset(DatasetRid(self.dataset_rid.clone())))
            .series_tags([(
                TagName("satellite".to_string()),
                TagValue(sat.name.clone()),
            )])
            .build();
        let request = CreateAssetRequest::builder()
            .title(sat.name.as_str())
            .properties([
                (
                    PropertyName(BATCH_PROPERTY.to_string()),
                    PropertyValue(batch.to_string()),
                ),
                (
                    PropertyName("norad_id".to_string()),
                    PropertyValue(sat.norad_id.to_string()),
                ),
                (
                    PropertyName("category".to_string()),
                    PropertyValue(sat.category.as_str().to_string()),
                ),
            ])
            .labels([
                Label(common_label.to_string()),
                Label(batch_label.to_string()),
            ])
            .data_scopes([scope])
            .build();
        let asset = self
            .assets
            .create_asset(&self.token, &request)
            .await
            .map_err(conj_err)?;
        Ok(asset.rid().clone())
    }

    async fn create_logs_asset(
        &self,
        batch: &str,
        batch_label: &str,
        common_label: &str,
        title: &str,
    ) -> Result<AssetRid> {
        let scope = CreateAssetDataScope::builder()
            .data_scope_name(DataSourceRefName("logs".to_string()))
            .data_source(DataSource::Dataset(DatasetRid(self.dataset_rid.clone())))
            .build();
        let request = CreateAssetRequest::builder()
            .title(title)
            .properties([
                (
                    PropertyName(BATCH_PROPERTY.to_string()),
                    PropertyValue(batch.to_string()),
                ),
                (
                    PropertyName("role".to_string()),
                    PropertyValue("logs".to_string()),
                ),
            ])
            .labels([
                Label(common_label.to_string()),
                Label(batch_label.to_string()),
            ])
            .data_scopes([scope])
            .build();
        let asset = self
            .assets
            .create_asset(&self.token, &request)
            .await
            .map_err(conj_err)?;
        Ok(asset.rid().clone())
    }

    pub async fn create_run(
        &self,
        preset: &Preset,
        common_label: &str,
        asset_rids: Vec<AssetRid>,
        start_unix: i64,
        debug: bool,
    ) -> Result<RunInfo> {
        let title = format!(
            "{}{} @ {}",
            preset.name,
            if debug { " [debug]" } else { "" },
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        let request = CreateRunRequest::builder()
            .title(title.as_str())
            .description(format!(
                "sat-fleet streaming session: {} satellites @ {} Hz",
                preset.count, preset.rate_hz
            ))
            .start_time(UtcTimestamp::new(SafeLong::try_from(start_unix)?))
            .properties([
                (
                    PropertyName(BATCH_PROPERTY.to_string()),
                    PropertyValue(preset.name.clone()),
                ),
                (
                    PropertyName("satellites".to_string()),
                    PropertyValue(preset.count.to_string()),
                ),
                (
                    PropertyName("rate_hz".to_string()),
                    PropertyValue(preset.rate_hz.to_string()),
                ),
                (
                    PropertyName("debug".to_string()),
                    PropertyValue(debug.to_string()),
                ),
            ])
            .labels([
                Label(common_label.to_string()),
                Label(preset.label.clone()),
            ])
            .assets(asset_rids)
            .build();
        let run = self
            .runs
            .create_run(&self.token, &request)
            .await
            .map_err(conj_err)
            .context("creating run")?;
        Ok(RunInfo {
            rid: run.rid().clone(),
            title,
            start_unix,
        })
    }

    pub async fn end_run(&self, rid: &RunRid, end_unix: i64) -> Result<()> {
        let request = UpdateRunRequest::builder()
            .end_time(UtcTimestamp::new(SafeLong::try_from(end_unix)?))
            .build();
        self.runs
            .update_run(&self.token, rid, &request)
            .await
            .map_err(conj_err)
            .context("ending run")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProvisionProgress {
    pub total: usize,
    pub existing: usize,
    pub created: usize,
    pub failed: usize,
}

pub struct ProvisionOutcome {
    pub asset_rids: Vec<AssetRid>,
    pub existing: usize,
    pub created: usize,
    pub failed: usize,
    pub first_error: Option<String>,
}

#[derive(Clone)]
pub struct RunInfo {
    pub rid: RunRid,
    pub title: String,
    pub start_unix: i64,
}

/// Builds the point-streaming client targeting the shared dataset. Separate
/// from `NominalApi` because the stream owns background flush threads and is
/// created/dropped per streaming session.
pub fn build_stream(
    env: &EnvConfig,
    handle: tokio::runtime::Handle,
    listener: std::sync::Arc<dyn nominal_streaming::listener::NominalStreamListener>,
) -> Result<nominal_streaming::stream::NominalDatasetStream> {
    use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};
    let token = BearerToken::new(&env.token).context("invalid NOMINAL_TOKEN")?;
    let dataset_rid =
        ResourceIdentifier::new(&env.dataset).context("NOMINAL_DATASET is not a valid rid")?;
    Ok(NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset_rid, handle)
        .with_options(NominalStreamOpts {
            base_api_url: env.base_url.clone(),
            // Measured (examples/throughput.rs): with ~250ms round trips to
            // staging, the default 100ms/4-buffered pipeline oscillates hard
            // at 50k pts/s. Larger batches + a deeper request queue hold a
            // steady ~100k pts/s with headroom to ~150-180k.
            max_request_delay: std::time::Duration::from_millis(250),
            max_buffered_requests: 16,
            ..NominalStreamOpts::default()
        })
        .add_listener(listener)
        .build())
}

/// Writes entries to the dataset's `log.system` channel via the storage
/// writer HTTP endpoint (same approach as sat-streams).
pub struct LogWriter {
    client: reqwest::blocking::Client,
    url: String,
    token: String,
}

impl LogWriter {
    pub fn new(env: &EnvConfig) -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
            url: format!("{}/storage/writer/v1/logs/{}", env.base_url, env.dataset),
            token: env.token.clone(),
        }
    }

    pub fn write(&self, message: &str, args: &[(&str, String)]) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let args: BTreeMap<&str, &str> =
            args.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let body = serde_json::json!({
            "logs": [{
                "timestamp": { "seconds": now.as_secs(), "nanos": now.subsec_nanos(), "picos": null },
                "value": { "message": message, "args": args },
            }],
            "channel": "log.system",
        });
        let response = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", self.token))
            .json(&body)
            .send()?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().unwrap_or_default();
            anyhow::bail!("log write failed ({status}): {text}");
        }
        Ok(())
    }
}
