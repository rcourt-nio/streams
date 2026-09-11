import AppKit
import ServiceManagement

// MARK: - Configuration

let streamsRoot = "/Users/ross.court/Workspace/_pvt/streams"
let logDir = NSHomeDirectory() + "/Library/Logs/StreamBar"

struct StreamDef {
    let id: String        // directory, crate, and binary name (all identical)
    let name: String      // display name in the menu
    let buildFlags: String
    let runFlags: String

    var dir: String { "\(streamsRoot)/\(id)" }
}

let streamDefs = [
    StreamDef(id: "count-stream", name: "Counters",
              buildFlags: "", runFlags: "--interval 1000 --no-console"),
    StreamDef(id: "sat-streams", name: "SATs",
              buildFlags: "--features nominal", runFlags: "--interval 500 --no-console"),
    StreamDef(id: "usage-logger", name: "MacBook Metrics",
              buildFlags: "--features nominal", runFlags: "--interval 500 --no-console"),
    StreamDef(id: "flink-streams", name: "Flink Scenarios",
              buildFlags: "", runFlags: "--scenario all --no-console"),
]

let timedDurations: [(label: String, seconds: TimeInterval)] = [
    ("5 minutes", 5 * 60),
    ("10 minutes", 10 * 60),
    ("30 minutes", 30 * 60),
    ("1 hour", 60 * 60),
]

// MARK: - Stream process management

final class StreamController {
    let def: StreamDef
    private(set) var process: Process?
    var deadline: Date?
    var onChange: (() -> Void)?

    init(def: StreamDef) { self.def = def }

    var isRunning: Bool { process?.isRunning ?? false }

    /// Starts the stream (or, if already running, just updates the deadline).
    func start(for duration: TimeInterval? = nil) {
        deadline = duration.map { Date().addingTimeInterval($0) }
        guard !isRunning else {
            onChange?()
            return
        }

        try? FileManager.default.createDirectory(
            atPath: logDir, withIntermediateDirectories: true)
        let logPath = "\(logDir)/\(def.id).log"

        // Rebuild if stale, then exec the release binary directly so this
        // Process IS the stream — terminate() kills it cleanly (no cargo-run
        // wrapper left behind, unlike the start-stream.sh scripts).
        let cmd = """
        export PATH="/opt/homebrew/bin:$HOME/.cargo/bin:/usr/local/bin:$PATH"
        cd '\(def.dir)' || exit 1
        exec >>'\(logPath)' 2>&1
        echo "[streambar] starting \(def.id) at $(date)"
        source ./.env || exit 1
        if command -v cargo >/dev/null 2>&1; then
            cargo build --release \(def.buildFlags) || exit 1
        else
            echo "[streambar] cargo not found on PATH; running existing binary"
        fi
        exec ./target/release/\(def.id) --nominal-token "$NOMINAL_TOKEN" --nominal-dataset "$NOMINAL_DATASET" --nominal-url "$NOMINAL_URL" \(def.runFlags)
        """

        let p = Process()
        p.executableURL = URL(fileURLWithPath: "/bin/bash")
        p.arguments = ["-c", cmd]
        p.terminationHandler = { [weak self] _ in
            DispatchQueue.main.async {
                self?.process = nil
                self?.deadline = nil
                self?.onChange?()
            }
        }
        do {
            try p.run()
            process = p
        } catch {
            NSLog("StreamBar: failed to start \(def.id): \(error)")
            process = nil
            deadline = nil
        }
        onChange?()
    }

    func stop() {
        deadline = nil
        process?.terminate()
        onChange?()
    }

    /// True if a stream binary with this name is running outside our control
    /// (e.g. the old tmux session).
    func isRunningExternally() -> Bool {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: "/usr/bin/pgrep")
        p.arguments = ["-f", "target/release/\(def.id)"]
        let pipe = Pipe()
        p.standardOutput = pipe
        do { try p.run() } catch { return false }
        p.waitUntilExit()
        let out = String(data: pipe.fileHandleForReading.readDataToEndOfFile(),
                         encoding: .utf8) ?? ""
        let pids = out.split(whereSeparator: \.isNewline).compactMap { Int32($0) }
        let own = process?.processIdentifier
        return pids.contains { $0 != own }
    }
}

// MARK: - Menu bar glyph

/// EKG-style pulse line — drawn in code so there's no asset catalog.
/// Template image: the system recolors it for light/dark menu bars, and
/// contentTintColor turns it white on the green "active" background.
func streamGlyph() -> NSImage {
    let img = NSImage(size: NSSize(width: 18, height: 18), flipped: false) { _ in
        NSColor.black.setStroke()
        let p = NSBezierPath()
        p.move(to: NSPoint(x: 1.5, y: 9))
        p.line(to: NSPoint(x: 5.5, y: 9))
        p.line(to: NSPoint(x: 7.5, y: 14.5))
        p.line(to: NSPoint(x: 10.5, y: 3.5))
        p.line(to: NSPoint(x: 12.5, y: 9))
        p.line(to: NSPoint(x: 16.5, y: 9))
        p.lineWidth = 1.7
        p.lineCapStyle = .round
        p.lineJoinStyle = .round
        p.stroke()
        return true
    }
    img.isTemplate = true
    return img
}

// MARK: - App delegate

struct TimedAction {
    let index: Int?  // nil = all streams
    let seconds: TimeInterval
}

final class AppDelegate: NSObject, NSApplicationDelegate, NSMenuDelegate {
    var statusItem: NSStatusItem!
    var controllers: [StreamController] = []
    var streamItems: [NSMenuItem] = []
    var toggleItems: [NSMenuItem] = []
    var startAllItem: NSMenuItem!
    var stopAllItem: NSMenuItem!
    var launchAtLoginItem: NSMenuItem!
    var externalFlags: [Bool] = []
    var tickTimer: Timer?

    func applicationDidFinishLaunching(_ notification: Notification) {
        controllers = streamDefs.map { StreamController(def: $0) }
        externalFlags = controllers.map { _ in false }
        for c in controllers {
            c.onChange = { [weak self] in self?.refresh() }
        }

        // First run: ask for a spot ~300pt from the right edge so the item
        // lands clear of the notch instead of in the hidden overflow area.
        // (macOS remembers the position under this key once the user drags it.)
        let posKey = "NSStatusItem Preferred Position StreamBar"
        if UserDefaults.standard.object(forKey: posKey) == nil {
            UserDefaults.standard.set(300, forKey: posKey)
        }

        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        statusItem.autosaveName = "StreamBar"
        if let button = statusItem.button {
            button.wantsLayer = true
            button.layer?.cornerRadius = 4
            button.image = streamGlyph()
            button.imagePosition = .imageLeading
        }
        statusItem.menu = buildMenu()

        let timer = Timer(timeInterval: 1.0, repeats: true) { [weak self] _ in
            self?.tick()
        }
        RunLoop.main.add(timer, forMode: .common)  // .common so it fires while the menu is open
        tickTimer = timer

        refresh()

        // One-time: enable launch-at-login by default. After this, the menu
        // toggle (or System Settings > Login Items) is the source of truth.
        let autoRegKey = "didAutoRegisterLoginItem"
        if !UserDefaults.standard.bool(forKey: autoRegKey) {
            UserDefaults.standard.set(true, forKey: autoRegKey)
            try? SMAppService.mainApp.register()
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) { [weak self] in
            self?.logDiagnostics()
        }
    }

    func logDiagnostics() {
        var lines: [String] = ["--- StreamBar diagnostics \(Date()) ---"]
        lines.append("launch-at-login status: \(SMAppService.mainApp.status.rawValue) (1 = enabled)")
        if let win = statusItem.button?.window {
            lines.append("status item window frame: \(win.frame)")
            lines.append("window on screen: \(win.isVisible), occlusion visible: \(win.occlusionState.contains(.visible))")
        } else {
            lines.append("status item button has NO window")
        }
        for screen in NSScreen.screens {
            lines.append("screen: \(screen.localizedName) frame=\(screen.frame) safeTop=\(screen.safeAreaInsets.top)")
            if let aux = screen.auxiliaryTopRightArea {
                lines.append("  visible top-right area (right of notch): \(aux)")
            }
        }
        try? FileManager.default.createDirectory(atPath: logDir, withIntermediateDirectories: true)
        if let data = (lines.joined(separator: "\n") + "\n").data(using: .utf8),
           let handle = FileHandle(forWritingAtPath: logDir + "/debug.log") ?? {
               FileManager.default.createFile(atPath: logDir + "/debug.log", contents: nil)
               return FileHandle(forWritingAtPath: logDir + "/debug.log")
           }() {
            handle.seekToEndOfFile()
            handle.write(data)
            try? handle.close()
        }
    }

    func applicationWillTerminate(_ notification: Notification) {
        for c in controllers where c.isRunning {
            c.stop()
        }
    }

    // MARK: Menu construction

    func buildMenu() -> NSMenu {
        let menu = NSMenu()
        menu.delegate = self

        for (i, c) in controllers.enumerated() {
            let item = NSMenuItem(title: c.def.name, action: nil, keyEquivalent: "")
            let sub = NSMenu()

            let toggle = NSMenuItem(title: "Start", action: #selector(toggleStream(_:)),
                                    keyEquivalent: "")
            toggle.target = self
            toggle.tag = i
            sub.addItem(toggle)
            sub.addItem(.separator())
            for d in timedDurations {
                let t = NSMenuItem(title: "Run for \(d.label)",
                                   action: #selector(runTimed(_:)), keyEquivalent: "")
                t.target = self
                t.representedObject = TimedAction(index: i, seconds: d.seconds)
                sub.addItem(t)
            }

            item.submenu = sub
            menu.addItem(item)
            streamItems.append(item)
            toggleItems.append(toggle)
        }

        menu.addItem(.separator())

        startAllItem = NSMenuItem(title: "Start All", action: #selector(startAll),
                                  keyEquivalent: "")
        startAllItem.target = self
        menu.addItem(startAllItem)

        stopAllItem = NSMenuItem(title: "Stop All", action: #selector(stopAll),
                                 keyEquivalent: "")
        stopAllItem.target = self
        menu.addItem(stopAllItem)

        let runAll = NSMenuItem(title: "Run All For", action: nil, keyEquivalent: "")
        let runAllSub = NSMenu()
        for d in timedDurations {
            let t = NSMenuItem(title: d.label, action: #selector(runTimed(_:)),
                               keyEquivalent: "")
            t.target = self
            t.representedObject = TimedAction(index: nil, seconds: d.seconds)
            runAllSub.addItem(t)
        }
        runAll.submenu = runAllSub
        menu.addItem(runAll)

        menu.addItem(.separator())

        let logs = NSMenuItem(title: "Open Logs in Terminal", action: #selector(openLogs),
                              keyEquivalent: "")
        logs.target = self
        menu.addItem(logs)

        launchAtLoginItem = NSMenuItem(title: "Launch at Login",
                                       action: #selector(toggleLaunchAtLogin),
                                       keyEquivalent: "")
        launchAtLoginItem.target = self
        menu.addItem(launchAtLoginItem)

        let quit = NSMenuItem(title: "Quit StreamBar", action: #selector(quit),
                              keyEquivalent: "q")
        quit.target = self
        menu.addItem(quit)

        return menu
    }

    // MARK: Actions

    @objc func toggleStream(_ sender: NSMenuItem) {
        let c = controllers[sender.tag]
        if c.isRunning { c.stop() } else { c.start() }
    }

    @objc func runTimed(_ sender: NSMenuItem) {
        guard let action = sender.representedObject as? TimedAction else { return }
        if let i = action.index {
            controllers[i].start(for: action.seconds)
        } else {
            for c in controllers { c.start(for: action.seconds) }
        }
    }

    @objc func startAll() {
        for c in controllers { c.start() }
    }

    @objc func stopAll() {
        for c in controllers { c.stop() }
    }

    @objc func openLogs() {
        try? FileManager.default.createDirectory(
            atPath: logDir, withIntermediateDirectories: true)
        let p = Process()
        p.executableURL = URL(fileURLWithPath: "/usr/bin/open")
        if FileManager.default.fileExists(atPath: "/Applications/Ghostty.app") {
            p.arguments = ["-na", "Ghostty", "--args", "--working-directory=\(logDir)"]
        } else {
            p.arguments = ["-a", "Terminal", logDir]
        }
        try? p.run()
    }

    @objc func toggleLaunchAtLogin() {
        if SMAppService.mainApp.status == .enabled {
            try? SMAppService.mainApp.unregister()
        } else {
            try? SMAppService.mainApp.register()
        }
        launchAtLoginItem.state = SMAppService.mainApp.status == .enabled ? .on : .off
    }

    @objc func quit() {
        NSApp.terminate(nil)
    }

    // MARK: State refresh

    func menuWillOpen(_ menu: NSMenu) {
        externalFlags = controllers.map { $0.isRunningExternally() }
        launchAtLoginItem.state = SMAppService.mainApp.status == .enabled ? .on : .off
        refresh()
    }

    func tick() {
        let now = Date()
        for c in controllers {
            if let d = c.deadline, d <= now, c.isRunning {
                c.stop()
            }
        }
        refresh()
    }

    func refresh() {
        for (i, c) in controllers.enumerated() {
            var title = c.def.name
            if c.isRunning, let d = c.deadline {
                title += " — \(Self.formatRemaining(d.timeIntervalSinceNow))"
            }
            if externalFlags[i] {
                title += "  (running outside StreamBar)"
            }
            streamItems[i].title = title
            streamItems[i].state = c.isRunning ? .on : .off
            toggleItems[i].title = c.isRunning ? "Stop" : "Start"
        }

        let activeCount = controllers.filter(\.isRunning).count
        startAllItem.isEnabled = activeCount < controllers.count
        stopAllItem.isEnabled = activeCount > 0

        updateStatusButton(activeCount: activeCount)
    }

    func updateStatusButton(activeCount: Int) {
        guard let button = statusItem.button else { return }
        let active = activeCount > 0

        button.layer?.backgroundColor = active ? NSColor.systemGreen.cgColor : nil
        button.contentTintColor = active ? .white : nil

        var text = ""
        if active {
            if let nearest = controllers.compactMap(\.deadline).min() {
                text = " \(Self.formatRemaining(nearest.timeIntervalSinceNow))"
            } else if activeCount < controllers.count {
                text = " \(activeCount)"
            }
        }
        button.attributedTitle = NSAttributedString(string: text, attributes: [
            .foregroundColor: active ? NSColor.white : NSColor.labelColor,
            .font: NSFont.monospacedDigitSystemFont(
                ofSize: NSFont.systemFontSize(for: .small), weight: .medium),
        ])
    }

    static func formatRemaining(_ interval: TimeInterval) -> String {
        let s = max(0, Int(interval.rounded()))
        if s >= 3600 {
            return String(format: "%d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60)
        }
        return String(format: "%d:%02d", s / 60, s % 60)
    }
}

// MARK: - Entry point

let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.setActivationPolicy(.accessory)
app.run()
