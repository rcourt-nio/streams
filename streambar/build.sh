#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

APP=StreamBar.app

# Quit a running instance so the binary isn't busy while we overwrite it.
osascript -e 'tell application "StreamBar" to quit' 2>/dev/null || true

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS"
cp Info.plist "$APP/Contents/"
swiftc -O -o "$APP/Contents/MacOS/StreamBar" StreamBar.swift
codesign --force --sign - "$APP"

echo "Built $APP — launch with: open $(pwd)/$APP"
