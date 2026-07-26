# Changelog

## 0.1.2 - 2026-07-26

- Raised a structured `DataNetError` when an authentication response succeeds
  without returning a usable JWT.

## 0.1.1 - 2026-07-17

- Added `get_presence(channel)` for authoritative occupancy and member lookups.

## 0.1.0 - 2026-06-12

Initial public release.

- Added async DataNet WebSocket client.
- Added sync/background-thread helpers.
- Added JSON publish and subscribe support.
- Added binary publish and subscribe support.
- Added DMX and Art-Net helper utilities.
- Added runnable examples for JSON, p5-style coordinates, and binary DMX.
- Added pytest coverage for client behavior, errors, and binary helpers.
