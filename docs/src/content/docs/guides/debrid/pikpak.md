---
title: PikPak Setup
description: Configure PikPak provider.
---

[PikPak](https://mypikpak.com/) is a supported provider. Unlike a traditional
debrid service, PikPak is a cloud drive with an **offline download** feature:
magnets are downloaded into your PikPak account and then streamed by decypharr.

## Credentials

PikPak supports two authentication methods in the `api_key` field:

### 1. Token / Bearer Authentication (Recommended)
You can directly paste an OAuth2 JWT token (e.g., from PikPak AI / MCP):

```json
{
  "debrids": [
    {
      "provider": "pikpak",
      "name": "PikPak",
      "api_key": "eyJhbGciOi...",
      "download_uncached": true
    }
  ]
}
```
*Note: You can also use `Bearer <token>` or `token:<token>` prefix.*

### 2. Username and Password Authentication
Alternatively, authenticate using your account **username and password** in `username:password` format. Your username may be the email or phone number you registered with:

```json
{
  "debrids": [
    {
      "provider": "pikpak",
      "name": "PikPak",
      "api_key": "you@example.com:your_password",
      "download_uncached": true
    }
  ]
}
```

## Download Uncached is required

PikPak has no instant "is this hash cached?" check — every magnet is queued as
an offline download. You must therefore enable **Download Uncached** so
decypharr will accept magnets that are not already in your drive. The setup
wizard turns this on automatically when you pick PikPak.

Downloads complete asynchronously: decypharr submits the magnet, then polls the
task until PikPak finishes transferring it to your account, at which point the
files become available for streaming.

## Notes

- A stable device id is derived from your credentials, so you won't trigger a
  new-device verification on every restart.
- Access tokens are refreshed automatically; you only ever supply your
  username and password.
- The repair sweep relies on download-time link validation for PikPak, since
  there is no per-file availability probe.

All other configuration options from [Real Debrid](./real-debrid/) apply (rate
limits, workers, proxy, etc.).

See [Configuration Reference](../configuration/#debrid-providers) for full options.
