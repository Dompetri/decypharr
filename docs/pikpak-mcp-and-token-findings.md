# PikPak MCP Endpoint & Token Authentication Findings

## Executive Summary

PikPak provides an official Model Context Protocol (MCP) server at `https://pikpak.ai/mcp` intended for AI agent integrations. Analysis and live testing revealed two critical findings:

1. **Full MCP Feature Suite**: The MCP server exposes **20 specialized tools** over JSON-RPC 2.0 (Streamable HTTP with `mcp-session-id`), covering cloud downloads (`add_link`, `task_get`, `task_ls`, `task_rm`), streaming (`get` with `url_type: "all"` producing direct CDN URLs), file management (`ls`, `cp`, `mv`, `rm`), and account quotas (`account_info`).
2. **Direct REST API Bypass**: The 90-day OAuth2 Bearer token issued for the MCP client (`aud: ald7wCTsXvAC9M3F`, scopes: `files:read files:write files:manage cloud_download share`) can be used **directly** against the standard PikPak REST Drive API (`https://api-drive.mypikpak.com`) **without requiring any captcha tokens, mobile device signatures, or username/password logins**.

---

## 1. The MCP Server Architecture (`https://pikpak.ai/mcp`)

### Protocol Specifications
- **Protocol**: JSON-RPC 2.0 over HTTP (MCP spec version `2024-11-05`).
- **Transport**: POST / GET with `mcp-session-id` session header.
- **Authentication**: `Authorization: Bearer <token>` in HTTP headers.
- **User-Agent Requirement**: Cloudflare gates requests from standard libraries like `Python-urllib` (Error 1010); requests must present a client User-Agent header (e.g. `curl/8.4.0` or standard agent).

### Session Lifecycle
1. **Initialize (`method: "initialize"`)**:
   - Client sends capabilities and client info.
   - Server returns protocol version, server info (`pikpak-mcp-server 1.0.0`), capabilities, and a response header:
     `mcp-session-id: mcp-session-<uuid>`
2. **Initialized Notification (`method: "notifications/initialized"`)**:
   - Sent by client with `mcp-session-id`.
3. **Tool Invocation (`method: "tools/call"`)**:
   - Standard MCP tool invocation with `name` and `arguments`.

---

## 2. MCP Tools Inventory

| Tool | Category | Input Parameters | Description |
|---|---|---|---|
| `account_info` | Account | None | Returns user ID, name, email, VIP status/expiration, storage quota. |
| `add_link` | Downloads | `url` (required), `parent` | Submits magnet links (`magnet:?xt=...`) or HTTP links for cloud download. |
| `task_get` | Downloads | `id` (required) | Fetches real-time status (`PHASE_TYPE_RUNNING`, `PHASE_TYPE_COMPLETE`), file ID, size, and progress. |
| `task_ls` | Downloads | `phase`, `type`, `limit`, `token` | Lists pending, running, error, or completed download tasks. |
| `task_rm` | Downloads | `ids` (array, required), `delete-files` | Deletes download tasks and optionally removes associated downloaded drive files. |
| `get` | Files / Stream | `id` (required), `url_type` (`"download"`, `"play"`, `"all"`), `raw` | Fetches file metadata. Generates direct CDN high-speed download & play links. |
| `ls` | Files | `parent_id`, `limit`, `raw`, `type`, `token` | Lists folder contents. `raw: true` returns the full drive API object with hashes & video duration. |
| `mkdir` | Files | `name` (required), `parent` | Creates a directory. |
| `mv` / `cp` / `rm` | Files | `id`, `target` / `ids` | Moves, copies, or deletes files and folders. |
| `share_*` | Sharing | Various | Manages PikPak share links (`share_create`, `share_get`, `share_ls`, `share_restore`, `share_rm`). |

---

## 3. Live Test Validation

Using the provided test token, live end-to-end tests succeeded:

1. **Offline Download (`add_link`)**:
   - Magnet submitted: `1992 (2024) 1080p BluRay DDP7 1 x264-ZoroSenpai`.
   - Result: Created task `VP6N-INN75xWZe8C_y6wqJQso2`.
2. **Status Polling (`task_get`)**:
   - Task completed in ~6 seconds with status `Complete (PHASE_TYPE_COMPLETE)`.
   - Resulting drive folder ID: `VP6N-INV75xWZe8C_y6wqJR1o2`.
3. **Directory Listing (`ls`)**:
   - Identified 11.8 GB MKV video: `1992 2024 1080p BluRay DDP7 1 x264-ZoroSenpai.mkv`.
   - File ID: `VP6N-INV75xWZe8C_y6wqJR5o2`.
4. **Stream URL Resolution (`get`)**:
   - Returned direct download URL:
     `http://dl-z01a-0041.mypikpak.com/download/?fid=...`
   - Valid direct HTTP range-capable streaming URL.

---

## 4. REST API Direct Compatibility (Option A)

Decoding the test JWT payload:
```json
{
  "iss": "https://user.mypikpak.com",
  "sub": "aveu-lFOZDjLYpOJ",
  "aud": "ald7wCTsXvAC9M3F",
  "exp": 1802400730,
  "iat": 1794624730,
  "scope": "files:read files:write files:manage cloud_download share"
}
```

Direct validation against official endpoints:
- `POST https://api-drive.mypikpak.com/drive/v1/files`: **HTTP 200 OK without captcha**.
- `GET https://api-drive.mypikpak.com/drive/v1/privilege/vip`: **HTTP 200 OK** (type: `platinum`).
- `GET https://api-dav.mypikpak.com/webdav/v1/applications`: **HTTP 200 OK** (WebDAV credentials returned).
