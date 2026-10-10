# Mavio Cloud Storage Providers (v1.75.2-mavio)

> English | [简体中文](MAVIO_PROVIDERS.zh-CN.md)

This fork of Rclone `v1.75.2` adds backends for eight Chinese cloud storage providers: 115 Cloud, 123 Cloud, GuangYa Drive, Xunlei Drive (Thunder), Baidu Netdisk, Quark Drive / UC Drive, Aliyun Drive and Tencent Weiyun. Their API knowledge comes from [AList](https://github.com/alist-org/alist) and its community fork [OpenList](https://github.com/OpenListTeam/OpenList); the implementations follow Rclone's backend conventions (`lib/rest`, `lib/pacer`, `lib/dircache`, `lib/encoder`).

It also makes serving remote objects through the rc (`--rc-serve`) cheaper for every backend (§7), and adds a cloud-side offline download interface (`fs.OfflineDownloader`) implemented by 115 Cloud, 123 Cloud, GuangYa Drive, Xunlei Drive and PikPak.

The backends have unit tests only. They have not yet been run against the live services with Rclone's integration test suite, so treat them as untested.

Per-backend reference documentation, including every option, is in `docs/content/<backend>.md`.

---

## Table of Contents
- [1. Design Principles (Comparison with AList)](#1-design-principles-comparison-with-alist)
- [2. Provider Naming & Access Methods](#2-provider-naming--access-methods)
- [3. Backends](#3-backends)
  - [3.1 Aliyun Drive (`aliyundrive`)](#31-aliyun-drive-aliyundrive)
  - [3.2 123 Cloud (`cloud123`)](#32-123-cloud-cloud123)
  - [3.3 GuangYa Drive (`guangyadrive`)](#33-guangya-drive-guangyadrive)
  - [3.4 115 Cloud (`cloud115`)](#34-115-cloud-cloud115)
  - [3.5 Xunlei Drive (`thunder`)](#35-xunlei-drive-thunder)
  - [3.6 Baidu Netdisk (`baidunetdisk`)](#36-baidu-netdisk-baidunetdisk)
  - [3.7 Quark Drive / UC Drive (`quark`)](#37-quark-drive--uc-drive-quark)
  - [3.8 Tencent Weiyun (`weiyun`)](#38-tencent-weiyun-weiyun)
- [4. Retries, Uploads & Caching](#4-retries-uploads--caching)
- [5. Configuration & Command Examples](#5-configuration--command-examples)
- [6. Implementation Notes](#6-implementation-notes)
- [7. Serving Remote Objects](#7-serving-remote-objects)

---

## 1. Design Principles (Comparison with AList)

| Aspect | AList | This fork |
| :--- | :--- | :--- |
| **Naming** | Related services are separate drivers (e.g. Quark vs UC). | English backend names (`cloud115`, `cloud123`, `guangyadrive`, `baidunetdisk`, …) with aliases; related services are modes of one backend (`quark` with `mode = uc`). |
| **Upload buffering** | Often caches the whole file in a temporary file (`CacheFullInTempFile`) before uploading. | Streams when the provider's required hashes can be obtained from the source up front (a local file computes them by reading the file); otherwise spools to a temporary file. GuangYa and Aliyun normally stream; Weiyun always needs random access (see §4). Whole files are never buffered in memory, except Weiyun files up to 1 MiB. |
| **Data path** | Served through AList's own WebDAV/HTTP layer. | Rclone talks to the provider's API and object storage (S3/OSS/CDN) directly. |
| **Modification times** | Mostly replaced by the upload time. | Aliyun Drive stores the source modification time (millisecond precision). Baidu receives it at upload but it can't be changed later. The other providers don't support it, and the backends say so (`fs.ModTimeNotSupported`) instead of pretending. |
| **Rate limiting** | Static limiters. | `lib/pacer` exponential backoff on HTTP 429/5xx, network errors and provider-specific throttling codes; token/cookie renewal on expiry. |
| **Rclone semantics** | — | `Rmdir` refuses non-empty directories, `Purge` deletes recursively, overwrites replace the old file instead of leaving duplicates, `DirMove` returns `ErrorDirExists` when the destination exists. |

---

## 2. Provider Naming & Access Methods

| Provider | Backend (`type`) | Aliases | Login |
| :--- | :--- | :--- | :--- |
| **115 Cloud** | `cloud115` | `115`, `115drive` | ① Cookie (`UID`, `CID`, `SEID`, `KID`)<br>② QR code login in `rclone config` (`qrcode_source` selects the device type) |
| **123 Cloud** | `cloud123` | `123`, `123cloud`, `123drive` | ① Phone/email and password<br>② Token (`token`), renewed by logging in again |
| **GuangYa Drive** | `guangyadrive` | `guangya`, `guangyacloud` | Refresh token (`refresh_token`), renewed automatically |
| **Xunlei Drive** | `thunder` | `xunlei`, `thunderdrive` | ① Username and password (as an Android client)<br>② Refresh token |
| **Baidu Netdisk** | `baidunetdisk` | `baidudrive` | Open platform OAuth2 refresh token, with a built-in or custom client ID |
| **Quark / UC Drive** | `quark` | `quarkdrive`, `ucdrive` | Browser cookie (`__puus`); `mode = quark` or `uc` |
| **Aliyun Drive** | `aliyundrive` | `aliyun`, `alibabadrive` | Open Platform refresh token, renewed through OpenList's service or your own app (see §3.1) |
| **Tencent Weiyun** | `weiyun` | `tencentweiyun` | Browser cookies (`uin`, `skey`, `p_skey`) |

---

## 3. Backends

### 3.1 Aliyun Drive (`aliyundrive`)
* **Authentication**: refresh token. With `use_online_api = true` (the default) access tokens are renewed through `oauth_token_url`, by default OpenList's `https://api.oplist.org/alicloud/renewapi`. Refresh tokens issued by its QR code tool belong to that service's Open Platform application, so only that service can renew them. **The refresh token is sent to this third-party service.** `alipan_type = alipanTV` selects tokens from the TV login. With your own application set `client_id`, `client_secret` and `use_online_api = false`; tokens are then renewed only with `openapi.alipan.com`, and the user in the new token is checked against the old one. The rotated token is saved to the config.
* **Uploads**: streamed in `chunk_size` parts (default 20 MiB, raised to stay within 10,000 parts) with one part in memory. Each part is retried, and part URLs are refreshed after 50 minutes or on HTTP 403. A temporary file is only used when the size is unknown, or when rapid upload needs the SHA1 and the source can't provide it.
* **Rapid upload** (`rapid_upload`, off by default, files over 100 KiB): offers the source SHA1 plus an 8-byte proof code read from a range of the source; without a SHA1 it first sends a 1 KiB pre-hash.
* **Hashes**: SHA1. **Modification times**: stored as `local_modified_at`.
* **Options**: `drive_type` (`resource`, `backup`, `default`), `drive_id`, `root_folder_id`, `remove_way` (`trash` or `delete`), `chunk_size`, `encoding`.
* Copy and Move work within one drive only.

---

### 3.2 123 Cloud (`cloud123`)
* **Authentication**: `user` and `pass`, or `token`. Expired tokens are renewed by logging in again.
* **Uploads**: 123 needs the MD5 first. If the source provides it, the file is streamed in `chunk_size` parts (5–16 MiB, default 16 MiB) to presigned S3 URLs, each part retried and its URL refreshed on HTTP 403. Otherwise the input is spooled to a temporary file while hashing.
* **Rapid upload**: automatic when 123 reports `reuse`; also `rclone backend rapid-upload`.
* **Hashes**: MD5. **Modification times**: not supported.
* **Offline download**: `addurl`, `tasks`, `deltask` backend commands and the RC operations in §6.4. 123 resolves the URL and downloads all of its files into the remote's directory under names it chooses, so `name` is not supported.

---

### 3.3 GuangYa Drive (`guangyadrive`)
* **Authentication**: `refresh_token` (and optionally `access_token`); both are renewed and saved automatically. A default `client_id` is built in.
* **Uploads**: streamed straight to Aliyun OSS — one PUT up to `chunk_size` (default 8 MiB), otherwise an OSS multipart upload. Each part is read into a reused buffer and retried; no temporary file is used. Requests are signed with OSS V1 signatures in Go, without the OSS SDK.
* **Rapid upload**: decided by the server; when it reports a match no data is sent.
* **Hashes**: none. **Modification times**: not supported.
* **Offline download**: `addurl`, `tasks`, `deltask` backend commands and the RC operations in §6.4. For a torrent all of its files are downloaded; `name` renames the download.

---

### 3.4 115 Cloud (`cloud115`)
* **Authentication**: paste a cookie, or log in by scanning the QR code shown by `rclone config`. The resulting cookie is saved and the one-time `qrcode_token` cleared. `qrcode_source` (default `tv`) selects the device type; 115 allows one session per device type, so logging in signs out other sessions of that type.
* **Uploads**: 115 needs the SHA1 of the whole file and of its first 128 KiB, and may ask for the SHA1 of a range it chooses. If the source provides SHA1 and can be read by range, the file streams; otherwise it is spooled to a temporary file while hashing. Files up to `chunk_size` (default 16 MiB) go to Aliyun OSS in one request, larger files as a multipart upload; each part is retried and the OSS token is refreshed every 50 minutes.
* **Rapid upload**: tried first, answering range proofs (up to three rounds); also `rclone backend rapid-upload` with `115://name|size|sha1|block_sha1` links.
* **Offline download**: `addurl`, `tasks`, `deltask`, `cleartasks` backend commands and the RC operations in §6.4.
* **Hashes**: SHA1. **Modification times**: not supported.

---

### 3.5 Xunlei Drive (`thunder`)
* **Authentication**: `username` and `password` (logging in as an Android client), or `refresh_token`. Refresh tokens are rotated and saved; `device_id` is generated if empty.
* **Uploads**: Xunlei needs the file's GCID first. When the source provides it, data streams to S3: one PUT below `upload_cutoff` (default 200 MiB), otherwise a multipart upload using `chunk_size` × `upload_concurrency` of memory (default 5 MiB × 4). Without a GCID, or when the size is unknown, the input is spooled to a temporary file while hashing.
* **Rapid upload**: when Xunlei already has content with the same GCID, no data is sent.
* **Hashes**: GCID, registered as the hash type `gcid`, so `rclone check`, `--checksum` and `rclone hashsum gcid` work against local files. **Modification times**: not supported.
* **Offline download**: `addurl`, `tasks`, `deltask` backend commands and the RC operations in §6.4; deleting a task keeps the files.
* Deleted files go to the trash.

---

### 3.6 Baidu Netdisk (`baidunetdisk`)
* **Authentication**: OAuth2 refresh token with a built-in client ID, or your own `client_id` / `client_secret`. The rotated refresh token is saved.
* **Uploads**: precreate needs the MD5 of every block, so data is read twice: a local source is opened a second time for hashing and then streamed; other sources are spooled to a temporary file while hashing. One block is held in memory. Block size follows the account: 4 MiB (normal), 16 MiB (VIP), 32 MiB (SVIP), up to 2048 blocks. Each block is retried.
* **Rapid upload**: precreate creates the file without data when Baidu already has it; `rclone backend rapid-upload` accepts `md5#slicemd5#size#name` links.
* **Download modes** (`download_api`): `official` uses the official download link; `crack` uses DLNA links (`filemetas?origin=dlna`) with the user agent from `custom_crack_ua` (default `netdisk`).
* **Hashes**: MD5. **Modification times**: the source time is sent as `local_mtime` on upload but can't be changed afterwards.
* Empty files can't be uploaded.

---

### 3.7 Quark Drive / UC Drive (`quark`)
* **Authentication**: browser cookie containing `__puus`. `mode = quark` uses `drive.quark.cn`; `mode = uc` uses `pc-api.uc.cn`. Refreshed `__puus`/`__pus` values are saved to the config.
* **Uploads**: Quark needs MD5 and SHA1 first. When the source provides both, data streams to OSS in parts of the size the server chooses, each part retried; otherwise the input is spooled to a temporary file while hashing.
* **Rapid upload**: from MD5 + SHA1 when the server already has the content.
* **Hashes**: none reported for remote files. **Modification times**: not supported.

---

### 3.8 Tencent Weiyun (`weiyun`)
* **Authentication**: browser cookies (`uin`, `skey`, `p_skey`).
* **Uploads**: the Weiyun SDK hashes every 1 MiB block first and then asks for pieces at arbitrary offsets, so files up to 1 MiB are buffered in memory and larger ones are spooled to a temporary file. Each piece is retried.
* **Rapid upload**: automatic when the server already has the content.
* **Hashes**: SHA1. **Modification times**: not supported.
* Not available on plan9 (the SDK's HTTP library doesn't build there).

---

## 4. Retries, Uploads & Caching

1. **Pacer**: every API call, upload part and download goes through `lib/pacer` (minimum interval 50–100 ms, backing off to 2–4 s). Retries cover HTTP 429/5xx, network errors and each provider's throttling or transient error codes; expired tokens or cookies are renewed and the call retried.
2. **Business error codes**: these APIs often answer HTTP 200 with an error code in the body. Every response is checked, so such errors are reported instead of being treated as success.
3. **Uploads**: most providers need hashes before an upload starts (§3). When the source can supply them — a local file, or another remote with the same hash type — the data streams with bounded memory. When it can't (e.g. `rclone rcat`, or a source without that hash) the input is spooled to a temporary file in `--temp-dir` while hashing.
4. **Directory cache**: path-to-ID lookups are cached with `lib/dircache` and flushed on changes.

---

## 5. Configuration & Command Examples

### 5.1 `rclone.conf` Examples

```ini
# 115 Cloud (cookie; or leave empty and use QR login in rclone config)
[my115]
type = cloud115
cookie = UID=...; CID=...; SEID=...; KID=...

# 123 Cloud
[my123]
type = cloud123
user = 13800138000
pass = *** ENCRYPTED ***

# Aliyun Drive (renewed through OpenList's service)
[myaliyun]
type = aliyundrive
refresh_token = your_refresh_token

# Aliyun Drive (your own Open Platform app; tokens stay with Aliyun)
[myaliyun_own]
type = aliyundrive
refresh_token = your_refresh_token
use_online_api = false
client_id = your_client_id
client_secret = your_client_secret

# Baidu Netdisk (DLNA download mode)
[mybaidu]
type = baidunetdisk
refresh_token = your_refresh_token
download_api = crack

# Quark Drive
[myquark]
type = quark
mode = quark
cookie = __puus=...; b-user-id=...

# UC Drive
[myuc]
type = quark
mode = uc
cookie = __puus=...

# Xunlei Drive
[mythunder]
type = thunder
username = 13800138000
password = *** ENCRYPTED ***

# Tencent Weiyun
[myweiyun]
type = weiyun
cookies = uin=123456789; skey=...; p_skey=...

# GuangYa Drive
[myguangya]
type = guangyadrive
refresh_token = your_refresh_token
```

Passwords must be obscured: enter them through `rclone config`, or use `rclone obscure`.

### 5.2 Command Line Examples

```bash
# List files
rclone lsf my115:

# Sync a local directory to Quark Drive
rclone sync /local/data myquark:backup -P --transfers=4

# Copy between providers
rclone copy myaliyun:movies my123:movies -P

# Verify against local files (Xunlei uses its GCID hash)
rclone check /local/movies mythunder:movies

# Mount
rclone mount mythunder: /Volumes/ThunderDrive --vfs-cache-mode writes

# Quota
rclone about my115:

# Server-side move within one account
rclone move my115:downloads/movie.mp4 my115:movies/ -P

# Rapid upload from hashes
rclone backend rapid-upload my115:movies "115://video.mkv|1048576000|<sha1>|<sha1 of first 128 KiB>"
rclone backend rapid-upload mybaidu:downloads "<md5>#<md5 of first 256 KiB>#1048576#sample.mp4"
rclone backend rapid-upload my123:backup archive.zip 52428800 <md5>

# Offline download (115, 123, GuangYa, Xunlei, PikPak)
rclone backend addurl my115:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl mythunder:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl my123:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl myguangya:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl mypikpak:downloads "https://example.com/file.iso"
rclone backend tasks mythunder:
rclone backend deltask my115: <info_hash>

# The same through the remote control API
rclone rc operations/addurl fs=my115:downloads url="magnet:?xt=..."
rclone rc operations/listoffline fs=mythunder:
rclone rc operations/deleteoffline fs=mypikpak: task_id=<task_id>
```

---

## 6. Implementation Notes

### 6.1 115 Cloud
1. **Session-safe cookie check**: `/status/login.php` signs out the account's other sessions, so the backend validates cookies with `CookieCheck()` (`/status/check.php`).
2. **CDN cookies**: download nodes reject requests without the account cookie, so `Object.Open` sends it explicitly.
3. **115driver**: the backend uses the upstream `github.com/SheltonZhu/115driver`. Upstream refuses calls whose caller's function name contains `alist`; Rclone's packages don't match, so no fork is needed.

### 6.2 123 Cloud
1. **Path signatures**: every request carries a CRC32-based signature in its query string, recomputed on each retry.
2. **Referer**: CDN downloads need `Referer: https://yun.123pan.com/`. Some download URLs answer with a JSON `redirect_url`, which is followed.
3. **Offline downloads**: `/v2/offline_download/task/resolve` turns the URL into a resource and `/v2/offline_download/task/submit` downloads its files into `upload_dir`; tasks are listed and deleted with `/offline_download/task/list` and `/offline_download/task/delete`.

### 6.3 Xunlei Drive
1. **Device signature**: logins carry a `div101.` device signature derived from the device ID, as the Android client does.
2. **Token and captcha handling**: codes `4121`, `4122`, `10` and `16` renew the access token; code `9` renews the per-action captcha token (`X-Captcha-Token`).
3. **GCID**: SHA1 over blocks whose size (256 KiB – 2 MiB) depends on the file size, then SHA1 of the concatenated block hashes. The hasher computes all candidate block sizes in one pass, so it can stream.

### 6.4 Offline Downloads (115, 123, GuangYa, Xunlei, PikPak)
1. **Interface**: `fs.OfflineDownloader` in `fs/features.go`, exposed as the `AddURL`, `ListOfflineTasks` and `DeleteOfflineTask` feature functions:
   - `AddURL(ctx, url, opt)` submits a URL (HTTP, magnet, ed2k) to download into the remote's directory; `opt["name"]` sets the file name where supported.
   - `ListOfflineTasks(ctx)` returns tasks with a common status (`init`, `running`, `complete`, `error`) and progress.
   - `DeleteOfflineTask(ctx, taskID)` deletes a task; 115, Xunlei and PikPak keep the downloaded files.
2. **RC**: `operations/addurl` (`fs`, `url`, optional `name` and `opt`), `operations/listoffline` (`fs`) and `operations/deleteoffline` (`fs`, `task_id`).
3. **Backend commands**: `addurl`, `tasks`, `deltask` on `cloud115`, `cloud123`, `guangyadrive`, `thunder` and `pikpak`. PikPak's `addurl` now returns the common task object instead of PikPak's own.

### 6.5 Quark & UC Drive
1. **One backend, two modes**: Quark and UC share the protocol but differ in endpoints, protocol flags and referers; `mode` selects them.
2. **Cookie rotation**: `__puus` expires after a few hours, but responses carry fresh values in `Set-Cookie`; they are merged and saved to the config.
3. **HTML entities in names**: listings return names with HTML entities (`&amp;`), which are unescaped.

### 6.6 Baidu Netdisk
1. **Obfuscated MD5s**: listings return MD5s scrambled with byte swaps and XOR; `DecryptMd5` recovers the real MD5.
2. **Redirects**: download links need a specific user agent that cross-domain redirects can lose, so the final CDN URL is resolved with a HEAD request first.
3. **Error codes**: tokens are renewed on errno `111`, `-6` and `20016`; errno `-9` means not found.

### 6.7 Tencent Weiyun
1. **CSRF token**: every call needs a `g_tk` computed from `skey` (done by the SDK).
2. **Parent keys**: downloads, deletes and moves need the parent directory's key as well as the file ID, so objects keep their parent key and directory keys are cached.

### 6.8 GuangYa Drive
1. **Two endpoints**: authentication uses `account.guangyapan.com`, storage operations `api.guangyapan.com`.
2. **Server-side tasks**: delete, move and copy start server-side tasks; the backend waits for them to finish.
3. **Offline downloads**: `/cloudcollection/v1/resolve_res` resolves the URL (for a torrent, its name and files), `/cloudcollection/v1/create_task` starts the download with the file indexes, `/cloudcollection/v1/list_task` lists tasks page by page with a cursor and `/cloudcollection/v2/delete_task` deletes them. A partially completed task is reported as `complete` with the error message `partially completed`.

## 7. Serving Remote Objects

The fork makes reads through `--rc-serve` cheaper and steadier, for every backend:

1. **Object cache** (`fs/objcache`): served objects are kept for `--rc-serve-cache-duration` (5 minutes), up to `--rc-serve-cache-size` objects. `operations/list` and `operations/stat` replace the entries they return; a listing of one folder without filters also drops the entries of the files gone from it. The rc's writes drop the entries of the paths they touch, in every root of the remote. While the cache holds objects of a remote whose backend can notify changes (`ChangeNotify`), it polls for changes every `--rc-serve-poll-interval` (1 minute, 0 to disable) and drops the entries of the changed paths the same way; polling stops once the remote's entries are gone or expired. Backends whose `NewObject` lists the parent folder declare the `FindByListing` feature: a lookup lists the folder once, keeps its objects (at most a quarter of the cache) and shares the listing with concurrent lookups of that folder. They are cloud115, aliyundrive, baidunetdisk, cloud123, guangyadrive, quark, weiyun, thunder, pikpak, pcloud, iclouddrive, fichier, filelu, protondrive, premiumizeme, sugarsync, zoho, huaweidrive, drime, linkbox, filen, internetarchive, googlephotos, chunker, and ftp on servers without `MLST`; crypt and hasher inherit it, union, combine and compress don't.
2. **Versions**: responses carry an `ETag` from the object's ID, the storage's version tag where the backend knows it (`fs.VersionTagger`), size, and the modification time and hash where they are cheap. `If-Match` with another version looks the object up again, then answers 412. A failed open of a cached object looks it up again and retries once; when the version changed, a request with `If-Match` gets 412 and one without is served the fresh object from the start. s3, azureblob, swift, webdav, oracleobjectstorage and qingstor read with `If-Match` set to the storage's ETag the object was found with, so a file replaced between the lookup and a read, or between the ranged reads of one stream, fails with 412 instead of mixing contents (s3's `--s3-no-read-if-match` turns this off; swift does it only for objects a HEAD showed are not large objects; webdav only with a strong `getetag`). googlecloudstorage already reads the generation found, and b2 the file ID unless `--b2-download-url` is set.
3. **Statuses** (`lib/http/serve/status.go`): failed lookups and opens answer with the storage's status instead of 404 and 500: 404, 412, 403, 429 and 503 with `Retry-After`, 502 otherwise. The status is read from the error chain; drive's `downloadQuotaExceeded`, mega's quota and rate errors and HTTP 509 become 429.
4. **Stream limits** (`lib/connlimit`): `--rc-serve-max-file-streams` closes a file's least recently read stream for a new one; `--rc-serve-max-streams` makes opens wait while an account (the innermost remote beneath wrappers) is full. Unset (0), they are the backend's estimates (the `MaxFileStreams` and `MaxStreams` features): cloud115, baidunetdisk, quark (and uc) and weiyun 2 per file and 4 per account; guangyadrive, mega and protondrive 2 and 6; cloud123, thunder, pikpak, pcloud and iclouddrive 3 and 8; aliyundrive and yandex 4 and 10; drive, onedrive, dropbox and box 4 and 16; mailru 4 and 4 (a download server serves 4 streams). Other backends have none. -1 turns a limit off.
5. **Seeking**: the `SeekReadsFromStart` feature, reported by `operations/fsinfo`, marks remotes that read from the start of a file to reach an offset: seafile's encrypted libraries and the zip files archive opens.
6. **Backend fixes**: webdav and http discard the bytes before the offset when a server answers a range with 200 (`lib/readers.SkipToRange`); http's errors carry their status code; crypt keeps a file's nonce after its first read, so a seek costs one request. filelu reads with ranges instead of discarding up to the offset; shade asks for the range on its first request; cloud115's API calls, weiyun's and baidunetdisk's downloads and putio's, quatrix's, seafile's, iclouddrive's and filescom's download errors carry their HTTP status to the pacer and the rc; baidunetdisk's downloads go through the pacer; thunder renews its link before the expiry the URL states; box no longer sends its token to download servers.
7. **Link cache** (`lib/linkcache`): the download links that `Open` requests are kept, keyed by the remote, the object's ID or path and its fingerprint, so the seeks of a player share one link. A link lives until the expiry the API or the URL states (S3, GCS and Azure signatures, `Expires`, `t`, `auth_key` and the like), else for the backend's estimate, at most 24 hours; it is renewed at four fifths of its life, a renewal that fails keeps using the live link, and concurrent opens share one request. A download refused with 401, 403, 404 or 410 drops the link and is retried once with a new one. It is used by cloud115 (bound to its user agent and cookies), aliyundrive (its `expiration`), baidunetdisk (the redirect the `dlink` resolves to), cloud123 (the listing's link first), guangyadrive, quark, weiyun, yandex, putio, sharefile, quatrix, ulozto (its `download_url_valid_until`), seafile (with `reuse=1`), filelu, iclouddrive (the listing's link first), filescom, mega (the SDK's download, with its MAC check run once), internxt (the shard URL, key and IV) and, for the redirect target of their download URL, onedrive, box, shade and internetarchive. pikpak, pcloud and thunder keep their links on the cached object; protondrive's SDK reads only through its own calls, and fichier's tokens are single-use.
