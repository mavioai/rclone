# Mavio 云存储后端（v1.75.2-mavio.2）

> [English](MAVIO_PROVIDERS.md) | 简体中文

本分支基于 Rclone `v1.75.2`，新增八个国内网盘后端：115 网盘、123 云盘、光鸭云盘、迅雷云盘、百度网盘、夸克网盘 / UC 网盘、阿里云盘和腾讯微云。接口知识来自 [AList](https://github.com/alist-org/alist) 及其社区分支 [OpenList](https://github.com/OpenListTeam/OpenList)，实现遵循 Rclone 的后端惯例（`lib/rest`、`lib/pacer`、`lib/dircache`、`lib/encoder`）。

另外让所有后端经由 rc（`--rc-serve`）提供远端文件时更省（§7），并新增云端离线下载接口（`fs.OfflineDownloader`），由 115 网盘、123 云盘、光鸭云盘、迅雷云盘和 PikPak 实现。

这些后端只有单元测试，尚未用 Rclone 的集成测试套件对真实服务跑过，应视为未经测试。

各后端的参考文档（含全部选项）位于 `docs/content/<backend>.md`。

---

## 目录
- [1. 设计原则（与 AList 对比）](#1-设计原则与-alist-对比)
- [2. 后端命名与登录方式](#2-后端命名与登录方式)
- [3. 各后端](#3-各后端)
  - [3.1 阿里云盘（`aliyundrive`）](#31-阿里云盘aliyundrive)
  - [3.2 123 云盘（`cloud123`）](#32-123-云盘cloud123)
  - [3.3 光鸭云盘（`guangyadrive`）](#33-光鸭云盘guangyadrive)
  - [3.4 115 网盘（`cloud115`）](#34-115-网盘cloud115)
  - [3.5 迅雷云盘（`thunder`）](#35-迅雷云盘thunder)
  - [3.6 百度网盘（`baidunetdisk`）](#36-百度网盘baidunetdisk)
  - [3.7 夸克网盘 / UC 网盘（`quark`）](#37-夸克网盘--uc-网盘quark)
  - [3.8 腾讯微云（`weiyun`）](#38-腾讯微云weiyun)
- [4. 重试、上传与缓存](#4-重试上传与缓存)
- [5. 配置与命令示例](#5-配置与命令示例)
- [6. 实现说明](#6-实现说明)
- [7. 通过 rc 提供远端文件](#7-通过-rc-提供远端文件)

---

## 1. 设计原则（与 AList 对比）

| 方面 | AList | 本分支 |
| :--- | :--- | :--- |
| **命名** | 相近的服务是独立驱动（如夸克与 UC）。 | 后端用英文名（`cloud115`、`cloud123`、`guangyadrive`、`baidunetdisk` 等）并提供别名；相近的服务是同一后端的不同模式（`quark` 的 `mode = uc`）。 |
| **上传缓冲** | 常先把整个文件缓存到临时文件（`CacheFullInTempFile`）再上传。 | 能事先从源获得网盘要求的哈希时流式上传（本地文件通过读取文件计算）；否则先写入临时文件。光鸭和阿里云盘一般直接流式上传；微云始终需要随机读取（见 §4）。除不超过 1 MiB 的微云文件外，不会把整个文件放进内存。 |
| **数据通路** | 经 AList 自身的 WebDAV/HTTP 层转发。 | Rclone 直接访问网盘 API 和对象存储（S3/OSS/CDN）。 |
| **修改时间** | 大多被上传时间取代。 | 阿里云盘保存源修改时间（毫秒精度）。百度上传时接收修改时间，但之后不能修改。其他网盘不支持，后端如实声明（`fs.ModTimeNotSupported`），不做假装。 |
| **限流** | 静态限速器。 | `lib/pacer` 对 HTTP 429/5xx、网络错误和各网盘的限流错误码做指数退避；令牌或 Cookie 过期时自动续期。 |
| **Rclone 语义** | — | `Rmdir` 拒绝删除非空目录，`Purge` 递归删除，覆盖上传会替换旧文件而不是留下重复文件，目标已存在时 `DirMove` 返回 `ErrorDirExists`。 |

---

## 2. 后端命名与登录方式

| 网盘 | 后端（`type`） | 别名 | 登录方式 |
| :--- | :--- | :--- | :--- |
| **115 网盘** | `cloud115` | `115`、`115drive` | ① Cookie（`UID`、`CID`、`SEID`、`KID`）<br>② 在 `rclone config` 中扫码登录（`qrcode_source` 选择设备类型） |
| **123 云盘** | `cloud123` | `123`、`123cloud`、`123drive` | ① 手机号/邮箱和密码<br>② 令牌（`token`），过期后重新登录续期 |
| **光鸭云盘** | `guangyadrive` | `guangya`、`guangyacloud` | 刷新令牌（`refresh_token`），自动续期 |
| **迅雷云盘** | `thunder` | `xunlei`、`thunderdrive` | ① 用户名和密码（以 Android 客户端身份登录）<br>② 刷新令牌 |
| **百度网盘** | `baidunetdisk` | `baidudrive` | 开放平台 OAuth2 刷新令牌，使用内置或自定义的 client ID |
| **夸克 / UC 网盘** | `quark` | `quarkdrive`、`ucdrive` | 浏览器 Cookie（`__puus`）；`mode = quark` 或 `uc` |
| **阿里云盘** | `aliyundrive` | `aliyun`、`alibabadrive` | 开放平台刷新令牌，经 OpenList 的服务或你自己的应用续期（见 §3.1） |
| **腾讯微云** | `weiyun` | `tencentweiyun` | 浏览器 Cookie（`uin`、`skey`、`p_skey`） |

---

## 3. 各后端

### 3.1 阿里云盘（`aliyundrive`）
* **认证**：刷新令牌。`use_online_api = true`（默认）时通过 `oauth_token_url` 续期访问令牌，默认是 OpenList 的 `https://api.oplist.org/alicloud/renewapi`。用其扫码工具获得的刷新令牌属于该服务的开放平台应用，只有该服务能续期。**刷新令牌会发送给这个第三方服务。** `alipan_type = alipanTV` 用于 TV 端登录获得的令牌。使用自己的应用时设置 `client_id`、`client_secret` 和 `use_online_api = false`，此时只与 `openapi.alipan.com` 续期，并校验新令牌的用户与旧令牌一致。轮换后的令牌会写回配置。
* **上传**：按 `chunk_size` 分片流式上传（默认 20 MiB，必要时调大以不超过 10,000 片），内存中只保留一片。每片单独重试，分片 URL 在 50 分钟后或遇到 HTTP 403 时刷新。只有大小未知，或秒传需要 SHA1 而源无法提供时才使用临时文件。
* **秒传**（`rapid_upload`，默认关闭，大于 100 KiB 的文件）：提交源的 SHA1 和从源某一区间读取的 8 字节校验码；没有 SHA1 时先发送 1 KiB 预哈希。
* **哈希**：SHA1。**修改时间**：保存为 `local_modified_at`。
* **选项**：`drive_type`（`resource`、`backup`、`default`）、`drive_id`、`root_folder_id`、`remove_way`（`trash` 或 `delete`）、`chunk_size`、`encoding`。
* 复制和移动只能在同一个 drive 内进行。

---

### 3.2 123 云盘（`cloud123`）
* **认证**：`user` 和 `pass`，或 `token`。令牌过期后重新登录续期。
* **上传**：123 需要先提供 MD5。源能提供时，按 `chunk_size`（5–16 MiB，默认 16 MiB）分片流式上传到预签名的 S3 URL，每片单独重试，遇到 HTTP 403 时刷新 URL。否则边计算哈希边写入临时文件。
* **秒传**：123 返回 `reuse` 时自动完成；也可用 `rclone backend rapid-upload`。
* **哈希**：MD5。**修改时间**：不支持。
* **离线下载**：后端命令 `addurl`、`tasks`、`deltask`，以及 §6.4 的 RC 操作。123 先解析 URL，再把其中所有文件下载到远端目录，文件名由 123 决定，因此不支持 `name`。

---

### 3.3 光鸭云盘（`guangyadrive`）
* **认证**：`refresh_token`（可选 `access_token`），两者都会自动续期并保存。内置默认 `client_id`。
* **上传**：直接流式上传到阿里云 OSS——不超过 `chunk_size`（默认 8 MiB）时一次 PUT，否则使用 OSS 分片上传。每片读入复用的缓冲区并单独重试，不使用临时文件。请求用 Go 实现的 OSS V1 签名，不依赖 OSS SDK。
* **秒传**：由服务器判定；服务器报告命中时不发送数据。
* **哈希**：无。**修改时间**：不支持。
* **离线下载**：后端命令 `addurl`、`tasks`、`deltask`，以及 §6.4 的 RC 操作。种子会下载其中所有文件；`name` 可重命名下载内容。

---

### 3.4 115 网盘（`cloud115`）
* **认证**：粘贴 Cookie，或在 `rclone config` 中扫描显示的二维码登录。登录得到的 Cookie 会被保存，一次性的 `qrcode_token` 会被清除。`qrcode_source`（默认 `tv`）选择设备类型；115 每种设备类型只允许一个会话，扫码登录会挤掉同类型的其他会话。
* **上传**：115 需要整个文件和前 128 KiB 的 SHA1，并可能要求它指定区间的 SHA1。源能提供 SHA1 且支持按区间读取时流式上传；否则边计算哈希边写入临时文件。不超过 `chunk_size`（默认 16 MiB）的文件一次请求上传到阿里云 OSS，更大的文件使用分片上传；每片单独重试，OSS 令牌每 50 分钟刷新。
* **秒传**：优先尝试，并应答区间校验（最多三轮）；也可用 `rclone backend rapid-upload` 和 `115://name|size|sha1|block_sha1` 链接。
* **离线下载**：后端命令 `addurl`、`tasks`、`deltask`、`cleartasks`，以及 §6.4 的 RC 操作。
* **哈希**：SHA1。**修改时间**：不支持。

---

### 3.5 迅雷云盘（`thunder`）
* **认证**：`username` 和 `password`（以 Android 客户端身份登录），或 `refresh_token`。刷新令牌会轮换并保存；`device_id` 为空时自动生成。
* **上传**：迅雷需要先提供文件的 GCID。源能提供时流式上传到 S3：小于 `upload_cutoff`（默认 200 MiB）时一次 PUT，否则分片上传，占用 `chunk_size` × `upload_concurrency` 的内存（默认 5 MiB × 4）。没有 GCID 或大小未知时，边计算哈希边写入临时文件。
* **秒传**：迅雷已有相同 GCID 的内容时不发送数据。
* **哈希**：GCID，注册为哈希类型 `gcid`，因此 `rclone check`、`--checksum` 和 `rclone hashsum gcid` 可以与本地文件比对。**修改时间**：不支持。
* **离线下载**：后端命令 `addurl`、`tasks`、`deltask`，以及 §6.4 的 RC 操作；删除任务会保留已下载的文件。
* 删除的文件进入回收站。

---

### 3.6 百度网盘（`baidunetdisk`）
* **认证**：OAuth2 刷新令牌，使用内置 client ID，或你自己的 `client_id` / `client_secret`。轮换后的刷新令牌会写回配置。
* **上传**：预创建需要每个分块的 MD5，因此数据要读两遍：本地源会再打开一次用于计算哈希然后流式上传；其他源边计算哈希边写入临时文件。内存中只保留一个分块。分块大小随账号等级：4 MiB（普通）、16 MiB（VIP）、32 MiB（SVIP），最多 2048 块。每块单独重试。
* **秒传**：百度已有该文件时预创建直接完成，不发送数据；`rclone backend rapid-upload` 接受 `md5#slicemd5#size#name` 链接。
* **下载模式**（`download_api`）：`official` 使用官方下载链接；`crack` 使用 DLNA 链接（`filemetas?origin=dlna`），User-Agent 取自 `custom_crack_ua`（默认 `netdisk`）。
* **哈希**：MD5。**修改时间**：上传时以 `local_mtime` 发送源修改时间，之后不能修改。
* 不能上传空文件。

---

### 3.7 夸克网盘 / UC 网盘（`quark`）
* **认证**：包含 `__puus` 的浏览器 Cookie。`mode = quark` 使用 `drive.quark.cn`；`mode = uc` 使用 `pc-api.uc.cn`。刷新后的 `__puus`/`__pus` 会写回配置。
* **上传**：夸克需要先提供 MD5 和 SHA1。源能同时提供两者时，按服务器指定的分片大小流式上传到 OSS，每片单独重试；否则边计算哈希边写入临时文件。
* **秒传**：根据 MD5 + SHA1，服务器已有该内容时完成。
* **哈希**：不报告远端文件的哈希。**修改时间**：不支持。

---

### 3.8 腾讯微云（`weiyun`）
* **认证**：浏览器 Cookie（`uin`、`skey`、`p_skey`）。
* **上传**：微云 SDK 先计算每个 1 MiB 分块的哈希，再按任意偏移索取数据，因此不超过 1 MiB 的文件缓冲在内存中，更大的文件写入临时文件。每片单独重试。
* **秒传**：服务器已有该内容时自动完成。
* **哈希**：SHA1。**修改时间**：不支持。
* 不支持 plan9（SDK 使用的 HTTP 库无法在该平台编译）。

---

## 4. 重试、上传与缓存

1. **Pacer**：所有 API 调用、上传分片和下载都经过 `lib/pacer`（最小间隔 50–100 ms，退避上限 2–4 s）。重试覆盖 HTTP 429/5xx、网络错误以及各网盘的限流或临时错误码；令牌或 Cookie 过期时续期后重试。
2. **业务错误码**：这些 API 常以 HTTP 200 返回、在响应体中给出错误码。每个响应都会检查，这类错误会被报告而不是当作成功。
3. **上传**：多数网盘在上传开始前就需要哈希（§3）。源能提供时（本地文件，或支持同类哈希的其他远端），数据以有限内存流式上传。不能提供时（如 `rclone rcat`，或源没有该哈希），边计算哈希边把输入写入 `--temp-dir` 下的临时文件。
4. **目录缓存**：路径到 ID 的查询结果由 `lib/dircache` 缓存，变更时刷新。

---

## 5. 配置与命令示例

### 5.1 `rclone.conf` 示例

```ini
# 115 网盘（Cookie；也可留空，在 rclone config 中扫码登录）
[my115]
type = cloud115
cookie = UID=...; CID=...; SEID=...; KID=...

# 123 云盘
[my123]
type = cloud123
user = 13800138000
pass = *** ENCRYPTED ***

# 阿里云盘（经 OpenList 的服务续期）
[myaliyun]
type = aliyundrive
refresh_token = your_refresh_token

# 阿里云盘（自己的开放平台应用；令牌只与阿里云盘交互）
[myaliyun_own]
type = aliyundrive
refresh_token = your_refresh_token
use_online_api = false
client_id = your_client_id
client_secret = your_client_secret

# 百度网盘（DLNA 下载模式）
[mybaidu]
type = baidunetdisk
refresh_token = your_refresh_token
download_api = crack

# 夸克网盘
[myquark]
type = quark
mode = quark
cookie = __puus=...; b-user-id=...

# UC 网盘
[myuc]
type = quark
mode = uc
cookie = __puus=...

# 迅雷云盘
[mythunder]
type = thunder
username = 13800138000
password = *** ENCRYPTED ***

# 腾讯微云
[myweiyun]
type = weiyun
cookies = uin=123456789; skey=...; p_skey=...

# 光鸭云盘
[myguangya]
type = guangyadrive
refresh_token = your_refresh_token
```

密码必须混淆：通过 `rclone config` 输入，或使用 `rclone obscure`。

### 5.2 命令行示例

```bash
# 列出文件
rclone lsf my115:

# 把本地目录同步到夸克网盘
rclone sync /local/data myquark:backup -P --transfers=4

# 在网盘之间复制
rclone copy myaliyun:movies my123:movies -P

# 与本地文件比对（迅雷使用 GCID 哈希）
rclone check /local/movies mythunder:movies

# 挂载
rclone mount mythunder: /Volumes/ThunderDrive --vfs-cache-mode writes

# 查看容量
rclone about my115:

# 同一账号内的服务端移动
rclone move my115:downloads/movie.mp4 my115:movies/ -P

# 根据哈希秒传
rclone backend rapid-upload my115:movies "115://video.mkv|1048576000|<sha1>|<前 128 KiB 的 sha1>"
rclone backend rapid-upload mybaidu:downloads "<md5>#<前 256 KiB 的 md5>#1048576#sample.mp4"
rclone backend rapid-upload my123:backup archive.zip 52428800 <md5>

# 离线下载（115、123、光鸭、迅雷、PikPak）
rclone backend addurl my115:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl mythunder:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl my123:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl myguangya:downloads "magnet:?xt=urn:btih:..."
rclone backend addurl mypikpak:downloads "https://example.com/file.iso"
rclone backend tasks mythunder:
rclone backend deltask my115: <info_hash>

# 通过远程控制 API 完成同样的操作
rclone rc operations/addurl fs=my115:downloads url="magnet:?xt=..."
rclone rc operations/listoffline fs=mythunder:
rclone rc operations/deleteoffline fs=mypikpak: task_id=<task_id>
```

---

## 6. 实现说明

### 6.1 115 网盘
1. **不挤掉会话的 Cookie 校验**：`/status/login.php` 会让该账号的其他会话下线，因此后端用 `CookieCheck()`（`/status/check.php`）校验 Cookie。
2. **CDN Cookie**：下载节点拒绝不带账号 Cookie 的请求，因此 `Object.Open` 会显式带上 Cookie。
3. **115driver**：后端使用上游 `github.com/SheltonZhu/115driver`。上游会拒绝调用者函数名包含 `alist` 的调用；Rclone 的包名不匹配，因此不需要 fork。

### 6.2 123 云盘
1. **路径签名**：每个请求的查询字符串都带有基于 CRC32 的签名，每次重试都重新计算。
2. **Referer**：CDN 下载需要 `Referer: https://yun.123pan.com/`。有些下载 URL 返回带 `redirect_url` 的 JSON，后端会跟随跳转。
3. **离线下载**：`/v2/offline_download/task/resolve` 把 URL 解析为资源，`/v2/offline_download/task/submit` 把其中的文件下载到 `upload_dir`；任务通过 `/offline_download/task/list` 和 `/offline_download/task/delete` 列出和删除。

### 6.3 迅雷云盘
1. **设备签名**：登录时像 Android 客户端一样附带由设备 ID 计算出的 `div101.` 设备签名。
2. **令牌与验证码处理**：错误码 `4121`、`4122`、`10`、`16` 触发续期访问令牌；错误码 `9` 触发续期按操作区分的验证码令牌（`X-Captcha-Token`）。
3. **GCID**：按文件大小选择分块大小（256 KiB – 2 MiB），对每块求 SHA1，再对拼接起来的分块哈希求 SHA1。哈希器一次读取同时计算所有候选分块大小，因此可以流式计算。

### 6.4 离线下载（115、123、光鸭、迅雷、PikPak）
1. **接口**：`fs/features.go` 中的 `fs.OfflineDownloader`，以功能函数 `AddURL`、`ListOfflineTasks` 和 `DeleteOfflineTask` 提供：
   - `AddURL(ctx, url, opt)` 提交一个 URL（HTTP、磁力、ed2k），下载到远端目录；支持时 `opt["name"]` 指定文件名。
   - `ListOfflineTasks(ctx)` 返回任务列表，包含统一的状态（`init`、`running`、`complete`、`error`）和进度。
   - `DeleteOfflineTask(ctx, taskID)` 删除任务；115、迅雷和 PikPak 会保留已下载的文件。
2. **RC**：`operations/addurl`（`fs`、`url`，可选 `name` 和 `opt`）、`operations/listoffline`（`fs`）和 `operations/deleteoffline`（`fs`、`task_id`）。
3. **后端命令**：`cloud115`、`cloud123`、`guangyadrive`、`thunder` 和 `pikpak` 上的 `addurl`、`tasks`、`deltask`。PikPak 的 `addurl` 现在返回统一的任务对象，而不是 PikPak 自己的结构。

### 6.5 夸克与 UC 网盘
1. **一个后端，两种模式**：夸克和 UC 协议相同，只是接口地址、协议参数和 Referer 不同；由 `mode` 选择。
2. **Cookie 轮换**：`__puus` 几小时后过期，但响应的 `Set-Cookie` 中带有新值；后端会合并并写回配置。
3. **文件名中的 HTML 实体**：列表接口返回的文件名含 HTML 实体（`&amp;`），后端会反转义。

### 6.6 百度网盘
1. **混淆的 MD5**：列表接口返回经过字节交换和异或混淆的 MD5；`DecryptMd5` 还原真实 MD5。
2. **重定向**：下载链接需要特定 User-Agent，跨域重定向时可能丢失，因此先用 HEAD 请求解析出最终的 CDN URL。
3. **错误码**：errno `111`、`-6`、`20016` 触发续期令牌；errno `-9` 表示不存在。

### 6.7 腾讯微云
1. **CSRF 令牌**：每次调用都需要由 `skey` 计算出的 `g_tk`（由 SDK 完成）。
2. **父目录 key**：下载、删除和移动除了文件 ID 还需要父目录的 key，因此对象会保存父目录 key，目录 key 也会被缓存。

### 6.8 光鸭云盘
1. **两个接口域名**：认证使用 `account.guangyapan.com`，存储操作使用 `api.guangyapan.com`。
2. **服务端任务**：删除、移动和复制会启动服务端任务；后端会等待任务完成。
3. **离线下载**：`/cloudcollection/v1/resolve_res` 解析 URL（种子则得到名称和文件列表），`/cloudcollection/v1/create_task` 带上文件索引开始下载，`/cloudcollection/v1/list_task` 用游标分页列出任务，`/cloudcollection/v2/delete_task` 删除任务。部分完成的任务报告为 `complete`，错误信息为 `partially completed`。

### 6.9 硬链接
1. **接口**：可选的 `fs.Linker` 接口（`Link(ctx, src, remote)`，即 `Link` 功能）把对象硬链接到同一远端的另一路径，两个名称共享其数据；远端无法链接（如跨卷）时返回 `fs.ErrorCantLink`。
2. **后端**：`local` 用 `os.Link` 链接；`sftp` 用服务器的 `hardlink@openssh.com` 扩展，服务器没有该扩展时关闭其 `Link` 功能；`smb` 在同一共享内用 SMB2 SET_INFO 的 `FileLinkInformation` 链接，由 go-smb2 的 fork（子模块 `third_party/go-smb2`，即 mavioai/go-smb2，经 `go.mod` 的 `replace` 引入）以 `Share.Link` 提供，服务器回应 `STATUS_NOT_SUPPORTED`、`STATUS_NOT_SAME_DEVICE`、`STATUS_INVALID_DEVICE_REQUEST` 或 `STATUS_INVALID_INFO_CLASS` 时视为无法链接。
3. **RC**：`operations/hardlink`（`srcFs`、`srcRemote`、`dstFs`、`dstRemote`，须为同一远端）在目标已存在时失败，使媒体服务器导入下载时，把文件硬链接进同一卷上的媒体库而非复制。

## 7. 通过 rc 提供远端文件

fork 让经由 `--rc-serve` 的读取在所有后端上更省、更稳：

1. **对象缓存**（`fs/objcache`）：提供过的对象保留 `--rc-serve-cache-duration`（5 分钟），最多 `--rc-serve-cache-size` 个。`operations/list` 和 `operations/stat` 替换它们返回的条目；不带过滤器地列一个文件夹时，还会删掉其中已不存在的文件的条目。rc 的写操作删除所涉路径的条目，对该远端的所有根都生效。缓存中有可通知变更（`ChangeNotify`）的远端的对象时，每隔 `--rc-serve-poll-interval`（1 分钟，0 为关闭）轮询变更，并同样删除变更路径的条目；该远端的条目都已删除或过期后停止轮询。`NewObject` 要列父文件夹的后端声明 `FindByListing` 特性：查找时把文件夹列一次，保留其中的对象（最多占缓存的四分之一），同一文件夹的并发查找共用这次列举。这些后端是 cloud115、aliyundrive、baidunetdisk、cloud123、guangyadrive、quark、weiyun、thunder、pikpak、pcloud、iclouddrive、fichier、filelu、protondrive、premiumizeme、sugarsync、zoho、huaweidrive、drime、linkbox、filen、internetarchive、googlephotos、chunker，以及不支持 `MLST` 的服务器上的 ftp；crypt 和 hasher 继承它，union、combine 和 compress 不继承。
2. **版本**：响应带 `ETag`，由对象的 ID、后端知道时存储自身的版本标记（`fs.VersionTagger`）、大小，以及代价低时的修改时间和哈希得出。`If-Match` 给出其他版本时重新查找对象，仍不一致就回 412。缓存对象打开失败时重新查找并重试一次；版本已变时，带 `If-Match` 的请求得到 412，不带的请求从头得到新对象。s3、azureblob、swift、webdav、oracleobjectstorage 和 qingstor 读取时把 `If-Match` 设为找到对象时存储给的 ETag，因此在查找和读取之间、或同一读取流的多次范围请求之间被替换的文件会以 412 失败，而不会混入新内容（s3 可用 `--s3-no-read-if-match` 关闭；swift 只对 HEAD 确认不是大对象的对象这样做；webdav 只在 `getetag` 是强 ETag 时这样做）。googlecloudstorage 本来就读取找到时的 generation，b2 在未设 `--b2-download-url` 时读取文件 ID。
3. **状态码**（`lib/http/serve/status.go`）：查找和打开失败时按存储的错误回应，不再一律 404 和 500：404、412、403，429 和 503 带 `Retry-After`，其他回 502。状态码从错误链里读取；drive 的 `downloadQuotaExceeded`、mega 的配额和限流错误以及 HTTP 509 都回 429。
4. **流数上限**（`lib/connlimit`）：`--rc-serve-max-file-streams` 为新的流关闭该文件最久没读的流；`--rc-serve-max-streams` 在一个账号（包装层之下最内层的远端）满额时让打开等待。未设置（0）时取后端的估计值（`MaxFileStreams` 与 `MaxStreams` 特性）：cloud115、baidunetdisk、quark（及 uc）和 weiyun 每文件 2、每账号 4；guangyadrive、mega 和 protondrive 为 2 和 6；cloud123、thunder、pikpak、pcloud 和 iclouddrive 为 3 和 8；aliyundrive 和 yandex 为 4 和 10；drive、onedrive、dropbox 和 box 为 4 和 16；mailru 为 4 和 4（一台下载服务器只服务 4 个流）。其他后端没有上限。-1 关闭上限。
5. **跳转**：`operations/fsinfo` 报告的 `SeekReadsFromStart` 特性标出要从文件开头读才能到达偏移的远端：seafile 的加密资料库，以及 archive 打开的 zip 文件。
6. **后端修复**：服务器对范围请求回 200 时，webdav 和 http 丢弃偏移之前的字节（`lib/readers.SkipToRange`）；http 的错误带上状态码；crypt 在第一次读取后保留文件的 nonce，一次跳转只需一个请求。filelu 用范围请求读取，不再丢弃偏移之前的数据；shade 第一次请求就带上范围；cloud115 的 API 调用、weiyun 与 baidunetdisk 的下载，以及 putio、quatrix、seafile、iclouddrive 和 filescom 的下载错误把 HTTP 状态码交给 pacer 和 rc；baidunetdisk 的下载经过 pacer；thunder 在链接 URL 标明的过期时间之前更新链接；box 不再把令牌发给下载服务器。
7. **链接缓存**（`lib/linkcache`）：保留 `Open` 申请的下载链接，以远端、对象的 ID 或路径及其指纹为键，播放器的多次跳转共用一个链接。链接存活到 API 或 URL 标明的过期时间（S3、GCS 和 Azure 签名、`Expires`、`t`、`auth_key` 等），否则按后端的估计值，最长 24 小时；到寿命的五分之四时更新，更新失败时继续使用仍有效的链接，并发打开共用一次请求。下载被 401、403、404 或 410 拒绝时丢弃链接，换新链接重试一次。使用它的有 cloud115（绑定其 user agent 与 cookie）、aliyundrive（其 `expiration`）、baidunetdisk（`dlink` 重定向到的地址）、cloud123（优先用列举给的链接）、guangyadrive、quark、weiyun、yandex、putio、sharefile、quatrix、ulozto（其 `download_url_valid_until`）、seafile（带 `reuse=1`）、filelu、iclouddrive（优先用列举给的链接）、filescom、mega（SDK 的下载，MAC 校验只做一次）、internxt（分片 URL、密钥与 IV），以及缓存下载地址重定向目标的 onedrive、box、shade 和 internetarchive。pikpak、pcloud 和 thunder 把链接保存在缓存的对象上；protondrive 的 SDK 只能通过它自己的调用读取，fichier 的令牌只能用一次。
