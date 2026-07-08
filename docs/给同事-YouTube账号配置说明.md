# YouTube 账号配置说明（给同事）

你好，麻烦你帮忙把 YouTube 账号准备好，之后我在系统里点一下授权就能对接。
你只需要做**账号那一端**的准备，下面按步骤来即可。

---

## 一句话说明分工

- **你（同事）**：把 YouTube 账号/频道准备好 + 满足一个 Google 端的小设置，然后把账号信息交给我。
- **我**：在系统面板上点「授权 YouTube」，用你准备的账号登录一下就完成对接。

---

## 你需要做的（每个要用的 YouTube 账号做一遍）

### 第 1 步：准备好 YouTube 频道
- 用一个 Google 账号，创建或选定要发布的 YouTube 频道。
- 确认这个 Google 账号能登录 https://studio.youtube.com 并能上传视频（即该频道的拥有者/管理员）。

### 第 2 步：把这个 Google 账号加入系统 OAuth 应用的「测试用户」⚠️ 最关键
> 不做这步，我在系统里授权时 Google 会拦下来报「访问被阻止 / 应用未经验证」。

- 打开 Google Cloud Console：https://console.cloud.google.com
- 选到我们系统用的这个 OAuth 应用所在的项目（应用的客户端 ID 是：
  **`917092367693-n4rbj9bn90rfa13jl88co1gpl6ulvd0k.apps.googleusercontent.com`**，认准这个项目）。
- 左侧进入 **API 和服务 → OAuth 同意屏幕 → 测试用户（Test users）→ 添加用户**。
- 把第 1 步那个 Google 账号的邮箱填进去，保存。（每个要授权的账号都要加，加完即时生效。）

> 如果你没有这个 Google Cloud 项目的访问权限，就把要授权的 Google 账号邮箱发给我，我来加。

### 第 3 步：确认 YouTube Data API 已开启（通常已开，确认即可）
- 同一项目里：**API 和服务 → 已启用的 API** → 确认列表里有 **YouTube Data API v3**。

---

## 你要交回给我的信息

对每个准备好的账号，给我这几项即可：

1. **Google 账号邮箱 + 密码**（我用来在系统里登录授权；如有两步验证请一并告知或先关掉）。
2. 这个账号要发布到**哪个新闻频道 + 哪个语言**（例如：科技新闻 / 中文）。
3. 确认第 2 步的「测试用户」已加好（或把邮箱给我我来加）。

---

## 我这边会怎么做（你了解即可，不用操作）

我在系统面板上：选中对应新闻频道 → 勾对应语言 → YouTube 区块点「授权 YouTube」→
弹出 Google 登录 → 用你给的账号登录并同意 → 显示「✓ 已绑定」→ 完成。
之后这个频道该语言的视频就会自动发布到这个 YouTube 账号。

---

## 固定信息（万一你配 OAuth 时要用）

| 项目 | 值 |
|---|---|
| OAuth 客户端 ID | `917092367693-n4rbj9bn90rfa13jl88co1gpl6ulvd0k.apps.googleusercontent.com` |
| 授权回调地址（重定向 URI，须已登记在该 OAuth 客户端里） | `https://aiagent.office.ihousejapan.cn/api/youtube/oauth/callback` |
| 需要的权限 | 上传/管理 YouTube 视频（youtube.upload、youtube） |

> 如果你打算用**你自己新建的** Google OAuth 应用（而不是上面这个现成的），
> 请把新应用的 **客户端 ID 和密钥** 一并发给我，并在你的应用里登记上面的回调地址，我来把它配进系统。

---

## 可能遇到的问题

| 现象 | 怎么办 |
|---|---|
| 我授权时报「访问被阻止 / This app isn't verified」 | 说明第 2 步没加测试用户，或加错了账号 → 回到第 2 步补加 |
| 报「redirect_uri_mismatch」 | 该 OAuth 客户端没登记回调地址 → 按「固定信息」表补登记 |
| 授权成功但系统提示没拿到 refresh_token | 该账号以前授权过 → 到 https://myaccount.google.com/permissions 撤销后我再重新授权 |

有问题随时问我。谢谢！
