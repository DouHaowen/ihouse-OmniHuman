# YouTube 账号配置与对接文档

> 目标：把每个 YouTube 账号（频道）对接到 iHouse OpenNews 系统里**指定的新闻频道 + 指定语言**，
> 之后系统自动做出来的视频会自动发布到对应账号。**全程在系统面板点按钮完成，不用改代码。**

---

## 一、整体原理（先理解）

- 系统已经内置好一个 **Google OAuth 应用**（client_id / secret 已配置在系统里），**同事不需要新建 OAuth 应用**。
- 对接一个 YouTube 账号 = 在系统面板上点「授权 YouTube」→ 用管理该 YouTube 频道的 **Google 账号**登录授权一次。
- 授权时系统会带上「哪个新闻频道、哪个语言」，回来后**把该账号绑定到那个频道+语言的槽位**，各不干扰。
  - 例：`科技新闻 / 中文` 绑 A 账号，`科技新闻 / 英文` 绑 B 账号，`军事新闻 / 中文` 绑 C 账号，互不影响。

---

## 二、同事需要在 Google 端准备的（账号端配置）

对**每一个**要用来发布的 YouTube 账号：

1. **确认该 Google 账号能管理目标 YouTube 频道**
   - 用该 Google 账号登录 https://studio.youtube.com ，能进入并上传视频即可。
   - 如果是品牌账号（Brand Account），确保该 Google 账号在该频道的权限是「拥有者/管理员」。

2. **把该 Google 账号加入 OAuth 应用的「测试用户」** ⚠️ 关键，最容易卡在这里
   - 系统的 Google OAuth 应用如果处于「测试(Testing)」状态，**只有被加入测试用户名单的 Google 账号才能授权成功**，否则授权时会报「应用未经验证 / 访问被阻止(access blocked)」。
   - 处理方式（由持有该 Google Cloud 项目的人操作，通常就是提供 client_id 的人）：
     - 打开 Google Cloud Console → **API 和服务 → OAuth 同意屏幕 → 测试用户 → 添加用户** → 填入要授权的 Google 账号邮箱。
   - 每个要授权的账号都要加进去。加完立即生效。
   - （备选：把应用发布为「正式(In production)」，但 `youtube.upload` 属敏感权限，需 Google 审核，可能数周，**不推荐**，用测试用户最快。）

3. **确认项目已开启 YouTube Data API v3**
   - Google Cloud Console → API 和服务 → 已启用的 API → 应有「YouTube Data API v3」。（系统现有账号能发说明已开启，一般无需再动。）

> 如果同事想用**自己新建的** Google OAuth 应用（而不是现有的），需要把新应用的
> **client_id / client_secret** 交给我方配置到系统，并在该应用里登记下面第三节的回调地址。
> 否则用现有应用即可，跳过此项。

---

## 三、给同事的关键固定信息

| 项目 | 值 |
|---|---|
| OAuth 回调地址（必须已登记在 Google OAuth 客户端的「已获授权的重定向 URI」里） | `https://aiagent.office.ihousejapan.cn/api/youtube/oauth/callback` |
| 申请的权限范围 | `https://www.googleapis.com/auth/youtube.upload` 和 `https://www.googleapis.com/auth/youtube` |
| 授权入口 | iHouse 面板 → OpenNews 新闻视频 → 频道面板 → ④ 发布账号 → YouTube → 「授权 YouTube」 |

---

## 四、在系统面板上对接（每个频道 × 每个语言各做一次）

1. 登录 iHouse 面板 → 进入 **OpenNews 新闻视频** 页面 → 找到**频道面板**。
2. 左侧选中要配置的**频道**（如「科技新闻」）。
3. 右侧 **② 语言版本** 勾选要发布的语言（如「中文」）。
4. 右侧 **④ 发布账号** 里找到该语言下的 **YouTube** 区块 → 点 **「授权 YouTube」**。
5. 弹出 Google 登录窗口 → **用管理目标 YouTube 频道的 Google 账号登录** → 同意授权。
   - ⚠️ 若该浏览器已登录了别的 Google 账号，先在弹窗里用「使用其他账号」切到正确账号。
6. 成功后页面提示「YouTube 授权成功，已绑定到频道 X / 语言 Y」，面板对应位置显示 **「✓ 已绑定：<频道名>」**。
7. 换下一个频道 / 语言，重复第 2~6 步，绑不同账号。
8. 全部绑完后，点 **「保存频道配置」**（保存不会覆盖已授权的账号）。

---

## 五、怎么验证对接成功

- 面板上该频道该语言的 YouTube 显示 **「✓ 已绑定：<频道名>」** 即成功。
- 之后该频道产出的新视频会自动发到该账号；可在对应 YouTube 频道的「内容」里看到新上传的视频。
- 如需换账号：点该位置的「解绑」，再重新「授权 YouTube」即可。

---

## 六、常见问题

| 现象 | 原因 / 处理 |
|---|---|
| 授权时「访问被阻止 / 应用未验证 / This app isn't verified」 | 该 Google 账号没加进 OAuth 应用测试用户名单 → 见第二节第 2 步 |
| 「redirect_uri_mismatch」 | Google OAuth 客户端里没登记第三节的回调地址 → 补登记 |
| 授权成功但没返回 refresh_token | 该账号之前授权过 → 到 https://myaccount.google.com/permissions 撤销后重新授权 |
| 绑定后仍未发布 | 确认该频道在面板「已启用」、③发布平台勾了 YouTube、④该语言 YouTube 勾了「发布」 |

---

*当前状态：系统已有 Google OAuth 应用配置完毕，「科技新闻/中文」已在自动发布 YouTube。
其余频道/语言按本文第四节自助授权即可接入。*
