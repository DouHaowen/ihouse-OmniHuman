# iHouse OmniHuman 新闻数字人自动化系统 —— 交接文档

> 面向接手的 AI/工程师。目标:看完这份文档就能理解整个系统、知道每条链路在哪、怎么部署、有哪些已知坑和待办。
> 最后更新:2026-07-10。对应代码 commit `6b8502b`(main 分支)。

---

## 0. 一句话概述

一个 **FastAPI 单体应用**,自动完成:**按新闻频道抓取英文新闻 → AI 写中文口播稿 → 生成"数字人"口播视频 → 自动发布到对应频道的 YouTube / Facebook**。另有"话题(topic)"和"房源(property/房产实拍)"两条并行的自动化产线。数字人视频生成跑在一台独立的 5090 GPU 机器上。

---

## 1. 基础设施与网络拓扑

### 1.1 三个位置
| 角色 | 访问方式 | 说明 |
|---|---|---|
| **本地开发机(macOS)** | 本仓库所在:`/Users/saita/saita/ihouse-OmniHuman` | 改代码、git;**沙箱访问不了 github**,推送要在用户自己终端做 |
| **主机 / 生产(ARM64)** | `ssh -p 8800 service@ihouseoffice.ddns.net`(即 192.168.0.88) | 跑应用容器 `OmniHuman`;主机源码目录 `/home/saita/ihouse-OmniHuman` |
| **5090 GPU 机(x86_64)** | `ssh -p 43612 saita@office.ihousejapan.cn` | 跑数字人生成(InfiniteTalk)和 TTS(Qwen3-TTS) |

### 1.2 应用容器(主机上)
- 容器名 **`OmniHuman`**,应用监听容器内 **3010** 端口。
- 对外链路:`https://aiagent.office.ihousejapan.cn` → **NPM 反代**(`/data/nginx/proxy_host/11.conf`,`client_max_body_size 3g`)→ `192.168.0.88:8001` → 容器 `:3010`。
- 代码通过 Dockerfile `COPY . .` **烤进镜像**(不是 volume 挂载);只有 `output/` 和 `assets/` 是 volume(见 §8 部署机制,**这是最容易踩的坑**)。

### 1.3 5090 上的服务(systemd 用户服务)
| 服务 | 端口 | systemd unit | 作用 |
|---|---|---|---|
| InfiniteTalk 数字人 | 8893 | `ihouse-infinitetalk.service` | 图片+音频→口播视频 |
| Qwen3-TTS 配音 | 8895 | `ihouse-qwen3-tts.service` | 文本→语音 |
| GPU 编排器 | 8898 | (gpu_orchestrator_5090.py) | 按"档位"切换上面两个服务(抢显存,见 §7) |

管理服务用 `systemctl --user`(需 `export XDG_RUNTIME_DIR=/run/user/$(id -u)`)。**不要用 pkill 手动重启**——会和 systemd 打架、触发 StartLimitBurst(5分钟崩3次就停用),正确做法是 `systemctl --user restart ihouse-infinitetalk.service`。

---

## 2. 代码结构(关键文件)

| 文件 | 职责 |
|---|---|
| `app.py`(~17k 行) | FastAPI 主体:所有路由、调度器、生产/发布编排、GPU 资源队列 |
| `templates/index.html` | 单页前端(原生 JS),含频道配置面板、话题/房源面板、各账号授权入口 |
| `opennews_trends.py` | 英文新闻抓取(GDELT / NewsData / Bing),**类目题材过滤(best-fit)** |
| `opennews_batch.py` | 批次抓取、seen.json 去重(精确/标题相似/事件 token) |
| `opennews_admin.py` | 候选搜索封装 |
| `topic_auto.py` | 话题自动化:话题接口拉取、topic_state 去重/重试 |
| `property_auto.py` | 房源自动化:物件接口拉取、下载实拍视频、状态管理 |
| `youtube_publisher.py` | YouTube 上传、OAuth 应用凭据(面板可填 client_id/secret) |
| `facebook_publisher.py` | Facebook Page 视频发布、OAuth |
| `x_browser_publisher.py` / `x_browser_login_manager.py` | X 浏览器自动化发布(ARM64 上因缺 H.264 编解码基本不可用) |
| `infinitetalk_avatar_client.py` | 调 5090 的 8893 生成数字人(提交+轮询到完成) |

### 2.1 运行期数据目录(容器内 `/app/output/`,volume 持久化)
- `opennews_batches/` — 频道配置 `channels_config.json`(`OPENNEWS_CHANNELS_CONFIG_PATH`)、`seen.json`(抓取去重记忆)、`batch_config.json`、`batch_jobs/`(生产任务)
- `youtube_auth/` — `youtube_token.json`(全局)+ 每频道 `youtube_token_<channel>_<lang>.json` + `oauth_app_config.json`
- `facebook_auth/` — `facebook_token.json`(全局)+ 每频道 token + `publish_state.json`(368 冷却/节流状态,**按 Page 分桶**)
- `x_auth/`、`topic_auto/`、`property_auto/`

---

## 3. 核心链路:OpenNews 频道新闻自动化

这是系统的主线。理解这条链路 = 理解 80% 的系统。

### 3.1 频道模型
频道配置在 `channels_config.json`,顶层 `scheduler_enabled`(总开关)+ `channels[]`。每个频道字段:
```
id, name, enabled, category, keyword, time_range, interval_minutes,
limit(抓取量), produce_limit(每批制作数), next_run_at,
languages:[cn/jp/en], platforms:{x,facebook,youtube},
accounts:{ <lang>: { x:{...}, facebook:{page_id,page_access_token,enabled}, youtube:{token_store_path,channel_name,enabled} } }
```
**当前启用的频道**:
- `technology`(科技新闻)→ YouTube「**OpenNews 科技前沿**」+ Facebook「OpenNews」页
- `real_estate_immigration`(房产移民)→ YouTube「**OpenNews 房产观察**」
- `general`(综合,**已禁用**)、`military`/`politics`/`finance`(禁用)

`category` 决定抓取用哪套内置查询词(见 `opennews_trends.py` 的 `TREND_CATEGORIES` / `NEWSDATA_*_MAP`)。合法类目在 `opennews_batch.py::VALID_CATEGORIES`。

### 3.2 调度器
- `_start_opennews_channel_scheduler`(poll 20s):遍历 `_opennews_channel_scheduler_due_channels`(enabled 且 next_run_at 到期),对每个频道调 `_run_opennews_channel_fetch`。
- `_start_opennews_batch_scheduler`(启动时):把 general/两小时通用自动化 config 设成 `enabled=False`(**通用自动化默认关闭**),然后启动频道调度器、compose-ready 恢复 worker、话题/房源调度器。
- **前端**:「OpenNews 频道自动化总控」卡片 = `scheduler_enabled` 总开关 + 一键跑全部启用频道;频道编辑器有"抓取间隔"「▶ 立即运行本频道」。

### 3.3 抓取(fetch)
`_run_opennews_channel_fetch` → `run_opennews_batch_fetch_once`(opennews_batch.py)→ `search_english_trends`(opennews_trends.py):
1. **源**:NewsData(免费版,已加 `nextPage` 分页,`NEWSDATA_MAX_PAGES` 默认3)+ GDELT(单次可 180 条,但**从生产服务器 IP 持续 429/超时,基本不可用**)+ Bing(仅当 <8 条时兜底)。
2. **类目题材过滤(best-fit)**:`_filter_articles_by_category_relevance` —— 一条新闻按各类目题材词命中数,归给命中最多的那一类;只有属于本类目才留。**这是防串台的第一层**(修复了"NewsData business 大类返回一堆跑题商业新闻")。
3. 聚类打分 → 中文转译 → 返回 candidates。
4. **seen.json 去重**(opennews_batch.py):精确 key / url / 标题 / 标题相似(`TITLE_SIMILAR_OVERLAP_MIN=7`,`TITLE_SIMILAR_RATIO=0.72`,env 可调)/ 事件 token。新条目写入 seen(记忆 14 天)。产量低时**先查源头 raw_count**,多半是源被限流,不是去重太严。

### 3.4 产出(produce)—— `_handle_opennews_batch_after_fetch`(after-fetch 回调)
1. `items` = 本批新增(未 seen)条目。
2. **题材过滤(best-fit)** `_filter_items_for_channel_topic(items, channel)`:混合新闻归主导题材,非本频道题材剔除(**防串台第二层**;关键词表 `_OPENNEWS_CHANNEL_TOPIC_KEYWORDS`,含 overlap 处理)。
3. `_select_opennews_auto_collection_items(items, category=...)`:
   - **科技类目(technology/ai/all)** 才走 `ai/robotics/other` 强制配比 + `_supplement_opennews_auto_collection_items` 补抓;
   - **其它频道(房产等)直接用已过滤的本题材料**(**防串台第三层**——修复了"给房产频道补抓 ai/robotics 科技料塞进 job")。
4. **完成事件去重**(`_opennews_recent_completed_event_identities(channel_id=...)`,**按频道隔离**):把已做过的同频道事件剔除,`job_items` 为空则跳过。
5. 拿 `OPENNEWS_BATCH_AUTO_PRODUCE_LOCK`(**全局串行,一次只产一个批次**)→ `create_opennews_batch_job` → 起线程跑 `_run_opennews_external_produce_job`。
6. 生产内部:每条 → 写口播稿 → **Qwen3-TTS 配音(切 tts 档)** → **InfiniteTalk 数字人(切 digital 档)** → 合成竖屏成片。生产完成走发布。

### 3.5 发布(publish)—— `_auto_publish_opennews_result_data`(所有发布路径的总入口)
由 `_schedule_opennews_post_compose_publish`(带 `.publish.lock`)在生产尾部调用。守卫(按顺序):
1. **未归属频道不发**:`channel_id=="general"` 或频道未配置 → 全部平台跳过(**防"general 混合料走全局 token 冒到别的频道"**)。
2. **平台开关**:`youtube/facebook/x_auto_publish`(频道 platforms + 全局 disabled env)。
3. **幂等**:重读盘上已发记录,某平台已发过就不重发。
4. **跨目录去重**:同一条新闻(**事件指纹 或 源文章 URL**)已发到本频道 → 不重发(`_opennews_channel_published_event_keys`,**防"同一条发两遍"**)。
5. 发布:`_publish_opennews_result_to_youtube/_facebook/_x`,用 `_opennews_youtube_token_path_for(channel, market)` 解析**对应频道的 token**。
   - YouTube:竖屏发 Shorts(`_build_youtube_shorts_metadata`,#Shorts)。
   - Facebook:**只发主语言(中文)单帖**(`OPENNEWS_FACEBOOK_PUBLISH_LANGUAGE_VERSIONS_ENABLED=0`)+ 最小间隔节流(`OPENNEWS_FACEBOOK_MIN_POST_INTERVAL_SECONDS`)+ **撞 368 自动冷却**(`_facebook_*` 一族,按 Page 分桶)。

### 3.6 compose-ready 恢复 worker(`_recover_ready_compose_histories_once`)
补救"成片好了却没发/中途失败"的旧目录。守卫:只处理 2 天内的、已发过的跳过、最近 15 分钟改动过的跳过、**本进程同一目录只重合成一次**(`_COMPOSE_READY_RECOVERY_ATTEMPTED`,防死循环)。注意它也走 §3.5 发布入口,所以 general 旧积压不会再被它发出去。

---

## 4. 话题自动化 & 房源自动化(两条并行产线)

- **话题(topic)**:`topic_auto.py` + `_start_topic_auto_scheduler`。定时拉话题接口 → 逐条做数字人视频 → 发到 **iHouse株式会社** 的 YouTube(`youtube_token_topic_auto.json`)和 Facebook(iHouse株式会社 Page)。配置 `topic_auto/` 下,前端「话题自动化」面板有 YouTube/Facebook 自动发布开关。
- **房源(property/房产实拍)**:`property_auto.py` + `_start_property_auto_scheduler`。拉物件接口(`bukken.office.ihousejapan.cn/api/properties/all`)→ 下载实拍视频 → AI 写文案 → 做"房源实拍成片"(默认中国市场/简体中文/温柔女声/不用BGM)→ **共用话题的同一个 YouTube 账号和 FB Page** 发布,只发 Shorts / 中文单帖。
- 两者发布走各自的 `_maybe_publish_{topic,property}_video_to_{youtube,facebook}`,**不走** §3.5 那个 OpenNews 频道入口。

---

## 5. 发布对接现状

| 平台 | 状态 |
|---|---|
| **YouTube** | ✅ 通。每频道独立 token/子频道。OAuth 应用凭据可在面板填(`/api/youtube/oauth-app`),授权时 `prompt=select_account%20consent`。 |
| **Facebook** | ⚠️ 机制通(房源→iHouse株式会社页实测发成功),但**科技频道用的 OpenNews 页被 Facebook 368 频率封锁**(历史被通用自动化刷太多)。App 凭据在主机 `.env`(`FACEBOOK_APP_ID/SECRET`,当前用应用 `opennewsagent` 1720498229294212)。**FB App Secret 必须 32 位十六进制**。 |
| **X(Twitter)** | ❌ ARM64 上 Playwright chromium 缺 H.264、系统 chromium 崩,视频发不了。用户已决定暂缓 X。 |

---

## 6. 去重与串台防线(汇总,面试重点)

**串台三层**:①源头 best-fit 过滤 → ②产出前 best-fit 过滤 → ③非科技频道不补抓跨题材;**外加**发布时"未归属频道不发"。
**重复多层**:①seen.json(抓取级)→ ②完成事件去重(按频道隔离)→ ③发布幂等(同目录)→ ④跨目录去重(事件指纹 + 源URL)。

---

## 7. 5090 GPU 编排(数字人为什么慢/服务为什么"老挂")

- InfiniteTalk(8893)和 Qwen3-TTS(8895)**抢显存、不能同时在显存里**。GPU 编排器(8898)按"档位"切:`digital`=起 InfiniteTalk;`tts`/`idle`=**停 InfiniteTalk** 起 TTS。容器每个 GPU 任务带 profile 去 `_run_with_5090_gpu_resource`(全局串行队列)抢卡。
- **坑**:早先每段数字人做完都切回 restore 档(停 InfiniteTalk),下一段又重启 → 反复 stop/start、慢且易被打断。已加 **keep-warm**(`LOCAL_DIGITAL_HUMAN_KEEP_WARM=1`,默认开):数字人任务做完**不切回**,保持 InfiniteTalk 常驻,下一个 TTS 任务需要显存时才切档停它 → 连续数字人任务零重启。
- **InfiniteTalk 很慢**:跑在 offload 慢档(`num_persistent_param_in_dit=0`,每步 ~68 秒,一条 ~9-14 分钟)。提速点:调高 `INFINITETALK_AVATAR_PERSISTENT_DIT`(env,让 DiT 常驻显存),但要在 5090 上小心防 OOM。**未做,是明确的待办/可选提速项。**
- **服务"老挂"其实是被编排器/被我手动 pkill 停的**,不是崩溃(journal 无 OOM/异常)。管理一律用 `systemctl --user restart`,别 pkill。

---

## 8. 部署机制 ⚠️(最容易踩的坑,务必读)

代码烤进镜像(`COPY . .`),`output/`+`assets/` 才是 volume。所以:
- `docker cp 文件 OmniHuman:/app/...` + `docker restart` 只改**运行中的容器**,是**临时的**——任何 `docker compose up -d`(会 Recreate 容器)都会**从旧镜像回退,丢掉 docker cp 的改动**。
- 改 `.env`(env_file)需要 `docker compose up -d` 才生效,但这一步会把 docker cp 的代码回退。

**正确固化流程**:
```bash
# 本地改完 → 同步到主机源码目录
scp -P 8800 app.py opennews_trends.py ... service@ihouseoffice.ddns.net:/home/saita/ihouse-OmniHuman/
# 主机上
cd /home/saita/ihouse-OmniHuman
docker compose build      # .dockerignore 已排除 output,构建很快
docker compose up -d      # 用新镜像重建,代码+env 都到位、重建不回退
```
GitHub 推送:**沙箱到不了 github.com**,必须在用户自己终端 `git push origin main`。

---

## 9. 本轮已修复问题清单(近期 commit)

| commit | 内容 |
|---|---|
| `6b8502b` | 未归属频道(general)不发布 + 按源URL跨目录去重 |
| `e550448` | ai/robotics 配比+补抓 只用于科技类频道(根治给房产补抓科技) |
| `2df3522` | 题材过滤改 best-fit(混合新闻归主导题材) |
| `494bed9` | 源头按类目题材过滤抓取结果 |
| `010901c` | 频道题材过滤 + 跨目录已发事件去重 |
| `c9de96b` | 修 OpenNews 视频重复发布(幂等) |
| `bb48495` | 修 compose-recovery 死循环 |
| `6b7cdbc` | 去重历史按频道隔离(房产频道被别频道历史误杀而不产) |
| `8c1f667` | 5090 数字人 keep-warm |
| `87e1a79`/`2ff55e3` | FB 只发中文单帖+368自动冷却;FB OAuth 回调错误显示 |
| 更早 | 话题/房源自动化、FB接入、频道面板、YouTube自助凭据、去重放宽、类目词库扩容、房产移民频道 等 |

---

## 10. 已知问题 / 待办(交给下一个 AI)

1. **GDELT 从生产服务器 IP 持续 429/超时** → 精准新闻源基本不可用,产量靠 NewsData 撑。可考虑:NewsData 付费版 / 加 Google News RSS 等更多免费源 / 想办法换 GDELT 出口。
2. **NewsData 免费版有速率上限**,密集测试会 429(科技频道会短暂"无新片")。别短时间大量抓取。
3. **InfiniteTalk 慢(offload 档)** → 提速点见 §7(调 `INFINITETALK_AVATAR_PERSISTENT_DIT`,防 OOM)。fix3(批内先集中 TTS 再集中数字人,减少切档)也未做。
4. **FB OpenNews 页被 368 封**(历史刷太多)→ 科技频道 FB 暂时发不进;等解封 / 换干净的 Page / 减少发帖。
5. **历史遗留脏数据**:YouTube 各频道上有修复前产生的重复、串台视频(如科技前沿的 AMD/SambaNova 重复、房产观察的 Meta 数据中心)。代码只防今后,**已上传的需人工在 YouTube Studio 删**。
6. **general 旧积压**仍会被恢复 worker 重合成(浪费一点 5090),但已"不发布",无污染;可进一步让恢复 worker 跳过 general 目录。
7. **X 发布**在 ARM64 上没解决(缺 H.264)。
8. **5090 服务稳定性**:靠 systemd,别 pkill;StartLimitBurst=3/300s,别短时间反复重启。

---

## 11. 诊断工具

容器内 `/app/tools/` 下有大量 `_diag_*.py`、`_check_*.py`、`_verify_*.py`、`_healthcheck_channels.py`(**本仓库 tools/ 下也有,多为未跟踪的临时脚本**)。常用:
- `_healthcheck_channels.py` — 两频道端到端体检(配置/绑定/抓取题材/发布路由)
- `_diag3problems.py` — 查两频道发布记录的重复/串台
- `_diag_tech_volume.py` — 查科技抓取量/源限流
运行:`docker exec OmniHuman sh -c 'cd /app && PYTHONPATH=/app python3 tools/xxx.py'`。写复杂脚本时**用文件+docker cp**,别用嵌套引号的 `python3 -c`(容易 EOF 崩)。

---

## 12. 安全约束(务必遵守)

- **不要把任何 token / 密钥 / .env 提交到 git 或写进记忆**。所有敏感凭据只放服务器端 `.env` 或 `output/*_auth/` 下的 token 文件。
- 给前端返回配置时抹掉密钥(见 `_sanitize_topic_auto_config_for_client`、FB `page_access_token` 脱敏)。
- 用户给的明文密码/token 视为弱凭据,别持久化进代码。

---

## 13. 常用命令速查

```bash
# 进主机 / 看容器
ssh -p 8800 service@ihouseoffice.ddns.net
docker logs --since 5m OmniHuman | grep -iE 'auto collection|topic filter|dedup|publish guard'
docker exec OmniHuman curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:3010/

# 部署(固化)
cd /home/saita/ihouse-OmniHuman && docker compose build && docker compose up -d

# 5090
ssh -p 43612 saita@office.ihousejapan.cn
export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user restart ihouse-infinitetalk.service
curl -s http://127.0.0.1:8893/health

# 触发某频道抓取(容器内)
# 改 channels_config.json 里该频道 next_run_at=0,调度器 20s 内会跑
```

---

*完。有不清楚的地方,优先读 `app.py` 里对应函数(函数名在本文都点名了),以及 `docs/` 下其它文档(给同事的 YouTube 配置文档等)。*
