# iHouse OmniHuman 系统交接文档

最后更新：2026-07-07  
项目名：`ihouse-OmniHuman`  
GitHub：<https://github.com/DouHaowen/ihouse-OmniHuman>  
本地工作目录：`/Users/saita/saita/ihouse-OmniHuman`

> 这份文档用于交给新的 AI / 开发者接手。本文不写入明文 API Key、OAuth token、服务器密码；敏感信息只说明配置位置。

## 1. 系统一句话概览

这是一个短视频内容生产与自动发布系统，核心能力包括：

- 普通销售选题视频：用户输入话题，生成文案、配音、数字人、素材段、字幕和成片。
- 管理员批量数字人：管理员给一个主题方向，系统生成多个选题并排队生产数字人视频。
- OpenNews 新闻视频：按频道抓取热点新闻，生成中文/日文/英文新闻短视频，目前自动发布主线只发布中文竖屏到 X 和 Facebook。
- 房源实拍成片：上传房源实拍素材，分析内容、生成时间轴、配音、字幕、BGM 并成片。
- 后台服务面板：展示主站、5090 服务、OpenNews 队列、发布账号、GPU/队列等状态。

主站是一个 FastAPI + Jinja/原生 JS 单体应用，绝大部分业务编排在 `app.py`，前端主页面在 `templates/index.html`。

## 2. 代码仓库与关键文件

本地仓库：

```bash
cd /Users/saita/saita/ihouse-OmniHuman
```

关键文件：

- `app.py`：主 FastAPI 应用。包含登录、任务队列、普通视频生产、OpenNews、发布、后台服务面板、历史记录等大部分路由和编排逻辑。
- `templates/index.html`：主前端页面，包含当前任务、历史记录、OpenNews、批量数字人、房源实拍、管理员入口等 UI。
- `templates/admin.html`：管理员看板入口。
- `templates/admin_services.html`：后端服务面板页面。
- `templates/opennews_data_center.html`：OpenNews 数据中心独立页面。
- `generate_script.py`：普通数字人/销售短视频文案生成，包含固定三段数字人后处理规则。
- `opennews_admin.py`：OpenNews 新闻抓取、新闻稿生成、多语言稿处理、新闻源解析。
- `fetch_materials.py`：免费素材 API/网页素材抓取、素材匹配、OpenNews 素材策略。
- `video_composer.py`：视频合成、字幕、竖屏/横屏排版。
- `generate_audio.py`：MiniMax TTS。当前用于普通数字人和批量数字人配音。
- `infinitetalk_avatar_client.py`：调用 5090 InfiniteTalk 数字人生成服务。
- `infinitetalk_avatar_api_server.py`：5090 端 InfiniteTalk API 服务代码。
- `x_browser_publisher.py`：X 浏览器自动化发布，替代 X API 发布。
- `x_browser_login_manager.py`：X 服务器端可视化登录/VNC/noVNC 管理。
- `facebook_publisher.py`：Facebook Graph API OAuth、Page token、视频发布和数据读取。
- `youtube_publisher.py`：YouTube 发布逻辑，目前自动化新闻主线暂停使用。
- `property_video_workflow.py` / `property_video_vision.py`：房源实拍视频工作流。
- `material_library.py`：素材库逻辑。当前产品规则下，素材库主要只保留房源实拍 BGM 管理。
- `opennews_batch.py` / `opennews_scheduler.py` / `opennews_trends.py` / `opennews_collections.py`：OpenNews 批次、定时、热点和旧合集能力。
- `tools/qwen3_tts_watchdog.py`：5090 Qwen3-TTS watchdog。
- `tools/gpu_orchestrator_5090.py`：5090 GPU 资源编排工具。
- `deploy/5090/*.service`：5090 服务器 systemd user service/timer 配置样板。

重要目录：

- `assets/`：主播图、Logo 等静态资产。
- `output/`：生产结果、历史记录、OAuth token、OpenNews 批次、X 浏览器 Profile 等运行数据。
- `material_library/`：历史素材库目录；当前只应保留房源实拍 BGM 相关使用。
- `docs/`：项目文档。

## 3. 生产服务器

主生产服务器连接方式：

```bash
ssh -4 service@ihouseoffice.ddns.net -p 8800
```

生产主机项目目录：

```bash
/home/saita/ihouse-OmniHuman
```

生产 Docker 主容器：

```text
OmniHuman
```

容器内运行代码路径：

```bash
/app
```

容器内模板路径：

```bash
/app/templates
```

生产主站端口：

- 宿主机 `3010` -> 容器 `3010`
- 线上域名通常走反代：`https://aiagent.office.ihousejapan.cn`

X 浏览器登录/noVNC 辅助端口：

- 宿主机 `6090` -> 容器 `6090`
- 常用访问形态：`https://aiagent.office.ihousejapan.cn:6090/vnc.html?autoconnect=1&resize=remote`

当前 `docker ps` 中与本项目直接相关的容器：

- `OmniHuman`：主应用，镜像 `ihouse-omnihuman-omnihuman`。
- `voice-gateway`：语音网关容器，端口 `8090`，不是主业务入口。

同机还有 Dify、Nginx Proxy Manager、ddns-go、registry cache 等容器，不是本项目主线，除非排查服务器网络/反代，否则不要随意改。

## 4. 生产部署规则

非常重要：线上改代码时，不要只改主机目录。主容器运行的是容器内 `/app`，必须同步到容器后重启。

常用部署流程：

```bash
# 1. 本地检查
python3 -m py_compile app.py generate_script.py opennews_admin.py fetch_materials.py video_composer.py

# 2. 传到生产主机
scp -4 -P 8800 app.py service@ihouseoffice.ddns.net:/home/saita/ihouse-OmniHuman/app.py

# 3. 从主机复制进容器
ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker cp /home/saita/ihouse-OmniHuman/app.py OmniHuman:/app/app.py"

# 4. 容器内语法检查
ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'python -m py_compile /app/app.py /app/generate_script.py /app/opennews_admin.py'"

# 5. 重启主容器
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker restart OmniHuman"
```

如果改前端但页面不生效，优先检查：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'ls -l /app/templates/index.html && grep -n \"你改的关键字\" /app/templates/index.html | head'"
```

常用排查命令：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker logs --tail 200 OmniHuman"

ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'python -m py_compile /app/app.py /app/generate_script.py /app/opennews_admin.py'"

ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'ls -la /app/output | tail'"
```

## 5. 5090 服务器

5090 服务器用途：

- 本地数字人模型服务：InfiniteTalk。
- OpenNews 本地配音：Qwen3-TTS。
- GPU 资源编排和 watchdog。

连接方式：

```bash
ssh -p 43612 saita@office.ihousejapan.cn
```

密码不要写进仓库文档，请通过安全渠道单独提供。

生产主站访问 5090 的常见内网地址：

```text
192.168.0.34
```

5090 当前保留目录：

- `/home/saita/InfiniteTalk`
- `/home/saita/qwen3-tts-service`
- `/home/saita/ihouse-OmniHuman`
- `/home/saita/ihouse-gpu-orchestrator`
- `/home/saita/logs`
- `/home/saita/miniforge3`
- `/home/saita/snap`

已清理/不再作为主线保留的内容：

- ComfyUI 相关服务/目录。
- 旧 `models` 目录。
- 旧 `cleanup-backup-*` 目录。
- 旧 `crontab.backup.*`。

当前 5090 systemd user 服务：

- `ihouse-qwen3-tts.service`：本地 Qwen3-TTS 服务。
- `ihouse-qwen3-tts-watchdog.timer` / `ihouse-qwen3-tts-watchdog.service`：TTS watchdog，负责健康检查和必要时重启。
- `ihouse-gpu-orchestrator.service`：GPU 资源编排服务。
- `ihouse-infinitetalk.service`：InfiniteTalk 服务配置样板在仓库 `deploy/5090/` 中；实际运行状态以 5090 上 `systemctl --user` 为准。

常用 5090 检查命令：

```bash
ssh -p 43612 saita@office.ihousejapan.cn "nvidia-smi"

ssh -p 43612 saita@office.ihousejapan.cn \
  "systemctl --user --no-pager --type=service --state=running"

ssh -p 43612 saita@office.ihousejapan.cn \
  "systemctl --user status ihouse-qwen3-tts.service --no-pager"

ssh -p 43612 saita@office.ihousejapan.cn \
  "systemctl --user status ihouse-qwen3-tts-watchdog.timer --no-pager"

ssh -p 43612 saita@office.ihousejapan.cn \
  "ls -la /home/saita/InfiniteTalk/api_jobs | tail"
```

InfiniteTalk 失败日志通常在：

```bash
/home/saita/InfiniteTalk/api_jobs/<job_id>/run.log
```

## 6. 运行配置与密钥位置

生产 `.env`：

```bash
/home/saita/ihouse-OmniHuman/.env
```

容器内 `.env`：

```bash
/app/.env
```

敏感项不要提交 Git。需要检查时只打印 key 是否存在，不打印值：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'grep -E \"^(MINIMAX_API_KEY|OPENAI_RELAY_API_KEY|FACEBOOK_APP_ID|X_CLIENT_ID|OPENNEWS_QWEN_TTS_TOKEN)=\" /app/.env | sed \"s/=.*$/=hidden/\"'"
```

当前重要配置规则：

- 文案模型统一走接口模型：
  - Base URL：`https://api.office.ihousejapan.cn`
  - API Key 在 `.env`，不要写入代码或文档。
  - `.env.example` 中相关项：`FORCE_SCRIPT_MODEL_PROVIDER=api_relay`、`OPENAI_RELAY_BASE_URL`、`OPENAI_RELAY_MODEL`、`OPENNEWS_MODEL_PROVIDER=relay`。
- 普通数字人和批量数字人配音：MiniMax TTS。
- OpenNews 新闻配音：只走 5090 Qwen3-TTS，不使用 MiniMax 兜底。
- 数字人视频生成：5090 InfiniteTalk。
- OpenNews 自动发布：当前只发布中文竖屏到 X 和 Facebook；YouTube 暂停。
- 联网检索功能：目前先不用，不作为当前主线。

## 7. 登录、权限和角色

用户配置在 `app.py` 的 `USERS` 常量附近。当前是应用内账号体系，不是外部数据库账号体系。

管理员账号能力：

- 可见管理员看板。
- 可见后端服务面板。
- 可见 OpenNews 频道/账号配置。
- 可见批量数字人功能。
- 可管理房源实拍 BGM。
- 可查看更多历史/任务状态。

员工账号能力：

- 普通视频选题生产。
- 房源实拍成片。
- 查看自己的当前任务和历史记录。
- 不应看到管理员批量数字人功能。

数字人权限规则曾多次调整，目前开发时要遵守最新产品意图：

- 单独数字人和批量数字人：配音用 MiniMax，数字人生成用 5090 InfiniteTalk。
- 每个任务要使用用户当时选择的主播图和音色，不要错误绑定 OpenNews 主播或默认主播。
- 员工侧不要暴露管理员专用配置项。

## 8. 主页面和主要入口

主站：

```text
https://aiagent.office.ihousejapan.cn/
```

管理员看板：

```text
/admin/dashboard
```

后端服务面板：

```text
/admin/services
```

OpenNews 数据中心：

```text
/opennews/data-center
```

OpenNews 小程序/外部接入相关：

```text
/lab/opennews
/lab/apps/opennews
/lab/opennews/manifest.json
/api/external/opennews/health
/api/external/opennews/candidate-batches
/api/external/opennews/produce-selected
/api/external/opennews/ready-videos
```

发布状态和账号：

```text
/api/x/status
/api/facebook/status
/api/opennews/accounts/summary
/api/opennews/channels/config
```

## 9. 普通数字人视频流程

入口：主页面“视频选题”/普通生产。

简化流程：

1. 用户输入选题。
2. 选择市场、部门、文案模型、音色、主播图、数字人引擎等。
3. `generate_script.py` 生成脚本。
4. 脚本后处理强制固定 3 段数字人：
   - 第 1 段必须是数字人。
   - 中间必须有 1 段数字人。
   - 最后一段必须是数字人。
   - 中间数字人段必须尽量短，只负责承上启下和露脸。
   - 其他信息优先走素材段。
   - 这是“固定 3 段”，不是“最多 3 段”。
5. MiniMax TTS 生成每段配音。
6. 数字人段提交 5090 InfiniteTalk 排队生成。
7. 素材段走免费素材 API 匹配。
8. `video_composer.py` 合成最终视频、字幕、封面等。
9. 结果写入 `output/<history_id>/result.json`，前端历史记录读取。

关键文件：

- `generate_script.py`
- `generate_audio.py`
- `infinitetalk_avatar_client.py`
- `fetch_materials.py`
- `video_composer.py`
- `app.py`

注意：

- 不要只改 prompt 来修数字人段结构，必须检查 `generate_script.py` 里的固定三段后处理逻辑。
- 任务取消时，需要同时尝试取消 5090 InfiniteTalk 外部 job，否则 UI 停了但 5090 可能还在跑。
- 运行中任务主要在内存 `tasks`，容器重启会丢失“正在跑”的内存状态，但已落盘的历史结果还在 `output/`。

## 10. 管理员批量数字人流程

入口：管理员可见的“批量数字人/一站式批量生成”模块。

目标：

- 管理员输入一个大方向，比如“日本房地产相关”。
- AI 自动生成 10 条待生产选题。
- 系统按队列逐条生产。
- 默认市场：中国市场/简体中文。
- 默认音色：MiniMax 温柔女声。
- 默认主播：女主播 C。
- 默认数字人引擎：5090 InfiniteTalk。
- 当前阶段先只生成到系统历史记录，不接自动发布。

关键接口：

```text
POST /api/auto-digital/topics
POST /api/auto-digital/batches
GET  /api/auto-digital/batches
GET  /api/auto-digital/batches/{batch_id}
POST /api/auto-digital/batches/{batch_id}/cancel
```

关键规则：

- 只管理员可见，员工不可见。
- 批量任务要排队，不能让多个 InfiniteTalk 同时抢 GPU。
- 数字人仍用 MiniMax 配音 + 5090 InfiniteTalk。

## 11. OpenNews 新闻视频主流程

当前产品主线：频道自动化，不再以“人工逐条审核素材”为主。

入口：

- 主页面 `OpenNews 新闻视频`
- OpenNews 数据中心 `/opennews/data-center`
- 频道配置 `/api/opennews/channels/config`

频道规划：

- 科技新闻：`technology`
- 军事新闻：`military`
- 政治新闻：`politics`
- 金融新闻：`finance`
- 综合新闻：`general`，默认关闭

频道配置文件：

```bash
/app/output/opennews_batches/channels_config.json
```

每个频道可配置：

- 是否启用。
- 分类和关键词。
- 时间范围：`1h` / `6h` / `24h`。
- 定时间隔，当前常用 120 分钟。
- 每次抓取数量。
- 每次制作数量，当前常用 6 条。
- 语言：`cn` / `jp` / `en`。
- 发布平台：X / Facebook / YouTube。
- 每个语言对应不同平台账号配置。

当前重要发布规则：

- 可以生成中文、日文、英文三个语言版本。
- 当前自动发布只启用中文版本到 X 和 Facebook。
- 日文/英文可以生成成片，但默认不自动发 X/Facebook，除非频道账号配置里明确打开。
- YouTube 自动发布目前暂停，旧代码保留，后续可能恢复。

OpenNews 自动化流程：

1. 后台 scheduler 读取 `channels_config.json`。
2. 如果频道启用且到时间，抓取热点新闻候选。
3. 去重，按频道和关键词过滤。
4. 选择 `produce_limit` 条进入制作。
5. 使用统一文案模型生成中文新闻稿，同时为日文/英文生成对应语言稿。
6. 语言稿需要校验，不能把英文稿误写成中文/日文，不能把日文稿误写成英文。
7. OpenNews 配音固定走 5090 Qwen3-TTS。
8. 素材只走免费素材 API/新闻源公开素材匹配；不要依赖旧本地素材库兜底。
9. 如果免费素材不命中，允许用新闻图卡保底，避免生成白底占位视频。
10. `video_composer.py` 合成各语言竖屏视频，字幕必须和对应语言配音/文案匹配。
11. 成片完成后触发发布：
    - X：浏览器自动化发布中文竖屏。
    - Facebook：Graph API 发布中文竖屏。
    - YouTube：当前跳过。
12. 结果写入 `output/<history_id>/result.json`，批次状态写入 `output/opennews_batches/batch_jobs/*.json`。

OpenNews 相关接口：

```text
GET  /api/opennews/batches/config
POST /api/opennews/batches/config
GET  /api/opennews/channels/config
POST /api/opennews/channels/config
GET  /api/opennews/accounts/summary
POST /api/opennews/batches/run-now
GET  /api/opennews/batches
POST /api/opennews/batches/produce
GET  /api/opennews/batches/jobs
GET  /api/opennews/batches/jobs/{job_id}
POST /api/opennews/batches/jobs/{job_id}/continue
GET  /api/opennews/auto/config
POST /api/opennews/auto/config
POST /api/opennews/auto/run-now
```

OpenNews 数据中心：

- 页面：`/opennews/data-center`
- 展示发布记录、平台数据、账号绑定、X 浏览器 Profile 管理等。
- 指标读取：
  - Facebook 可通过 Graph API 读取部分互动数据。
  - X 浏览器自动发布后主要保存发布 URL/记录；X 互动数据如果不接付费 API，能力有限。
  - YouTube 暂停时不会产生新数据。

## 12. OpenNews 文案、配音和字幕规则

文案：

- 当前统一走接口模型：`https://api.office.ihousejapan.cn`。
- 不应回退到旧 Claude/GLM/本地 5090 文案链路，除非用户明确要求。
- 日文和英文不是简单翻译，而是应基于新闻事实生成对应市场语言的新闻播报稿。
- 如果模型输出语种错误，后端需要 fallback 生成对应语种，而不是让错误稿进入成片。

配音：

- OpenNews 必须走 5090 Qwen3-TTS。
- 不能用 MiniMax 兜底。
- 5090 不通或 GPU 出错时，应该重试/等待服务恢复；用户可能会手动重启 5090。

字幕：

- 中文视频必须显示中文字幕。
- 日文视频必须显示日文字幕。
- 英文视频必须显示英文字幕。
- 字幕切分要按语言特点处理，英文不要变成“一个单词挨着一个单词”的散乱字幕。

## 13. OpenNews 发布账号

X：

- 当前不使用 X 付费 API 发布。
- 使用 `x_browser_publisher.py` 通过 Chromium/Playwright 控制浏览器自动发帖。
- 浏览器 Profile 存在：

```bash
/app/output/x_browser/profile
```

- Profile 导入/导出接口：

```text
GET  /api/x/browser-profile/export
POST /api/x/browser-profile/import
POST /api/x/browser-profile/clear
GET  /api/x/browser-profile/status
```

- 服务器端 X 登录窗口：

```text
GET  /api/x/browser-login/status
POST /api/x/browser-login/start
POST /api/x/browser-login/stop
```

X OAuth 旧接口仍存在，但当前 OpenNews 发布主线不应依赖它：

```text
GET /api/x/oauth/start
GET /api/x/oauth/callback
```

Facebook：

- 使用 Facebook Graph API。
- OAuth token 存储：

```bash
/app/output/facebook_auth/facebook_token.json
```

- 当前绑定 Page 曾为 `OpenNews`。实际以 `/api/facebook/status` 和 `/api/opennews/accounts/summary` 返回为准。

Facebook OAuth：

```text
GET /api/facebook/oauth/start
GET /api/facebook/oauth/callback
```

Facebook 发布：

```text
POST /api/facebook/upload
```

频道分账号：

- OpenNews 频道配置支持按频道、语言、平台绑定不同账号。
- 当前应把现有 X/Facebook 全局账号配置到“科技新闻/中文”发布。
- 日文/英文默认关闭发布，避免误发三语。

## 14. YouTube 状态

YouTube 代码仍保留：

- `youtube_publisher.py`
- `/api/youtube/status`
- `/api/youtube/oauth/start`
- `/api/youtube/oauth/callback`
- `/api/youtube/upload`
- OpenNews 旧合集/Shorts 发布接口

但当前产品规则：

- OpenNews 自动化先不用 YouTube。
- 不要让自动化流程等待 YouTube 合集或 Shorts。
- 有成片后即可按配置发布到 X/Facebook。
- YouTube 代码不要删除，后续可能恢复。

## 15. 房源实拍成片流程

入口：主页面“房源实拍成片”。

流程：

1. 上传房源实拍视频/素材。
2. 后端视觉分析，生成镜头/时间轴。
3. 生成文案和字幕。
4. 选择配音音色。
5. 选择 BGM。
6. 合成最终房源视频。

关键文件：

- `property_video_workflow.py`
- `property_video_vision.py`
- `video_composer.py`
- `material_library.py`

BGM：

- 当前素材库在产品上主要只保留房源实拍 BGM。
- 管理接口：

```text
GET    /api/property-video/bgm
POST   /api/property-video/bgm/upload
DELETE /api/property-video/bgm/{item_id}
```

## 16. 素材库状态

历史上系统有素材库、素材采集、LocalTok 审核等能力，但当前已精简：

- OpenNews 和普通数字人：只走免费素材 API/公开新闻素材匹配，不使用本地素材库图片兜底。
- 房源实拍：保留 BGM 音频管理。
- 素材库页面和采集候选池接口仍可能存在，但应视为停用/遗留兼容接口。

相关接口会返回“已精简/停用”提示：

```text
GET    /api/material-library
POST   /api/material-library/upload
POST   /api/material-library/harvest/jobs
DELETE /api/material-library/{item_id}
```

新开发不要重新引入旧本地图片素材库逻辑，除非用户明确要求。

## 17. 5090 GPU 编排规则

当前资源冲突重点：

- OpenNews Qwen3-TTS 会占用 5090 GPU。
- InfiniteTalk 数字人生成也会占用 5090 GPU。
- 两者不能无脑并发，否则容易导致显存/驱动异常。

当前设计方向：

- Qwen3-TTS 最大并发默认 1。
- InfiniteTalk 数字人任务排队，一条一条跑。
- GPU orchestrator 负责切换/保护本地服务资源。
- Qwen3-TTS watchdog 负责发现 TTS 服务异常并重启。
- Qwen3-TTS 服务有请求预算/重启策略，避免长时间连续请求导致 CUDA launch failure。

开发注意：

- 不要把 OpenNews 多语言 TTS 全部并发打到 5090。
- 不要让批量数字人多个 InfiniteTalk job 同时跑。
- 如果用户在页面点取消，需要调用 `cancel_infinitetalk_jobs()` 尽量停止 5090 实际 job。
- 如果 5090 GPU 掉了，主系统应该显示等待/重试，而不是悄悄使用 MiniMax 给 OpenNews 兜底。

## 18. 后端服务面板

页面：

```text
/admin/services
```

API：

```text
/api/admin/services-status
```

用途：

- 查看主应用状态。
- 查看 5090 Qwen3-TTS health。
- 查看 InfiniteTalk 服务状态。
- 查看 OpenNews 频道、批次、队列状态。
- 查看 X/Facebook 绑定状态。
- 查看当前任务和队列。

如果开发新外部服务，应优先把 health check 接进这个页面。

## 19. 历史记录与输出结构

每条任务的结果目录在：

```bash
/app/output/<history_id>/
```

典型结构：

```text
result.json
script.json
script_readable.txt
audio/
digital_human/
materials/
final_video.mp4
vertical_video.mp4
```

OpenNews 多语言版本通常会在 `result.json` 里记录：

- `language_versions`
- `target_market`
- `x_publish_records`
- `facebook_publish_records`
- `youtube_publish_records`
- `platform_metrics`
- `workflow_config`
- `opennews_channel_id`
- `opennews_channel_name`

历史记录 API：

```text
GET    /api/history
GET    /api/history/{history_id}/result
GET    /api/history/{history_id}/files
POST   /api/history/{history_id}/compose
POST   /api/history/{history_id}/resume
DELETE /api/history/{history_id}
```

## 20. 当前任务和取消/重试

当前任务主要保存在 `app.py` 内存变量 `tasks`。任务进度通过 tracker messages 返回。

接口：

```text
GET  /api/tasks/active
GET  /api/tasks/{task_id}/progress
POST /api/tasks/{task_id}/cancel
POST /api/tasks/{task_id}/retry
GET  /api/tasks/{task_id}/result
```

注意：

- 容器重启会清空内存中的 running task。
- 已经写入 `output/` 的历史可以继续查看/恢复。
- OpenNews compose-ready recovery 会扫描部分已到成片阶段但未合成/未发布的任务。
- 取消数字人任务时要确认 5090 `api_jobs` 中实际 job 是否停止。

## 21. 主播图和音色

主播图：

- 静态文件在 `assets/`。
- 主播图 manifest 在 `assets/avatar_library_manifest.json`。
- 当前需要保留的恢复图包括：
  - `avatar_recovered_gray_suit_male_04.png`
  - `avatar_recovered_white_shirt_female_05.png`
  - 以及当前线上已有的男女主播图。

最近用户上传过高清替换图：

- `/Users/saita/Desktop/1.png`
- `/Users/saita/Desktop/2.png`

这些已用于替换对应主播图时，务必同步本地、生产主机和容器 assets。

音色：

- 普通数字人和批量数字人使用 MiniMax 音色。
- `みん` 音色 ID：`moss_audio_08266d37-33be-11f1-a17e-22a10454a6ba`
- `bin` 音色已要求去掉，不要再在 UI 中展示。
- 音色和主播图需要性别兼容检查；不要因为“已启用”导致按钮变白不可选。

## 22. 文案模型状态

当前目标状态：

- 所有普通文案、AI 修改、批量数字人选题、OpenNews 新闻稿和多语言稿都走统一接口模型。
- 接口：`https://api.office.ihousejapan.cn`
- Key 在 `.env`，不要写入仓库。

旧/可选链路：

- Claude：旧文案模型。
- GLM/智谱：曾经用于管理员模型选择。
- 5090 local Qwen：曾用于本地文案测试。

现在不要因为 `LOCAL_QWEN_API_KEY` 缺失而阻断普通文案主流程；用户已明确当前文案模型不是 GLM/智谱 key，也不是本地 5090 文案。

## 23. OpenNews 素材策略

当前产品要求：

- 素材匹配不要过严，避免“页面有素材但后端判空”。
- 只对接免费素材 API/公开素材，不恢复旧本地素材库图片兜底。
- 如果素材不足，应尽量：
  - 放宽实体/场景匹配。
  - 使用新闻源图片。
  - 生成新闻图卡保底。
  - 避免白底占位视频。

之前常见错误：

```text
OpenNews 素材为空：本地正式素材库没有拿到可用素材，已中止以避免生成白底占位视频。
```

这类错误不应再用“本地素材库为空”作为中止理由。应按免费素材 API/新闻图卡策略继续成片。

## 24. OpenNews 自动发布当前约束

当前配置目标：

- 科技新闻频道每 2 小时自动制作 6 条。
- 生成中文、日文、英文成片。
- X/Facebook 只发布中文版本。
- YouTube 不发布。

实现要点：

- 发布不能在“只有素材/文案、还没合成 mp4”时触发。
- `POST /api/history/{history_id}/compose` 成片完成后也要触发自动发布。
- 如果日文/英文生成失败，不应影响中文成片发布。
- 如果 X 成功、Facebook 失败，要分别记录，不要整体吞掉。
- 如果频道账号配置只勾选中文，不要发布日文/英文。

排查一条 OpenNews 是否可发布：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'python /app/tools/inspect_opennews_publish_state.py'"
```

如果容器内没有该工具，先从本地同步 `tools/inspect_opennews_publish_state.py`。

## 25. X 浏览器自动化发布注意事项

为什么不用 X API：

- X API 发布视频成本高，用户要求改为浏览器模拟手动发布。

当前实现：

- Docker 镜像内装 Chromium、Xvfb、Openbox、x11vnc、noVNC、Playwright 依赖。
- `x_browser_publisher.py` 打开 `https://x.com/compose/post`，填写文字、上传视频、点击 Post。
- `x_browser_login_manager.py` 提供可视化登录窗口。

常见问题：

- X 限制登录：需要冷却后通过 noVNC 手动登录一次。
- 发出文字但没视频：通常是上传控件未成功、视频格式/大小问题或点击过早，需要看 debug 截图和 log。
- 发到错误账号：检查浏览器 Profile 实际登录账号，不要只看首页推荐内容。

Debug 位置：

```bash
/app/output/x_browser/debug/
/app/output/x_browser/screenshots/
```

## 26. Facebook 发布注意事项

Facebook 发布需要：

- App ID / App Secret 在 `.env`。
- OAuth 回调 URL 配置正确。
- 授权账号必须管理目标 Page。
- 需要 Page access token。

Token 存储：

```bash
/app/output/facebook_auth/facebook_token.json
```

常见问题：

- `Error validating client secret`：`.env` 中 `FACEBOOK_APP_SECRET` 与 Meta 后台不一致。
- `当前授权账号没有可管理的 Facebook Page`：当前 Facebook 账号没有 Page 管理权限，需创建/切换 Page。
- 应用未启用：Meta App 处于关闭或测试限制，需开发者后台启用/加测试用户。

## 27. 旧功能/遗留功能清单

以下功能代码仍存在，但不是当前主线。新开发时不要优先依赖：

- YouTube 自动合集/Shorts 发布。
- OpenNews LocalTok 审核。
- OpenNews 手动逐条审核素材老流程。
- 素材库图片采集/素材候选池。
- 管理员主播图实验室，用户已要求从看板移除；后端接口可能仍残留。
- 费用统计，用户已要求不需要；`/api/admin/stats` 当前返回 410。
- HunyuanVideo-Avatar：5090 旧测试用途，不是当前主数字人链路。
- 火山 OmniHuman：历史/应急数字人通道，当前最新规划是数字人统一 5090 InfiniteTalk。

## 28. 开发时的高风险点

1. 线上主机文件和容器文件不一致。
   - 修复后必须 `docker cp` 到 `OmniHuman:/app/` 并重启。

2. OpenNews 语言串线。
   - 日文稿、英文稿必须校验，字幕和配音必须对应语言。

3. OpenNews 发布误发三语。
   - 当前只发中文到 X/Facebook，除非频道语言账号配置明确打开。

4. 5090 GPU 被抢占。
   - Qwen3-TTS 和 InfiniteTalk 要排队/限并发。

5. 数字人任务取消后 5090 仍在跑。
   - 需要调用 InfiniteTalk cancel 接口并查 `/home/saita/InfiniteTalk/api_jobs`。

6. 素材匹配过严。
   - 不要因为免费素材匹配分数过低直接中止整条新闻。

7. 敏感信息泄漏。
   - 不要把 `.env`、token JSON、API Key、服务器密码提交 Git。

## 29. 新开发者建议先读顺序

1. `docs/ihouse_omnihuman_system_handoff_2026-07-07.md`
2. `README.md`
3. `.env.example`
4. `docker-compose.yml`
5. `app.py` 顶部常量区、OpenNews channel config 区、发布区、任务区。
6. `templates/index.html` 中 OpenNews、历史记录、管理员入口、批量数字人 UI。
7. `opennews_admin.py`
8. `fetch_materials.py`
9. `generate_script.py`
10. `infinitetalk_avatar_client.py`
11. `x_browser_publisher.py`
12. `facebook_publisher.py`

## 30. 最小健康检查清单

生产主站：

```bash
curl -sS https://aiagent.office.ihousejapan.cn/ | head
```

容器日志：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker logs --tail 200 OmniHuman"
```

后端服务面板：

```text
https://aiagent.office.ihousejapan.cn/admin/services
```

5090 GPU：

```bash
ssh -p 43612 saita@office.ihousejapan.cn "nvidia-smi"
```

Qwen3-TTS：

```bash
ssh -p 43612 saita@office.ihousejapan.cn \
  "systemctl --user status ihouse-qwen3-tts.service --no-pager"
```

OpenNews 频道配置：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'python - <<\"PY\"\nimport json\np=\"/app/output/opennews_batches/channels_config.json\"\nprint(json.dumps(json.load(open(p)), ensure_ascii=False, indent=2)[:3000])\nPY'"
```

X/Facebook 绑定：

```text
https://aiagent.office.ihousejapan.cn/opennews/data-center
```

或：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net \
  "docker exec OmniHuman /bin/sh -lc 'ls -l /app/output/x_browser/profile /app/output/facebook_auth/facebook_token.json 2>/dev/null || true'"
```

## 31. 当前业务决策摘要

- 普通数字人：统一 MiniMax 配音 + 5090 InfiniteTalk。
- 批量数字人：仅管理员可见，统一 MiniMax 配音 + 5090 InfiniteTalk，先只生成到系统，不自动发布。
- OpenNews：频道化自动生产，科技/军事/政治/金融可分别配置。
- OpenNews 文案：统一接口模型。
- OpenNews 配音：只用 5090 Qwen3-TTS。
- OpenNews 素材：免费素材 API/新闻源素材/新闻图卡保底，不用旧本地图片素材库。
- OpenNews 发布：当前只发布中文竖屏到 X 和 Facebook。
- X：浏览器自动化发布，不用付费 API。
- Facebook：Graph API Page 发布。
- YouTube：暂停自动发布，代码保留。
- 素材库：页面精简，主要保留房源实拍 BGM。
- 后端服务状态：看 `/admin/services`。

