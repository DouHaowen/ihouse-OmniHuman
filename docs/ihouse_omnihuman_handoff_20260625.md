# iHouse OmniHuman 项目交接文档

更新时间：2026-06-25  
项目名：`ihouse-OmniHuman`  
用途：iHouse AI 视频生产系统，包含数字人短视频、房源实拍成片、OpenNews 新闻视频自动化、素材库、外部接口、小程序/移动端嵌入页面等功能。

---

## 1. 项目仓库与目录

### GitHub 仓库

```text
https://github.com/DouHaowen/ihouse-OmniHuman
```

当前主分支通常是：

```text
main
```

### 本地工作目录

```text
/Users/saita/saita/ihouse-OmniHuman
```

常用本地关键文件：

```text
/Users/saita/saita/ihouse-OmniHuman/app.py
/Users/saita/saita/ihouse-OmniHuman/generate_script.py
/Users/saita/saita/ihouse-OmniHuman/opennews_admin.py
/Users/saita/saita/ihouse-OmniHuman/opennews_batch.py
/Users/saita/saita/ihouse-OmniHuman/fetch_materials.py
/Users/saita/saita/ihouse-OmniHuman/ai_material_harvester.py
/Users/saita/saita/ihouse-OmniHuman/video_composer.py
/Users/saita/saita/ihouse-OmniHuman/youtube_publisher.py
/Users/saita/saita/ihouse-OmniHuman/templates/index.html
/Users/saita/saita/ihouse-OmniHuman/templates/lab_opennews.html
```

### 当前交接文档位置

```text
/Users/saita/saita/ihouse-OmniHuman/docs/ihouse_omnihuman_handoff_20260625.md
```

---

## 2. 线上主站与部署方式

### 主生产服务器 SSH

```bash
ssh -4 service@ihouseoffice.ddns.net -p 8800
```

### 主生产服务器项目目录

```text
/home/saita/ihouse-OmniHuman
```

### Docker 容器

容器名：

```text
OmniHuman
```

容器内实际运行代码路径：

```text
/app
```

容器内前端模板路径：

```text
/app/templates
```

### 重要部署原则

线上改代码时，不能只改主机目录。

必须执行：

1. 本地文件传到主机 `/home/saita/ihouse-OmniHuman/`
2. 再 `docker cp` 到 `OmniHuman:/app/`
3. 最后 `docker restart OmniHuman`

例如同步 `app.py`：

```bash
scp -4 -P 8800 app.py service@ihouseoffice.ddns.net:/home/saita/ihouse-OmniHuman/app.py
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker cp /home/saita/ihouse-OmniHuman/app.py OmniHuman:/app/app.py && docker exec OmniHuman /bin/sh -lc 'python -m py_compile /app/app.py' && docker restart OmniHuman"
```

同步多个核心文件：

```bash
scp -4 -P 8800 app.py opennews_admin.py opennews_batch.py fetch_materials.py video_composer.py youtube_publisher.py service@ihouseoffice.ddns.net:/home/saita/ihouse-OmniHuman/
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker cp /home/saita/ihouse-OmniHuman/app.py OmniHuman:/app/app.py && docker cp /home/saita/ihouse-OmniHuman/opennews_admin.py OmniHuman:/app/opennews_admin.py && docker cp /home/saita/ihouse-OmniHuman/opennews_batch.py OmniHuman:/app/opennews_batch.py && docker cp /home/saita/ihouse-OmniHuman/fetch_materials.py OmniHuman:/app/fetch_materials.py && docker cp /home/saita/ihouse-OmniHuman/video_composer.py OmniHuman:/app/video_composer.py && docker cp /home/saita/ihouse-OmniHuman/youtube_publisher.py OmniHuman:/app/youtube_publisher.py && docker exec OmniHuman /bin/sh -lc 'python -m py_compile /app/app.py /app/opennews_admin.py /app/opennews_batch.py /app/fetch_materials.py /app/video_composer.py /app/youtube_publisher.py' && docker restart OmniHuman"
```

同步前端模板：

```bash
scp -4 -P 8800 templates/index.html service@ihouseoffice.ddns.net:/home/saita/ihouse-OmniHuman/templates/index.html
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker cp /home/saita/ihouse-OmniHuman/templates/index.html OmniHuman:/app/templates/index.html && docker restart OmniHuman"
```

如果前端页面没生效，优先检查容器里是否真的更新：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker exec OmniHuman /bin/sh -lc 'ls -lh /app/templates/index.html && grep -n \"关键字\" /app/templates/index.html | head'"
```

---

## 3. 常用线上检查命令

查看容器状态：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker ps --format 'table {{.Names}}\t{{.Ports}}\t{{.Status}}'"
```

查看日志：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker logs --tail 200 OmniHuman"
```

检查 Python 编译：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker exec OmniHuman /bin/sh -lc 'python -m py_compile /app/app.py /app/generate_script.py /app/opennews_admin.py /app/opennews_batch.py'"
```

检查环境变量，不要输出密钥明文：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker exec OmniHuman /bin/sh -lc 'grep -E \"^(OPENNEWS|QWEN|VOICE|MINIMAX|YOUTUBE|ZHIPUAI)\" /app/.env | sed -E \"s/(KEY|TOKEN|SECRET|API_KEY)=.*/\\1=hidden/\"'"
```

检查 `ZHIPUAI_API_KEY`：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker exec OmniHuman /bin/sh -lc 'grep ^ZHIPUAI_API_KEY= /app/.env | sed \"s/=.*$/=hidden/\"'"
```

---

## 4. 5090 服务器

### 连接方式

```bash
ssh -p 43612 saita@office.ihousejapan.cn
```

密码：

```text
admin@123
```

### 当前用途

5090 服务器主要承担本地模型测试和服务：

- Qwen3-TTS 本地配音服务
- OpenNews 本地文案模型 Ollama/Qwen
- 素材库视觉标签与向量检索服务
- GPU 编排服务
- InfiniteTalk 数字人服务
- HunyuanVideo-Avatar 旧测试服务，OpenNews 合集片头目前不建议走 Hunyuan，只保留 InfiniteTalk 方向

### 重要路径

```text
/home/saita/qwen3-tts-service
/home/saita/qwen3-tts-service/server.py
/home/saita/qwen3-tts-service/outputs
/home/saita/ollama-bin
/home/saita/ollama-data
/home/saita/logs/ollama-opennews.log
/home/saita/hunyuanvideo-avatar-poc
```

### 5090 当前关键端口

```text
8895  Qwen3-TTS 本地配音服务
8897  素材向量/视觉服务相关服务
8898  GPU Orchestrator 服务
8893  InfiniteTalk 数字人服务
11434 Ollama 本地文案模型服务
```

### 检查 5090 服务

```bash
sshpass -p 'admin@123' ssh -p 43612 -o StrictHostKeyChecking=no saita@office.ihousejapan.cn "ps -ef | grep -E 'ollama serve|qwen3-tts|uvicorn server:app --host 0.0.0.0 --port 8895|gpu_orchestrator_5090|infinitetalk' | grep -v grep"
```

检查 Ollama：

```bash
sshpass -p 'admin@123' ssh -p 43612 -o StrictHostKeyChecking=no saita@office.ihousejapan.cn "curl -sS http://127.0.0.1:11434/api/version && /home/saita/ollama-bin/bin/ollama list"
```

检查 Qwen3-TTS：

```bash
sshpass -p 'admin@123' ssh -p 43612 -o StrictHostKeyChecking=no saita@office.ihousejapan.cn "curl -sS -H 'x-token: local-qwen3-tts-5090' http://127.0.0.1:8895/health"
```

查看音色：

```bash
sshpass -p 'admin@123' ssh -p 43612 -o StrictHostKeyChecking=no saita@office.ihousejapan.cn "curl -sS -H 'x-token: local-qwen3-tts-5090' http://127.0.0.1:8895/voices"
```

当前 Qwen3-TTS 可用音色：

```text
aiden
dylan
eric
ono_anna
ryan
serena
sohee
uncle_fu
vivian
```

OpenNews 当前女声：

```text
serena
```

OpenNews 当前男声：

```text
aiden
```

检查 GPU：

```bash
sshpass -p 'admin@123' ssh -p 43612 -o StrictHostKeyChecking=no saita@office.ihousejapan.cn "nvidia-smi"
```

如果出现：

```text
Unable to determine the device handle for GPU0: Unknown Error
No devices were found
```

说明 5090 显卡掉了，服务进程可能还在，但 CUDA/GPU 实际不可用。通常需要重启 5090 主机，重启后再检查 `nvidia-smi` 是否恢复。

### 5090 重启后的自启动情况

目前 Ollama 已设置 `@reboot` 自启动：

```text
OLLAMA_HOST=0.0.0.0:11434
OLLAMA_MODELS=/home/saita/ollama-data
/home/saita/ollama-bin/bin/ollama serve
```

Qwen3-TTS、GPU Orchestrator 等也已配置为重启后自动启动，但如果显卡掉线，进程在不代表 GPU 可用，必须看 `nvidia-smi`。

---

## 5. 系统账号

账号定义主要在：

```text
app.py
USERS = {...}
```

已有账号包括：

```text
admin
zhong
bin
ricky
tai
ri
da
liyh
zhoubing
ikemoto
zck
saita
han
sunqinxue
aki
baicy
lidj
zhaozy
```

典型新增账号规则：

- 员工账号只能使用普通权限
- 管理员账号可看到管理员功能、模型选择、素材审核、自动化控制等
- 员工账号不开放 Hunyuan、本地引擎、部分高级模型选择

### JClaw / 主系统 SSO 映射

之前已对接 JClaw 主系统账号映射。

主系统进入 AI 子系统时，会携带 handoff / lab token。

重要映射示例：

```text
zhong  <- xin
bin    <- bin
ricky  <- ricky
tai    <- min
liyh   <- liyh
zhoubing <- zhoubing
ikemoto <- ikemoto
zck    <- zck-zr
saita  <- saita
han    <- han
sunqinxue <- sunqinxue
aki    <- aki
baicy  <- baicy
lidj   <- lidj
zhaozy <- zhaozy
```

JClaw handoff secret 曾经更新过，注意生产 `.env` 中实际值，不要在文档里写明文密钥。

---

## 6. 三大核心功能总览

系统目前重点有三大功能：

1. 数字人短视频
2. 房源实拍成片
3. OpenNews 新闻视频

移动端 / 小程序页面中也应该把这三项作为平级核心入口，而不是只突出新闻。

---

## 7. 数字人短视频流程

### 入口

网页主页面：

```text
https://aiagent.office.ihousejapan.cn/
```

移动端 / Lab 页面：

```text
https://aiagent.office.ihousejapan.cn/lab/apps/opennews
```

其中移动端页面是 JClaw Lab 小程序协议嵌入的网页，不是 iOS 原生页面。

### 基本流程

1. 用户输入视频选题
2. 可选择是否启用实时联网检索
3. 生成文案
4. 用户确认或编辑文案
5. 系统按固定结构拆分：
   - 第 1 段必须是数字人
   - 中间必须有 1 段数字人
   - 最后一段必须是数字人
   - 固定 3 段数字人，不是最多 3 段
   - 中间数字人段是短过渡段，必须尽量短
   - 其他内容优先走素材段
6. 生成配音
7. 生成数字人段
8. 生成素材段
9. 合成最终视频

### 关键代码

```text
generate_script.py
app.py
fetch_materials.py
video_composer.py
```

如果数字人段数不对，优先看：

```text
generate_script.py
```

不要只改 prompt，必须检查固定三段数字人的后处理逻辑。

### 权限规则

普通员工：

- 文案模型默认不开放多模型选择
- 数字人引擎强制火山 OmniHuman
- 不开放 Hunyuan / 本地数字人

管理员：

- 可选文案模型
- 可选数字人引擎
- 可看到更多调试与管理员功能

### 主播图片与性别限制

主播图片在：

```text
assets/
```

相关配置在：

```text
app.py
AVATAR_OPTION_LABELS
AVATAR_OPTION_META
VOICE_PRESETS
```

性别限制：

- 男声音色不能选择女主播
- 女声音色不能选择男主播
- 台湾市场曾经调整为不强制锁音色，只做默认推荐

已新增过的主播：

- 女主播 C
- 男主播 B
- 奥特曼，男生
- OpenNews 女主播：`opennews_anchor_daily.png`
- OpenNews 男主播：`opennews_anchor_daily_male.png`

---

## 8. 房源实拍成片流程

### 业务目标

销售人员实地拍摄房屋视频，可以是：

- 一个长视频
- 多个视频片段

用户上传视频，并填写想说的介绍内容。系统负责：

1. 合并视频
2. 识别或根据销售补充信息生成解说文案
3. 用户可以编辑文案
4. 配音
5. 字幕
6. 最终拼接成一个长视频

### 重要设计规则

房源视频不是数字人视频，不需要固定三段数字人结构。

房源实拍核心是：

```text
实拍视频 + 解说配音 + 字幕 + BGM + 成片
```

### 视频与音频时长匹配

用户明确要求：

- 不要把视频剪短
- 不要用末帧补视频
- 不要用静音补满视频

正确方向：

- 先合并整体视频
- 计算总时长
- 根据总时长生成或扩写文案
- 再配音
- 让解说尽量匹配视频总时长

如果配音太短，需要扩写文案。

如果配音太长，需要压缩文案。

### 一镜到底问题

曾经发现问题：

```text
画面在厕所，文案在讲厨房
```

解决方向：

- 一镜到底时间轴模式
- 每个时间段只讲当前画面里的空间
- 销售可以逐段修改
- 生成时逐段配音

但也发现：

- 视觉识别不够精准
- 切分太碎会导致音频段被截断
- 文案不连贯

后续优化应该在：

1. 视频场景识别准确性
2. 时间段切分
3. 每段文案长度控制
4. 段间自然衔接

### 字幕样式

房源视频字幕曾经优化过：

- 一句一句按时间戳显示
- 放到一行
- 字幕和配音时间戳匹配
- 字体比默认更大
- 样式比黑底白字更高级

OpenNews 的字幕样式后来也参考了房源视频样式。

### BGM

房源成片支持 BGM。

注意配音音量：

- 有用户反馈音频偏低
- 后续视频合成中有调高音量逻辑

---

## 9. OpenNews 新闻视频流程

OpenNews 是目前最复杂的自动化模块。

目标：

```text
自动抓取英文圈/全球热点新闻
转成中文新闻稿
匹配素材
配音
生成 Shorts 和横屏合集
可自动发布 YouTube
```

### 页面入口

主系统页面：

```text
https://aiagent.office.ihousejapan.cn/
```

OpenNews 页面在主页面内。

移动端 Lab 页面：

```text
https://aiagent.office.ihousejapan.cn/lab/apps/opennews
```

### 新闻抓取逻辑

抓取源包括：

- NewsData.io
- GDELT，曾经接入但经常不可用或限流
- Bing News RSS 兜底
- 部分指定新闻网站/RSS

目前用户希望重点抓取：

```text
AI
机器人
科技
金融
房产
移民
军事
政治
能源
```

同时要求：

- 新闻必须新
- 不要重复新闻
- 同一事件即便标题不同，也应该去重

### 新闻去重

历史问题：

- 同一个新闻不同标题重复出现
- 比如前美联储主席格林斯潘去世，短时间内被重复制作
- Grok 4.3 类新闻也出现过标题不同但事件相同的问题

已做方向：

- 标题规范化
- event identity
- seen.json
- 近历史成片去重
- 批次内部去重

需要继续关注：

```text
/app/output/opennews_batches/seen.json
/app/output/opennews_batches/batch_jobs/*.json
/app/output/opennews_batches/batches/*.json
```

如果重复严重，优先排查：

```text
opennews_batch.py
app.py 中 _opennews_is_duplicate_auto_event / _opennews_recent_completed_event_identities 等逻辑
```

### OpenNews 文案模型

当前已接入：

1. 优先 5090 本地 Qwen 文案模型
2. 失败时回退 GLM-5.2

生产容器 `.env`：

```text
OPENNEWS_TEXT_MODEL_PROVIDER=local_qwen
OPENNEWS_LOCAL_LLM_BASE_URL=http://192.168.0.34:11434/v1
OPENNEWS_LOCAL_LLM_MODEL=hf.co/bartowski/Qwen_Qwen3-30B-A3B-Instruct-2507-GGUF:Q4_K_M
OPENNEWS_LOCAL_LLM_TIMEOUT_SECONDS=240
OPENNEWS_LOCAL_LLM_RETRY_ATTEMPTS=1
OPENNEWS_GLM_MODEL=glm-5.2
```

本地 Qwen 模型：

```text
hf.co/bartowski/Qwen_Qwen3-30B-A3B-Instruct-2507-GGUF:Q4_K_M
```

运行方式：

```text
Ollama OpenAI-compatible API
http://192.168.0.34:11434/v1/chat/completions
```

重要兼容点：

Ollama/Qwen 有时会把 JSON 放在：

```text
message.reasoning
```

而不是：

```text
message.content
```

代码里 `_extract_chat_completion_text` 已兼容 `reasoning`。

### 文案风格

用户最终确认的风格：

```text
把爬取到的新闻标题、摘要和正文内容进行事实提炼，生成新闻稿类型的口播稿。
```

不要：

- 评论感
- 过度短视频口水化
- “这条新闻值得关注”
- “从已经公开的信息看”
- “后续仍需关注”
- 原文没有的国家、人物、时间、数字、计划、演习

要：

- 新闻稿模式
- 主播可直接播报
- 第一导语交代核心事实
- 主体提炼事实、背景、各方表态、影响对象
- 原文没有后续安排就不要编

核心提示词在：

```text
opennews_admin.py
_polish_opennews_broadcast_copy
```

### OpenNews 配音

当前原则：

1. 优先 5090 本地 Qwen3-TTS
2. 失败再回退 MiniMax

OpenNews 女声：

```text
serena
```

OpenNews 男声：

```text
aiden
```

相关代码：

```text
app.py
OPENNEWS_QWEN_TTS_FEMALE_SPEAKER
OPENNEWS_QWEN_TTS_MALE_SPEAKER
_generate_opennews_qwen_tts_audio
_opennews_minimax_fallback_voice
```

MiniMax 兜底：

```text
OPENNEWS_MINIMAX_FALLBACK_FEMALE_VOICE_PRESET_ID=mandarin_female
OPENNEWS_MINIMAX_FALLBACK_MALE_VOICE_PRESET_ID=mandarin_male
```

### OpenNews 男/女主播交替

合集批次主播交替状态文件：

```text
/app/output/opennews_batches/presenter_state.json
```

逻辑：

- 上一次是 female，下一次就是 male
- 上一次是 male，下一次就是 female

强制下一轮用男主播，可写：

```json
{
  "last_gender": "female",
  "batch_count": 0,
  "updated_at": 1782360000
}
```

强制下一轮用女主播，可写：

```json
{
  "last_gender": "male",
  "batch_count": 0,
  "updated_at": 1782360000
}
```

### OpenNews 自动化批次

手动启动完整批次接口：

```text
POST /api/opennews/batches/run-manual-production
```

页面按钮：

```text
启动一轮完整发布 / 启动一轮 10 条成片
```

流程：

1. 抓取 20 条新闻
2. 根据规则筛选 10 条
3. 同一事件去重
4. 制作单条新闻视频
5. 最高热度新闻单独发布竖屏 Shorts
6. 10 条左右生成横屏合集
7. 生成合集封面
8. 发布 YouTube

用户后来要求：

- 自动化暂时可关闭
- 需要时手动按钮启动一轮
- YouTube 账号被封/限流时不要自动乱发

### 当前最近一次批次状态

最近启动过：

```text
opennews_batch_1782365457_aec73b31
```

状态：

```text
partial
```

原因：

```text
横屏合集已生成，但 YouTube 发布失败：gpt-image-2 封面尚未生成成功，已停止发布 YouTube。
```

其中：

- 2 条单独新闻完成
- 多条重复被跳过
- 1 条因 GLM-5.2 内容安全过滤失败

另外 5090 曾掉显卡，用户重启服务器后服务进程恢复，但 `nvidia-smi` 仍显示 Unknown Error，这一点要优先检查。

### 近期修复

曾发现：

```text
OpenNews 单条任务 result.json 已写出，但内存 tasks 丢失，导致批次一直等中间产物完成。
```

已在 `app.py` 增加恢复逻辑：

```text
_recover_opennews_task_from_output
_wait_for_opennews_task_done(..., expected_title=...)
```

目的：

如果内存任务丢失，但 `/app/output/.../result.json` 已存在且素材可用，就恢复任务并继续合成，避免批次卡住。

---

## 10. OpenNews 素材匹配与素材库

### 素材问题背景

用户非常重视素材准确性和安全性。

历史问题：

- 网络爬取素材经常不准
- 有裸露/不安全图片混入，导致 YouTube 账号被警告/移除/封禁
- AI 生图质量不稳定
- 素材库覆盖不够，Grok、新模型、新人物等新闻很难精准匹配

用户现在倾向：

1. 优先使用实时网络爬取对应新闻图片
2. 网络爬取图片必须经过 5090 Qwen3-VL 视觉审核
3. 审核重点：
   - 是否和新闻文案主题匹配
   - 是否裸露/不安全
   - 是否无关
4. 通过的图片才能用于视频
5. 通过的图片自动入库
6. 如果网络没有合格图片，再用本地素材库向量匹配兜底

### 当前实际情况

系统里已经有正式素材库，路径类似：

```text
/app/material_library
/app/static/material-library
/app/output/opennews_batches
```

前端素材库页面可以上传、审核、删除素材。

用户明确要求：

- 当前数字人和新闻不要默认强依赖素材库，素材库先作为兜底和建设中能力
- 后来 OpenNews 出现网络裸露图片风险，曾要求短期内只用素材库
- 之后又认为最新新闻很难完全用素材库覆盖，所以提出“网络爬取 + Qwen3-VL 审核 + 入库 + 素材库兜底”的路线

这块仍是重点开发区域。

### 素材库视觉向量库

5090 上部署过 Qwen3-VL + 本地向量模型/向量库，为素材打标签与匹配。

目标：

```text
素材图片 -> Qwen3-VL 视觉分析 -> 自动标签 -> 向量入库 -> 新闻文案/素材主题向量检索 -> 精准匹配图片
```

已对素材库大批图片做过视觉标签处理。

用户曾问为什么 5090 标注了 496 张，而前端素材库显示 438 张：

- 可能因为有些是历史/待审核/删除/重复/向量库残留
- 前端正式素材数和向量库记录数不一定完全一致

### 素材安全

必须高度注意：

- 裸露图片不能进入成片
- YouTube 曾因裸露/社区准则问题移除视频或封账号
- 网络图片兜底必须严格过滤
- 封面和视频素材不能重复、不能低质、不能白板

已加过：

- 白底/白板图检测
- 素材为空则中止，不生成白底视频
- 同一素材引用限制
- 网络图片同源/同页频控
- 同一照片一天最多一次、一个月最多五次的限制方向

---

## 11. YouTube 自动发布

### 当前 YouTube 功能

OpenNews 可以自动发布：

- 竖屏 Shorts
- 横屏合集

发布相关文件：

```text
youtube_publisher.py
app.py
```

### YouTube OAuth

回调地址：

```text
https://aiagent.office.ihousejapan.cn/api/youtube/oauth/callback
```

曾经配置过两个账号。

老账号：

```text
ihouse AI Lab
```

新账号：

```text
OpenNews 每日热点
```

新频道：

```text
OpenNews 每日热点
```

channel_id 曾显示：

```text
UCOmArqMBkpEbXUrzqQ78quA
```

注意：

- 不要在文档里写 refresh_token、client_secret 明文
- 生产 `.env` 和 `/app/output/youtube_auth` 里有实际授权信息

### 常见 YouTube 错误

`403 Access forbidden`：

- OAuth scope 不对
- refresh token 对应账号不对
- 测试用户没加
- YouTube 账号/频道状态异常

`uploadLimitExceeded`：

- 新账号上传频率太高
- 账号没有养号
- 每天发布过多视频

YouTube 账号曾被封/移除原因：

- 新账号没养号
- 每天固定发 20 多条
- 题材涉及 AI、军事、政治
- 素材中曾出现裸露/不安全图片

建议：

- 降低发布频率
- 先人工审核一段时间
- 每天少量发布
- 避免军事政治过多
- 素材安全放第一位

### 封面

用户要求：

- 合集封面要专业、个性化
- 不要“今日几条新闻”这种土模板
- 中文标题大
- 不要 iHouse 字样
- 频道名是 `OpenNews 每日热点`
- 封面每次根据合集内容重新生成
- 优先使用中转站 `gpt-image-2-sale`

当前图片模型配置：

```text
OPENNEWS_COLLECTION_THUMBNAIL_KUAIGOU_BASE_URL=https://api.kuaigouai.com/v1
OPENNEWS_COLLECTION_THUMBNAIL_IMAGE_PROVIDERS=kuaigou
OPENNEWS_COLLECTION_THUMBNAIL_IMAGE_MODELS=gpt-image-2-sale
OPENNEWS_COLLECTION_THUMBNAIL_IMAGE_SIZE=1792x1024
OPENNEWS_COLLECTION_THUMBNAIL_IMAGE_QUALITY=medium
OPENNEWS_COLLECTION_THUMBNAIL_RESPONSE_FORMAT=b64_json
OPENNEWS_COLLECTION_THUMBNAIL_OUTPUT_FORMAT=png
```

封面必须生成成功后再发布合集。

如果封面生成失败：

```text
gpt-image-2 封面尚未生成成功，已停止发布 YouTube
```

这种情况下合集视频可能已经生成，但不会发布 YouTube。

---

## 12. 外部接口

### OpenNews 外部审核接口

对接同事系统 / LocalTok / 展示系统使用。

Base URL：

```text
https://aiagent.office.ihousejapan.cn
```

请求头：

```text
X-Token: NEWSdesk8821Aki6000HsVp
```

注意：这是文档里已有共享令牌，继续使用前最好和用户确认是否允许暴露给第三方。

接口：

#### 查询已做过标题

```http
GET /api/external/opennews/used-titles
```

#### 获取自动抓取候选批次

```http
GET /api/external/opennews/candidate-batches?limit=10
```

返回 batch/items：

```text
id
batch_item_id
title
summary
source_name
source_url
published_at
trend_score
```

#### 外部提交选中新闻并触发生产

```http
POST /api/external/opennews/produce-selected
```

示例：

```json
{
  "item_ids": ["新闻id1", "新闻id2"],
  "target_market": "cn",
  "aspect_ratio": "vertical",
  "feedback": "突出影响和背景，做成短新闻口播"
}
```

目标：

- 对方选好新闻
- 我们系统一站式生成视频
- 不要再到页面二次点击成片
- 最终 ready-videos 返回视频

#### 查询 job

```http
GET /api/external/opennews/jobs/{job_id}
```

#### 查询成片

```http
GET /api/external/opennews/ready-videos?limit=50
```

返回包括：

```text
vertical_url
horizontal_url
title
source_name
source_url
published_at
completed_at
```

#### 下载视频

下载 `vertical_url` 或 `horizontal_url` 时也要带：

```text
X-Token: NEWSdesk8821Aki6000HsVp
```

### 数字人 + 房源外部成片接口

同事曾要求：

```text
/api/external/ready-videos
```

返回数字人和房源做好的最终成片，不要中间片段。

每条给：

```text
标题
类型
完成时间
竖屏下载地址
```

注意：数字人视频中间会生成很多过渡片段和中间段，同事只要最终成片。

最终成片常见字段/文件：

```text
final_video.mp4
final_video_vertical.mp4
final_video_horizontal.mp4
final_edit/
```

如果接口 404，说明代码没有同步到 Docker 容器或没有重启。

---

## 13. JClaw Lab 小程序 / 移动端页面

### 背景

同事有 JClaw Lab 小程序协议，希望在他们的原生 App 内打开我们的 AI 子系统页面，并传递登录态。

本质：

```text
原生 App 内嵌 WebView / 小程序 Web 页面
```

我们这边做的是适配移动端 UI 的网页，不是 iOS 原生代码。

### 页面

```text
https://aiagent.office.ihousejapan.cn/lab/apps/opennews
```

页面设计方向：

- 移动端专业 UI
- 三大功能平级：数字人、房源实拍、新闻视频
- 不要把新闻做得过于复杂
- 不要总显示太多任务历史
- 页面右上角显示 AI 子系统账号
- 去掉“浏览器预览”等无关信息

### 账号协同

通过 JClaw token / handoff 实现。

同事曾提供 introspect 方案：

```text
GET https://jclaw-next.ihousejapan.cn/api/lab/introspect
Authorization: Bearer <token>
```

推荐：

- 使用 introspect 免共享密钥验签
- 避免双方 HMAC secret 不一致导致 401

如果本地验签：

- HMAC key 直接用 secret 字符串
- 不要 base64 解码 secret

---

## 14. 当前模型使用情况

### 普通数字人视频文案

目前普通数字人文案模型曾多次切换：

- Claude
- GLM-5.1
- ChatGPT
- API 中转模型
- 本地 Qwen 只用于 OpenNews，不用于普通数字人

当前实际以生产 `.env` 和代码为准，排查：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker exec OmniHuman /bin/sh -lc 'grep -E \"SCRIPT|MODEL|CLAUDE|GLM|OPENAI|RELAY\" /app/.env | sed -E \"s/(KEY|TOKEN|SECRET|API_KEY)=.*/\\1=hidden/\"'"
```

### 房源实拍文案

房源实拍也依赖主文案模型/视觉模型。

曾经接 OpenAI 视觉模型分析视频关键帧，用户给过 OpenAI key，但密钥不要在文档或回复中输出。

### OpenNews 文案

当前：

```text
优先 5090 本地 Qwen3-30B-A3B-Instruct-2507 GGUF
失败回退 GLM-5.2
```

### OpenNews 配音

当前：

```text
优先 5090 Qwen3-TTS
女声 serena
男声 aiden
失败回退 MiniMax
```

### OpenNews 数字人片头

用户多次调整：

- 曾暂停数字人片头，因为慢且贵
- 后来又要求合集开头加入数字人
- 目前要求男/女主播交替
- 本地模型优先方向是 InfiniteTalk
- 不要走 Hunyuan
- 5090 不可用再考虑火山 OmniHuman，但火山贵

当前需要看生产 `.env`：

```text
OPENNEWS_COLLECTION_INTRO_ENABLED
OPENNEWS_COLLECTION_INTRO_LOCAL_DIGITAL_ENABLED
OPENNEWS_COLLECTION_INTRO_LOCAL_ENGINES
```

### 图片/封面模型

OpenNews 合集封面：

```text
优先中转站 gpt-image-2-sale
```

---

## 15. 最近一次重要状态

### 5090 状态

用户刚重启过 5090，因为显卡掉了。

检查结果：

- Ollama 进程已启动
- Qwen3-TTS 进程已启动
- Qwen3-TTS health 显示 loaded true/cuda true
- 但是 `nvidia-smi` 显示：

```text
Unable to determine the device handle for GPU0: Unknown Error
No devices were found
```

这代表 GPU 仍异常。

下一位 AI 接手时，优先检查：

```bash
sshpass -p 'admin@123' ssh -p 43612 -o StrictHostKeyChecking=no saita@office.ihousejapan.cn "nvidia-smi"
```

如果仍然 Unknown Error：

- 不要启动大批量 GPU 任务
- 先让用户确认是否再次重启
- 或检查驱动/PCIe/电源/BIOS Gen4 相关稳定性

### 最近 OpenNews 批次

最近 job：

```text
opennews_batch_1782365457_aec73b31
```

状态：

```text
partial
```

消息：

```text
横屏合集已生成，但 YouTube 发布失败：gpt-image-2 封面尚未生成成功，已停止发布 YouTube。
```

完成项：

```text
摩根士丹利将中国类人机器人出货量预测翻倍，商业化加速
贝森特预计今年GDP增长强劲，Kalshi交易员对此持怀疑态度
```

失败项：

```text
PACOM：取消前缀的深层含义
```

失败原因：

```text
GLM-5.2 内容安全过滤
```

---

## 16. 常见问题排查

### 文案生成失败

看日志：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker logs --tail 300 OmniHuman | grep -E '文案|OpenNews|GLM|local_qwen|fallback|Traceback|失败'"
```

OpenNews 本地 Qwen 失败常见：

- 5090 Ollama 不通
- GPU 掉了
- Ollama 返回空 content/reasoning
- JSON 解析失败

系统会回退 GLM-5.2，但 GLM 可能内容安全过滤。

### OpenNews 批次卡住

查 job：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net 'docker exec OmniHuman python -c "import json; from pathlib import Path; p=Path(\"/app/output/opennews_batches/batch_jobs/JOB_ID.json\"); d=json.loads(p.read_text()); print(d.get(\"status\"), d.get(\"message\")); print([(i.get(\"title\"), i.get(\"status\"), i.get(\"history_id\"), i.get(\"error\")) for i in d.get(\"items\",[])])"'
```

查 output：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker exec OmniHuman /bin/sh -lc 'find /app/output -maxdepth 2 -type d -mmin -60 | sort | tail -30'"
```

如果 result.json 已生成但 job 卡住，检查修复函数：

```text
_recover_opennews_task_from_output
_wait_for_opennews_task_done
```

### 成片出现白板

必须阻止发布。

排查：

```text
result.json
segments[].material_paths
segments[].material_items
material_review
```

如果素材为空或图片白底，应中止，不生成白底视频。

### 素材不匹配

优先看：

```text
fetch_materials.py
ai_material_harvester.py
opennews_admin.py 的素材关键词生成
Qwen3-VL 审核返回
向量素材库匹配结果
```

注意用户对素材要求非常高：

- 不要泛化政治人物图
- 不要素材库中 AI 新闻乱匹配黄仁勋/马斯克
- 新闻讲 Grok，就不能随便用手机图
- 网络爬取必须抓对应新闻/对应主体

### YouTube 发布失败

查：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker logs --tail 300 OmniHuman | grep -E 'YouTube|youtube|thumbnail|封面|publish|上传|失败'"
```

常见：

- OAuth 失效
- 上传限制
- 账号封禁/警告
- 封面生成失败
- gpt-image-2 限流

### 5090 GPU 掉线

如果 `nvidia-smi` Unknown Error：

- 不要相信服务 health
- 服务进程可能还在，但 GPU 实际不能用
- 需要重启主机或排查硬件/驱动/BIOS

用户曾问 BIOS 固定 Gen4 是否有用：

- 对 PCIe/5090 掉卡有一定可能帮助
- 但不是软件层确定性修复

---

## 17. 开发注意事项

1. 不要随意重置用户未提交修改。
2. 线上代码必须同步到 Docker 容器。
3. 改 OpenNews 自动化前，先看是否 YouTube 账号可用。
4. 改素材逻辑时，安全优先，宁可少图也不能裸露/无关/白板。
5. 改文案逻辑时，用户要的是新闻稿事实提炼，不是评论。
6. 改配音时，OpenNews 男声固定 `aiden`，女声 `serena`。
7. 数字人和房源实拍暂时不要随便切到 5090 本地模型，除非用户明确要求。
8. OpenNews 本地 Qwen 文案模型只用于 OpenNews，不要影响普通数字人/房源文案。
9. 普通数字人固定三段数字人结构不能破坏。
10. 房源实拍不能剪短用户视频、不能末帧补视频、不能静音补满。

---

## 18. 推荐下一步

新 AI 接手后建议按这个顺序做：

1. 先检查 git 状态：

```bash
cd /Users/saita/saita/ihouse-OmniHuman
git status --short
```

2. 检查生产容器健康：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"
```

3. 检查 5090 GPU：

```bash
sshpass -p 'admin@123' ssh -p 43612 -o StrictHostKeyChecking=no saita@office.ihousejapan.cn "nvidia-smi"
```

4. 检查最近 OpenNews job：

```bash
ssh -4 -p 8800 service@ihouseoffice.ddns.net "docker exec OmniHuman /bin/sh -lc 'ls -lt /app/output/opennews_batches/batch_jobs | head'"
```

5. 如果用户要继续 OpenNews 批次，先确保：

- 5090 GPU 正常
- YouTube 账号没有封禁/限流
- gpt-image-2 封面服务可用
- 素材安全策略开启

6. 如果用户要提交 GitHub：

```bash
git status --short
git add ...
git commit -m "..."
git push
```

---

## 19. 一句话给新 AI 的接手提醒

这个项目不是单纯网页项目，而是一个生产中的 AI 视频流水线。最重要的是：

```text
线上 Docker 同步、5090 GPU 状态、OpenNews 素材安全、YouTube 风控、数字人固定三段结构、房源视频时长匹配。
```

每次改动前先确认当前业务状态，尤其不要在 YouTube 账号被封、5090 GPU Unknown Error、OpenNews 批次 running 时贸然启动新的大任务。

