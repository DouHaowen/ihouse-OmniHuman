# iHouse AI Agent App API 规范

本文档用于 iOS / App 前端对接当前后端。原则是：网页端原有 `/api/...` 接口继续保留，App 端优先使用 `/api/app/...` 聚合接口；复杂上传任务可以继续调用原有生产接口。

## 基础信息

- Base URL: `https://aiagent.office.ihousejapan.cn`
- 数据格式: JSON；上传视频/图片时使用 `multipart/form-data`
- 下载文件: 直接 `GET` 下载地址
- 时间戳: Unix 秒级时间戳

## 认证

App 端不需要模拟浏览器 Cookie，使用 Bearer Token。

### 登录

`POST /api/app/auth/login`

请求:

```json
{
  "username": "aki",
  "password": "aki123"
}
```

返回:

```json
{
  "ok": true,
  "user": {
    "username": "aki",
    "role": "admin",
    "display_name": "aki",
    "interface_language": "zh-CN",
    "department_id": "real_estate",
    "target_market": "cn"
  },
  "access_token": "xxx",
  "token_type": "bearer",
  "expires_at": 1780000000,
  "expires_in": 2592000
}
```

后续请求统一加请求头:

```http
Authorization: Bearer <access_token>
```

### 当前用户

`GET /api/app/me`

返回当前登录用户。

### 退出

`POST /api/app/auth/logout`

Bearer Token 是无状态的，App 本地删除 token 即可。

## App 启动配置

`GET /api/app/bootstrap`

用途：App 启动后一次性拿用户、可选音色、主播、市场、部门、字幕模板、数字人引擎、文案模型、BGM 和当前任务。

返回核心字段:

```json
{
  "ok": true,
  "user": {},
  "options": {
    "voice_presets": [],
    "avatars": [],
    "target_markets": [],
    "departments": [],
    "digital_human_engines": [],
    "script_models": [],
    "property_bgm_tracks": []
  },
  "active_tasks": []
}
```

## 任务状态

### 当前任务列表

`GET /api/app/tasks`

返回当前用户可见的运行中任务，管理员可见全部。

### 单个任务轮询

`GET /api/app/tasks/{task_id}`

推荐 App 每 3-5 秒轮询一次。返回:

```json
{
  "ok": true,
  "task": {
    "task_id": "abcd1234",
    "mode": "property_video",
    "topic": "房源实拍成片",
    "status": "running",
    "step": 2,
    "total_steps": 4,
    "created_at": 1780000000,
    "history_id": "1780000000_full_xxx",
    "messages": [
      {
        "message": "正在生成配音...",
        "step": 2,
        "total_steps": 4,
        "time": 1780000000
      }
    ],
    "result_ready": false
  }
}
```

状态值通常为:

- `running`
- `done`
- `error`
- `cancelled`

网页端仍支持 SSE: `GET /api/tasks/{task_id}/progress`。App 如果不想接 SSE，用 `/api/app/tasks/{task_id}` 轮询即可。

## 历史记录和成片

### 历史列表

`GET /api/app/history?limit=50`

返回当前用户可见历史。每条包含:

- `history_id`
- `title`
- `topic`
- `created_at`
- `total_duration`
- `segment_count`
- `lifecycle`

### 历史详情

`GET /api/app/history/{history_id}`

返回:

```json
{
  "ok": true,
  "history": {},
  "files": []
}
```

`history` 是完整结果，`files` 是可下载文件列表。

### 统一成片列表

`GET /api/app/ready-videos?video_type=all&limit=50`

`video_type` 可选:

- `all`
- `digital_human`
- `property_video`
- `opennews`

每条成片包含:

- `title`
- `type`
- `type_label`
- `created_at`
- `completed_at`
- `duration`
- `vertical_url`
- `horizontal_url`
- `variants`

注意:

- 数字人视频、房源实拍视频、OpenNews 新闻视频都可以从这里统一取。
- OpenNews 的 `published_at` 是新闻原文发布时间，不是系统成片时间。
- 系统成片时间统一看 `completed_at`。

## 数字人视频流程

### 1. 生成文案

`POST /api/script-preview`

类型: `multipart/form-data`

字段:

- `topic_text`: 用户输入主题或链接
- `source_url`: 可选
- `topic`: 兼容旧字段，可选
- `use_web_search`: `true` / `false`
- `target_market`: `cn` / `tw` / `jp`
- `department_id`: `real_estate` / `robotics`
- `script_model`: 默认 `api_relay`
- `digital_human_engine`: 默认火山数字人

返回 `script` 和 `preview`。

### 2. 提交生产

`POST /api/produce`

类型: `multipart/form-data`

字段:

- `topic_text`
- `source_url`
- `topic`
- `script_json`: 上一步返回的 `script` JSON 字符串
- `voice_preset_id`
- `avatar_id`
- `speed`
- `use_web_search`
- `target_market`
- `department_id`
- `script_model`
- `digital_human_engine`

返回:

```json
{
  "task_id": "abcd1234",
  "reused_existing": false
}
```

然后用 `/api/app/tasks/{task_id}` 轮询。

## 房源实拍成片流程

### 1. 可选：AI 分析视频

`POST /api/property-video/analyze`

类型: `multipart/form-data`

字段:

- `videos`: 多个视频文件
- `target_market`
- `notes`: 销售补充说明

返回 AI 分析结果和建议文案。

### 2. 提交成片

`POST /api/property-video/jobs`

类型: `multipart/form-data`

字段:

- `videos`: 一个或多个视频
- `script_text`: 解说文案
- `voice_preset_id`
- `speed`
- `target_market`
- `bgm_item_id`
- `bgm_volume`
- `timeline_segments`: 可选，一镜到底时间轴 JSON 字符串

返回 `task_id`，然后轮询 `/api/app/tasks/{task_id}`。

## OpenNews 新闻视频流程

### 获取新闻源和分区

`GET /api/opennews/sources`

### 抓取英文热点

`POST /api/opennews/trends/search`

JSON 请求，按分区和时间范围抓取热点。

### 自动抓取批次

- `GET /api/opennews/batches/config`
- `POST /api/opennews/batches/config`
- `POST /api/opennews/batches/run-now`
- `GET /api/opennews/batches`

### 一站式制作选中新闻

`POST /api/opennews/batches/produce`

JSON 请求:

```json
{
  "item_ids": ["batch_item_id_1", "batch_item_id_2"],
  "target_market": "cn",
  "voice_preset_id": "mandarin_male",
  "aspect_ratio": "horizontal",
  "youtube_auto_publish": true,
  "youtube_privacy_status": "public",
  "youtube_aspects": ["horizontal", "vertical"],
  "notes": "做成简短新闻口播"
}
```

返回 `job_id`，查询:

`GET /api/opennews/batches/jobs/{job_id}`

## OpenNews 外部审核接口

给外部系统/同事使用，认证头:

```http
X-Token: NEWSdesk8821Aki6000HsVp
```

核心接口:

- `GET /api/external/opennews/candidate-batches?limit=10`
- `POST /api/external/opennews/produce-selected`
- `GET /api/external/opennews/jobs/{job_id}`
- `GET /api/external/opennews/ready-videos?limit=50`

这些接口已经会在选定新闻后自动生成横屏/竖屏视频，并可自动发布 YouTube。

## 素材库

### 列表

`GET /api/material-library`

查询参数:

- `q`
- `kind`
- `category`
- `uploader`

### 上传

`POST /api/material-library/upload`

类型: `multipart/form-data`

字段:

- `files`: 多个文件
- `notes`: 备注

### 管理员审核

- `POST /api/material-library/{item_id}/review`
- `POST /api/material-library/review-batch`
- `DELETE /api/material-library/{item_id}`

## 下载规则

所有下载 URL 都可以直接 `GET`。

App 端如果下载 `/api/app/ready-videos` 返回的 `vertical_url` / `horizontal_url`，仍要带:

```http
Authorization: Bearer <access_token>
```

外部 OpenNews 接口返回的下载地址则带:

```http
X-Token: NEWSdesk8821Aki6000HsVp
```

## 建议 App 首版接入顺序

1. `POST /api/app/auth/login`
2. `GET /api/app/bootstrap`
3. `POST /api/script-preview`
4. `POST /api/produce`
5. `GET /api/app/tasks/{task_id}`
6. `GET /api/app/history`
7. `GET /api/app/ready-videos`
8. 房源视频再接 `property-video/analyze` 和 `property-video/jobs`
9. OpenNews 再接 batches 系列接口
