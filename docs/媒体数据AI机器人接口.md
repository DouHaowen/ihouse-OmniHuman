# 媒体数据 AI 机器人接口

本接口向内部 AI 机器人提供本系统发布视频的只读事实数据。数据来源是系统发布回执和平台回采结果，不包含平台上由人工发布、且系统没有发布回执的内容。

## 接口

- 地址：`GET https://aiagent.office.ihousejapan.cn/api/external/media-insights/agent-context`
- 默认统计范围：最近 30 天
- 响应格式：JSON
- 缓存策略：`Cache-Control: no-store`
- Schema 版本：`1.0`

查询参数：

| 参数 | 默认值 | 范围 | 含义 |
| --- | ---: | ---: | --- |
| `days` | 30 | 1-3650 | 区间统计的最近天数 |
| `top_limit` | 10 | 0-100 | 按浏览量返回的热门视频数量 |
| `recent_limit` | 20 | 0-100 | 最近发布视频数量 |
| `comment_limit` | 20 | 0-100 | 最近已回采评论数量 |

## 认证

服务端对服务端调用使用单独的只读密钥：

```http
Authorization: Bearer <MEDIA_INSIGHTS_AGENT_API_KEY>
```

也支持 `X-Media-Insights-Key` 或 `X-API-Key` 请求头。不要把密钥放在 URL 查询参数、前端代码或日志中。

现有 JClaw 小程序也可以使用 `app=ihouse-media-insights` 的 JClaw Lab JWT，通过 `Authorization: Bearer <JWT>` 或 `X-JClaw-Lab-Token` 调用。

调用示例：

```bash
curl -sS \
  -H "Authorization: Bearer $MEDIA_INSIGHTS_AGENT_API_KEY" \
  "https://aiagent.office.ihousejapan.cn/api/external/media-insights/agent-context?days=30&top_limit=10&recent_limit=20&comment_limit=20"
```

## 返回内容

顶层字段：

| 字段 | 含义 |
| --- | --- |
| `generated_at_iso` | 本次事实包生成时间 |
| `range` | 本次区间统计的起止时间 |
| `system` | 账号、频道、支持/已配置/已发布平台、发布、制作任务和评论汇总 |
| `platforms` | YouTube、Facebook、X 的总量和区间数据 |
| `channels` | 各频道关联账号及发布统计 |
| `accounts` | 各账号的绑定状态、数据可读状态、频道和发布统计 |
| `top_videos` | 区间内按浏览量从高到低的视频 |
| `recent_videos` | 最近发布的视频 |
| `recent_comments` | 最近成功回采的评论正文 |
| `sync` | 平台数据同步状态 |

每个汇总对象同时提供：

- `total`：数据库当前保存的全部历史发布数据。
- `range`：`days` 指定区间内发布的数据。
- `published_video_count`：平台发布记录数。同一成片发布到 3 个平台，计为 3 条。
- `production_task_count`：按 `history_id` 去重后的系统制作任务数。
- `view_count`、`like_count`、`comment_count`：平台可读取部分的累计指标。
- `metrics_readable_count`、`metrics_unavailable_count`、`metrics_coverage_ratio`：数据覆盖情况。

账号的 `analytics_status`：

- `ready`：区间内指标均可读取。
- `partial`：区间内只有部分指标可读取。
- `unavailable`：区间内有发布记录，但平台指标暂不可读取。
- `waiting`：区间内没有待统计的发布记录。

## AI 使用规则

1. 回答时区分 `total` 和 `range`，并说明使用的统计区间。
2. 区分“平台发布记录数”和“制作任务数”，不要混用。
3. `analytics_status=unavailable` 或指标值为 `null` 时，只能回答“暂不可读取”，不能回答为 0。
4. 涉及当前数据时参考 `generated_at_iso`；同步失败时同时检查 `sync`。
5. 只把接口返回内容视为本系统已发布内容，不推断账号上人工发布的视频。
