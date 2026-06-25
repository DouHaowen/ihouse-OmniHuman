# OpenNews 本地新闻素材图片库建设规范

目标：建立一个可控、安全、可长期复用的本地新闻图片库，用于 OpenNews 新闻视频制作。素材匹配必须优先准确，宁可少用图，也不能把错误实体图片塞进视频。

## 核心原则

1. 先建库，再匹配。不要每条新闻临时全网乱抓图片。
2. 标签必须从“大类标签”升级为“实体标签 + 场景标签 + 用途标签”。
3. 新闻讲具体人物、公司、机构时，必须优先命中对应实体素材。
4. 找不到实体素材时，才允许用安全通用场景图。
5. 找不到安全通用图时，宁可生成标题信息卡，也不要使用不确定图片。
6. 所有进入正式库的图片都必须通过管理员审核，并经过 5090 视觉模型打标签后再参与自动匹配。

## 素材匹配优先级

1. 精确实体素材
   例如新闻讲“黄仁勋”，优先匹配 `entity:nvidia_huang` 或 `entity:nvidia`。

2. 同实体相关场景
   例如新闻讲“英伟达 AI 芯片”，可匹配 `NVIDIA 发布会 / GPU / 数据中心 / 芯片晶圆`。

3. 同主题通用场景
   例如新闻讲“AI 投资增长”，可匹配 `数据中心 / 服务器机房 / AI 软件界面 / 科技办公场景`。

4. 安全信息图/标题卡
   当素材库没有准确图时，使用系统生成的安全文字信息卡。

## 禁止规则

1. 不能因为同属 AI 新闻，就把 OpenAI、英伟达、马斯克、苹果、机器人随便混用。
2. 金融新闻不能混入 AI 公司人物图，除非新闻主体明确是该公司股票。
3. 政治新闻不能混入军事冲突画面，除非新闻主体明确涉及军事行动。
4. 军事新闻禁止血腥、伤者、尸体、裸露、过度暴力图片。
5. 所有新闻视频禁止成人、裸露、擦边、内衣、泳装、医疗裸露、低俗图片。

## 推荐标签结构

每张图片至少包含下面几类标签：

- `domain:*`：大领域，例如 `domain:ai`、`domain:finance`、`domain:politics`
- `entity:*`：具体实体，例如 `entity:nvidia_huang`、`entity:grok`、`entity:white_house`
- `scene:*`：画面场景，例如 `scene:data_center`、`scene:press_briefing`
- `usage:*`：使用方式，例如 `usage:exact_entity`、`usage:generic_safe`
- `safety:*`：安全状态，例如 `safety:youtube_safe`

## 第一批必须建设的高频专题

| 专题 | 必备实体标签 | 场景标签 | 备注 |
| --- | --- | --- | --- |
| 英伟达/黄仁勋 | `entity:nvidia` `entity:nvidia_huang` | `scene:gpu` `scene:data_center` `scene:chip` | AI 芯片、股价、出口限制、算力新闻高频 |
| OpenAI/ChatGPT | `entity:openai` `entity:chatgpt` | `scene:ai_interface` `scene:office` | 不要混 Anthropic/Google/NVIDIA |
| Anthropic/Claude | `entity:anthropic` `entity:claude` | `scene:ai_interface` `scene:office` | 不要混 OpenAI |
| xAI/Grok | `entity:xai` `entity:grok` `entity:elon_musk` | `scene:ai_interface` `scene:data_center` | Grok 新闻必须命中 Grok/xAI/马斯克之一 |
| Amazon/AWS/Bedrock | `entity:amazon` `entity:aws` `entity:bedrock` | `scene:cloud` `scene:data_center` | 云服务/模型上架新闻 |
| Google/Gemini | `entity:google` `entity:gemini` | `scene:ai_interface` `scene:cloud` | 不要混 OpenAI |
| Apple/Siri/WWDC | `entity:apple` `entity:siri` `entity:wwdc` | `scene:iphone` `scene:developer_conference` | 苹果新闻专用 |
| 数据中心/服务器 | `entity:none` | `scene:data_center` `scene:server_rack` | AI 能源、算力、电力新闻通用 |
| 机器人 | `entity:robotics` | `scene:humanoid_robot` `scene:industrial_robot` | 只给机器人新闻使用 |
| 白宫/美国政府 | `entity:white_house` `entity:us_government` | `scene:press_briefing` `scene:government_building` | 政策、监管、总统发言 |
| 特朗普 | `entity:trump` | `scene:rally` `scene:press_conference` | 特朗普新闻专用 |
| 美联储/鲍威尔 | `entity:federal_reserve` `entity:jerome_powell` | `scene:central_bank` `scene:press_conference` | 利率、通胀、股市 |
| 华尔街/股市 | `entity:wall_street` | `scene:stock_board` `scene:trading_floor` | 泛金融，不要混科技人物 |
| 石油/能源 | `entity:opec` `entity:oil_market` | `scene:oil_tanker` `scene:refinery` `scene:pipeline` | 油价、中东、能源安全 |
| 美国房产 | `entity:us_housing` | `scene:suburban_home` `scene:real_estate_sign` `scene:apartment` | 房价、房贷、租金 |
| 移民/签证 | `entity:immigration` | `scene:passport` `scene:airport` `scene:visa_office` | 移民、留学、签证 |
| 军事/北约 | `entity:nato` `entity:us_military` | `scene:warship` `scene:fighter_jet` `scene:missile` `scene:drone` | 必须安全、无血腥 |
| 中东/伊朗/以色列 | `entity:iran` `entity:israel` `entity:hormuz` | `scene:map` `scene:oil_tanker` `scene:diplomacy` | 军事和能源交叉 |

## 入库工作流

1. 按专题收集图片到桌面审核文件夹。
2. 人工删除明显不准、不安全、低质量图片。
3. 导入素材库，状态进入 `pending`。
4. 管理员审核为 `approved`。
5. 5090 Qwen-VL 自动生成 `ai_summary / ai_tags / news_topics`。
6. 向量库写入或更新该素材。
7. OpenNews 生产时按实体/场景/向量匹配素材。

## 生产匹配策略

1. 先从新闻标题、正文、文案中抽取实体。
2. 如果有实体，素材必须命中实体或其强相关别名。
3. 如果没有实体，再按领域和场景匹配。
4. 如果匹配分低于阈值，不使用该图。
5. 不足素材时允许减少片段素材数，但不能使用错误图。
6. 仍不足时使用安全标题信息卡兜底。
