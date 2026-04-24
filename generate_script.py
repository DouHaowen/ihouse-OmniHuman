"""
文案生成模块
输入选题 → 输出完整播报稿+时间轴
"""

import json
import os
import re
import time
from typing import Any

import anthropic
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SCRIPT_MODEL_CLAUDE = "claude"
SCRIPT_MODEL_GLM = "glm_5_1"
SCRIPT_MODEL_CHATGPT = "chatgpt"

MAX_DIGITAL_HUMAN_TOTAL_SECONDS = 35
TARGET_DIGITAL_HUMAN_TOTAL_SECONDS = 30
MAX_DIGITAL_HUMAN_SEGMENTS = 3


class UpstreamBusyError(Exception):
    pass


class OpenAIFallbackUnavailableError(Exception):
    pass

SYSTEM_PROMPT = """你是一个专业的AI短视频内容制作助手，服务于iHouse公司。

每次用户输入一个选题，你必须严格按照以下JSON格式输出内容，不要输出任何其他文字：

{
  "title": "视频标题",
  "cover_title": "封面标题（20字以内，吸引点击）",
  "total_duration": 总秒数,
  "segments": [
    {
      "type": "digital_human",  // 数字人段落
      "start": 开始秒数,
      "end": 结束秒数,
      "duration": 时长秒数,
      "script": "播报文案（严格控制在15秒内，中文4-5字/秒）",
      "action": "数字人动作描述"
    },
    {
      "type": "material",  // 素材段落
      "start": 开始秒数,
      "end": 结束秒数,
      "duration": 时长秒数,
      "script": "配音文案",
      "material_keyword": "素材关键词（按目标市场语言输出，给运营查看）",
      "material_search_keyword": "素材检索关键词（英文，给素材库检索使用）",
      "material_desc": "素材描述"
    }
  ],
  "social_post": "面向当前目标市场的SNS发布文案"
}

规则：
1. 数字人每段严格≤15秒
2. 总视频60~120秒
3. 不要再强制数字人和素材交替出现，要根据内容功能智能判断：观点表达、开场抓人、转折总结更适合 digital_human；政策说明、数据案例、时间线、流程步骤、画面展示更适合 material
4. 开头和结尾必须是数字人段落，中间固定保留 1 段短过渡型数字人，所以整条视频固定为 3 段数字人
5. 中间那段数字人不要承担复杂信息，只用于承上启下、让主播短暂露面，时长尽量控制在 6~10 秒，文案要短，不讲长数据、长流程和复杂案例
6. 数字人总时长目标约 30 秒，硬上限 35 秒；其余内容优先用素材承接，以降低数字人频率和成本
7. social_post 只输出一份，必须面向当前目标市场，语气适合社交媒体发布
8. 所有数字必须是整数
9. digital_human 的 action 只能描述主播坐在台前即可完成的动作与表情，例如点头、微笑、自然眨眼、轻微摆头、表情认真、语气坚定等
10. digital_human 的 action 严禁出现任何凭空道具、场景、背景元素或夸张肢体动作，例如不能写手持计算器、指向图表、站起身、走动、在街头、在客厅等
11. material_desc 只描述素材画面本身应该出现什么内容，不要描述数字人主播，也不要写镜头外的设定
12. material_keyword 要跟随目标市场语言输出，给运营直接阅读
13. material_search_keyword 必须使用简洁准确的英文关键词，专门给素材库检索使用"""

WEB_SEARCH_GUIDANCE = """

当前任务已启用实时联网检索。请严格遵守以下额外要求：
1. 涉及“最新、最近、政策、法规、发布、新闻、数据、机器人、移民、归化”等时效性信息时，优先使用联网检索。
2. 只在有可信来源支持时再写“最新”或具体政策结论，避免凭空编造。
3. 如果检索结果不充分，就降低表述确定性，例如改为“近期有相关讨论”而不是断言结论。
4. 优先参考权威官网、官方博客、主流媒体、监管机构和公司公告。
5. 即使启用了联网检索，最终输出仍然必须是严格 JSON，不要输出引用说明、markdown 或额外解释。"""



TARGET_MARKET_RULES = {
    "cn": {
        "name": "中国市场",
        "language_label": "简体中文",
        "content_rules": "整条输出必须使用简体中文。title、cover_title、segments 中的 script、social_post 都要使用面向中国大陆用户的简体中文表达。material_keyword 也使用简体中文，但 material_search_keyword 必须保留英文。",
    },
    "tw": {
        "name": "台湾市场",
        "language_label": "繁體中文",
        "content_rules": "整條輸出必須使用繁體中文。title、cover_title、segments 中的 script、social_post 全部都要使用繁體中文，措辭要更貼近台灣使用者。嚴禁輸出簡體字。material_keyword 也必須使用繁體中文，但 material_search_keyword 必須保留英文。",
    },
    "jp": {
        "name": "日本市场",
        "language_label": "日语",
        "content_rules": "整条输出必须使用自然日语。title、cover_title、segments 中的 script、social_post 全部都要使用日语，不要夹杂中文。material_keyword 也使用日语，但 material_search_keyword 必须保留英文。",
    },
}

DEPARTMENT_RULES = {
    "real_estate": "内容角度更偏房地产、置业、房屋持有、移居置业、住宅决策与生活方式。",
    "robotics": "内容角度更偏机器人、AI硬件、智能设备、产业动态、产品进展与应用场景。",
}

XIAOHONGSHU_SAFE_MODE_GUIDANCE = """

如果目标市场是中国市场，请额外遵守“小红书知识科普安全模式”：
1. 内容定位必须是知识科普、新闻解读、生活方式认知差，不要写成房产中介广告。
2. 严禁直接提及公司品牌、机构品牌、团队品牌，例如 iHouse、IHOUSE、艾豪斯 等。
3. 严禁出现顾问身份、自我介绍式导流，例如“我是某某顾问”“我们团队”“欢迎咨询我们”。
4. 严禁出现咨询导流词，例如私信、微信、加微、扫码、联系客服、联系我们、一对一解答、关注我们获取服务。
5. 严禁出现强交易或强营销表达，例如上车、抄底、捡漏、最佳时机、赶紧买、立即行动、稳赚、高收益、资产配置首选。
6. 标题、封面标题和正文要更像中立问题句、误区拆解、制度科普或新闻解读，不要像销售文案。
7. 结尾只允许轻互动，例如“你最在意哪一点？”“你之前最大的误解是什么？”，不要出现转化型 CTA。
8. social_post 也必须遵守以上规则，不得出现品牌、顾问、咨询和导流表达。
"""

CN_MARKETING_RISK_TERMS = [
    "ihouse", "艾豪斯", "咨询", "私信", "微信", "加微", "扫码", "联系客服", "联系我们",
    "顾问", "团队", "置业顾问", "一对一", "关注我们", "帮你买房", "带你买房", "上车",
    "抄底", "捡漏", "稳赚", "高收益", "高回报", "投资回报", "最佳时机", "立即行动",
    "抓紧", "限时", "资产配置首选",
]

CN_MARKETING_RISK_PATTERNS = [
    ("置业引导", r"(如果你|假如你|对于.*的你|正在考虑|打算).{0,12}(置业|买房|购房|入手)"),
    ("交易导向", r"(是否值得买|现在还能买吗|适合买房吗|值不值得入手|值得关注的基本指标)"),
    ("评论区导流", r"(欢迎在评论区|评论区告诉我|留言告诉我|欢迎留言)"),
]


def _build_context_guidance(target_market: str, department_id: str) -> str:
    market = TARGET_MARKET_RULES.get(target_market, TARGET_MARKET_RULES["cn"])
    department_rule = DEPARTMENT_RULES.get(department_id, "内容角度保持通用商业资讯表达。")
    extra = XIAOHONGSHU_SAFE_MODE_GUIDANCE if target_market == "cn" else ""
    return f"""

当前内容目标市场：{market['name']}
语言要求：{market['language_label']}
市场输出规则：{market['content_rules']}
当前业务部门：{department_id}
部门表达要求：{department_rule}
{extra}
"""


def _digital_human_action_fallback(target_market: str) -> str:
    if target_market == "tw":
        return "坐在台前，語氣自然穩定，適度點頭並自然眨眼"
    if target_market == "jp":
        return "卓上で落ち着いて話し、自然にうなずきながら穏やかに締める"
    return "坐在台前，语气自然稳重，轻微点头并自然眨眼"


def _material_desc_fallback(target_market: str) -> str:
    if target_market == "tw":
        return "以新聞截圖、數據圖表、案例畫面或現場實景來支撐這段旁白，不要出現主播。"
    if target_market == "jp":
        return "このナレーションを支えるニュース画面、データ図表、事例映像、現地の実景を見せ、司会者は出さない。"
    return "用新闻截图、数据图表、案例画面或现场实景来支撑这段旁白，不要出现主播。"


def _material_search_keyword_fallback(department_id: str) -> str:
    if department_id == "robotics":
        return "robotics ai technology product demonstration industry footage"
    return "news data infographic real estate policy city footage"


def _material_keyword_fallback(script_text: str, target_market: str) -> str:
    cleaned = re.sub(r"\s+", " ", (script_text or "").strip())
    limit = 20 if target_market in {"cn", "tw"} else 28
    return cleaned[:limit] or ("新闻素材" if target_market == "cn" else "新聞素材" if target_market == "tw" else "ニュース素材")


def _middle_transition_fallback(target_market: str) -> str:
    if target_market == "tw":
        return "但真正關鍵的，其實是接下來這一點。"
    if target_market == "jp":
        return "ただ、本当に大事なのはこの次のポイントです。"
    return "但真正关键的，其实是接下来这一点。"


def _shorten_middle_transition_script(script_text: str, target_market: str) -> str:
    text = re.sub(r"\s+", " ", (script_text or "").strip())
    if not text:
        return _middle_transition_fallback(target_market)
    parts = [part.strip(" ，,。；;！!？?") for part in re.split(r"[。！？!?；;，,]", text) if part.strip()]
    candidate = parts[0] if parts else text
    max_len = 24 if target_market in {"cn", "tw"} else 34
    if len(candidate) > max_len:
        candidate = candidate[:max_len].rstrip("，,、 ")
    if len(candidate) < 6:
        return _middle_transition_fallback(target_market)
    suffix = "。" if target_market in {"cn", "tw"} else "。"
    return candidate + ("" if candidate.endswith(("。", "！", "？", ".", "!", "?")) else suffix)


def _segment_type_priority(seg: dict) -> int:
    script_text = (seg.get("script") or "").strip()
    text = script_text.lower()
    strong_anchor_tokens = [
        "最后", "總結", "总结", "所以", "其實", "其实", "為什麼", "为什么", "你可能",
        "很多人", "重點", "重点", "關鍵", "关键", "先講結論", "先讲结论",
        "結論", "结论", "最後一個", "最后一个", "まず結論", "つまり", "要するに",
    ]
    info_heavy_tokens = [
        "數據", "数据", "比例", "流程", "步驟", "步骤", "法規", "法规", "政策",
        "時間線", "时间线", "案例", "金額", "金额", "%", "年", "月", "日", "億",
        "万", "萬", "条", "項", "项", "第", "政府", "公告", "報告", "报告",
    ]
    score = 0
    if any(token in script_text for token in strong_anchor_tokens) or any(token in text for token in ["why", "summary", "key", "important", "finally"]):
        score += 4
    if any(token in script_text for token in info_heavy_tokens):
        score -= 3
    if len(script_text) <= 42:
        score += 1
    if len(re.findall(r"\d", script_text)) >= 2:
        score -= 2
    return score


def _convert_segment_to_material(seg: dict, target_market: str, department_id: str) -> dict:
    converted = {
        "type": "material",
        "start": int(seg.get("start", 0) or 0),
        "end": int(seg.get("end", 0) or 0),
        "duration": int(seg.get("duration", 0) or 0),
        "script": (seg.get("script") or "").strip(),
        "material_keyword": seg.get("material_keyword") or _material_keyword_fallback(seg.get("script", ""), target_market),
        "material_search_keyword": seg.get("material_search_keyword") or _material_search_keyword_fallback(department_id),
        "material_desc": seg.get("material_desc") or _material_desc_fallback(target_market),
    }
    return converted


def _convert_segment_to_digital_human(seg: dict, target_market: str) -> dict:
    return {
        "type": "digital_human",
        "start": int(seg.get("start", 0) or 0),
        "end": int(seg.get("end", 0) or 0),
        "duration": int(seg.get("duration", 0) or 0),
        "script": (seg.get("script") or "").strip(),
        "action": seg.get("action") or _digital_human_action_fallback(target_market),
    }


def _is_short_transition_candidate(seg: dict) -> bool:
    duration = int(seg.get("duration", 0) or 0)
    script_text = (seg.get("script") or "").strip()
    if 6 <= duration <= 10 and len(script_text) <= 48:
        return True
    transition_tokens = [
        "但真正关键", "但更重要", "接下来", "再看一个", "还有一点", "先别急", "不过真正",
        "但问题是", "但重点是", "說到這裡", "接著看", "次に", "ここで大事", "ただ本当に重要",
    ]
    return any(token in script_text for token in transition_tokens)


def _rebalance_segment_mix(data: dict, target_market: str, department_id: str) -> dict:
    segments = list((data or {}).get("segments") or [])
    if not segments:
        return data

    normalized_segments = []
    for seg in segments:
        if seg.get("type") == "digital_human":
            normalized_segments.append(_convert_segment_to_digital_human(seg, target_market))
        else:
            normalized_segments.append(_convert_segment_to_material(seg, target_market, department_id))
    segments = normalized_segments

    if segments[0].get("type") != "digital_human":
        segments[0] = _convert_segment_to_digital_human(segments[0], target_market)
    if len(segments) > 1 and segments[-1].get("type") != "digital_human":
        segments[-1] = _convert_segment_to_digital_human(segments[-1], target_market)

    def dh_indices() -> list[int]:
        return [idx for idx, seg in enumerate(segments) if seg.get("type") == "digital_human"]

    def dh_total_duration() -> int:
        return sum(int(seg.get("duration", 0) or 0) for seg in segments if seg.get("type") == "digital_human")

    protected = {0, len(segments) - 1}
    target_middle_index = None
    candidate_pool = [idx for idx in range(1, len(segments) - 1)]
    if candidate_pool:
        preferred = [idx for idx in candidate_pool if _is_short_transition_candidate(segments[idx])]
        source_pool = preferred or candidate_pool
        target_middle_index = min(
            source_pool,
            key=lambda idx: (
                abs(idx - (len(segments) // 2)),
                -_segment_type_priority(segments[idx]),
                int(segments[idx].get("duration", 0) or 0),
            ),
        )
        segments[target_middle_index] = _convert_segment_to_digital_human(segments[target_middle_index], target_market)
        segments[target_middle_index]["script"] = _shorten_middle_transition_script(segments[target_middle_index].get("script", ""), target_market)

    interior_dh = [idx for idx in dh_indices() if idx not in protected]
    if interior_dh:
        keep_indices = {target_middle_index} if target_middle_index is not None else set()
        for idx in interior_dh:
            if idx not in keep_indices:
                segments[idx] = _convert_segment_to_material(segments[idx], target_market, department_id)

    while len(dh_indices()) > MAX_DIGITAL_HUMAN_SEGMENTS:
        removable = [idx for idx in dh_indices() if idx not in protected]
        if not removable:
            break
        idx = min(removable, key=lambda item: (_segment_type_priority(segments[item]), int(segments[item].get("duration", 0) or 0)))
        segments[idx] = _convert_segment_to_material(segments[idx], target_market, department_id)

    data["segments"] = segments
    return data

def _iter_text_values(value: Any):
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_text_values(item)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_text_values(item)


def _find_cn_marketing_hits(data: dict) -> list[str]:
    raw_texts = [text for text in _iter_text_values(data) if isinstance(text, str)]
    haystack = "\n".join(raw_texts).lower()
    raw_haystack = "\n".join(raw_texts)
    hits = []
    for term in CN_MARKETING_RISK_TERMS:
        if term.lower() in haystack:
            hits.append(term)
    for label, pattern in CN_MARKETING_RISK_PATTERNS:
        if re.search(pattern, raw_haystack, flags=re.IGNORECASE):
            hits.append(f"[pattern]{label}")
    return hits


def _rewrite_script_for_cn_safety(topic: str, data: dict, enable_web_search: bool, target_market: str, department_id: str, provider: str = SCRIPT_MODEL_CLAUDE) -> tuple[dict, dict]:
    hits = _find_cn_marketing_hits(data)
    prompt = f"""
下面是一份已经生成好的短视频 JSON 脚本，但它面向中国市场，需要进一步改成“小红书知识科普安全模式”。

选题：{topic}
当前脚本：
{json.dumps(data, ensure_ascii=False, indent=2)}

已命中的高风险词：{', '.join(hits) if hits else '无'}

请你在不改变整体主题、不改变 JSON 结构的前提下，重写 title、cover_title、segments 中的 script、social_post，让它变成更中立的知识型内容。
必须遵守：
1. 保留 total_duration 与每段的 type/start/end/duration 不变。
2. digital_human 继续保留 action 字段，但动作描述只允许坐在台前能完成的动作。
3. material_keyword、material_search_keyword、material_desc 保持原意，可以按需要微调表达，但不要改成营销口吻。
4. 严禁出现品牌名、公司名、顾问、团队、咨询导流、微信私信、强交易词、收益承诺。
5. 标题和封面标题更像问题句、误区拆解、制度科普或新闻解读。
6. social_post 结尾只允许轻互动，不允许转化导流。
7. 只返回合法 JSON，本次不要输出任何解释。
8. 避免第二人称交易导向表达，例如“如果你在考虑日本置业”“如果你正打算买房”。
9. 避免“欢迎在评论区告诉我”这类偏运营导向收口，改成更克制、更中立的问题句。
"""
    rewritten, usage = _request_json_by_provider(provider, prompt, max_tokens=4200, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
    return rewritten, usage


def _rewrite_segment_for_cn_safety(topic: str, script_data: dict, segment_index: int, segment: dict, enable_web_search: bool, target_market: str, department_id: str, provider: str = SCRIPT_MODEL_CLAUDE) -> tuple[dict, dict]:
    prompt = f"""
下面是一条面向中国市场的小红书知识型短视频脚本中的单个段落，请你把它改成更安全、更中立的科普表达。

选题：{topic}
整条脚本：
{json.dumps(script_data, ensure_ascii=False, indent=2)}

当前段落：
{json.dumps(segment, ensure_ascii=False, indent=2)}

要求：
1. 保留 type/start/end/duration 不变。
2. 如果是 digital_human，只返回 type/start/end/duration/script/action。
3. 如果是 material，只返回 type/start/end/duration/script/material_keyword/material_search_keyword/material_desc。
4. 严禁品牌名、公司名、顾问身份、咨询导流、私信微信、强交易词和收益承诺。
5. 表达要更像知识科普、误区拆解、制度解释，而不是营销文案。
6. 只返回合法 JSON，不要输出解释。
7. 避免第二人称交易导向表达，例如“如果你在考虑日本置业”“如果你正打算买房”。
8. 避免“欢迎在评论区告诉我”这类偏运营导向收口，改成更克制、更中立的问题句。
"""
    rewritten, usage = _request_json_by_provider(provider, prompt, max_tokens=1800, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
    return rewritten, usage


def _extract_json_text(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return raw.strip()


def _extract_usage_from_message(message: Any) -> dict:
    usage_obj = getattr(message, 'usage', None)
    usage = {
        'input_tokens': 0,
        'output_tokens': 0,
        'cache_creation_input_tokens': 0,
        'cache_read_input_tokens': 0,
        'web_search_calls': 0,
    }
    if usage_obj is not None:
        for key in ('input_tokens', 'output_tokens', 'cache_creation_input_tokens', 'cache_read_input_tokens'):
            try:
                usage[key] = int(getattr(usage_obj, key, 0) or 0)
            except Exception:
                usage[key] = 0
    for block in getattr(message, 'content', []) or []:
        block_type = getattr(block, 'type', '') or ''
        if 'web_search' in block_type or 'server_tool_use' in block_type:
            usage['web_search_calls'] += 1
    return usage


def _merge_usage(base: dict | None, extra: dict | None) -> dict:
    merged = dict(base or {})
    for key, value in (extra or {}).items():
        merged[key] = int(merged.get(key, 0) or 0) + int(value or 0)
    return merged


def _get_openai_api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def _get_glm_api_key() -> str:
    return (os.getenv("ZHIPUAI_API_KEY") or os.getenv("GLM_API_KEY") or "").strip()


def _get_openai_fallback_model() -> str:
    return (os.getenv("OPENAI_FALLBACK_MODEL") or "gpt-5-mini").strip()


def _get_openai_compat_fallback_model() -> str:
    return (os.getenv("OPENAI_COMPAT_FALLBACK_MODEL") or "gpt-5-mini").strip()


def _is_retryable_anthropic_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    text = str(exc).lower()
    if status_code in {429, 500, 502, 503, 504, 529}:
        return True
    return any(token in text for token in ["overloaded_error", "overloaded", "rate limit", "rate_limit", "timeout", "temporarily unavailable"])


def _is_anthropic_overloaded_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    text = str(exc).lower()
    return status_code == 529 or "overloaded_error" in text or "overloaded" in text


def _is_anthropic_billing_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "credit balance is too low" in text
        or "purchase credits" in text
        or "plans & billing" in text
        or "billing" in text and "anthropic" in text
    )


def _create_message_with_retry(**kwargs):
    attempts = max(1, int(os.getenv("ANTHROPIC_RETRY_ATTEMPTS", "6")))
    base_delay = max(1.0, float(os.getenv("ANTHROPIC_RETRY_BASE_DELAY_SECONDS", "3")))
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return client.messages.create(**kwargs)
        except Exception as exc:
            last_error = exc
            if (_is_anthropic_overloaded_error(exc) or _is_anthropic_billing_error(exc)) and _get_openai_api_key():
                fallback_reason = "Claude 当前过载" if _is_anthropic_overloaded_error(exc) else "Claude 账户余额不足"
                raise UpstreamBusyError(f"{fallback_reason}，切换到 OpenAI 备用模型") from exc
            if not _is_retryable_anthropic_error(exc) or attempt >= attempts:
                break
            time.sleep(base_delay * attempt)
    if last_error and _is_retryable_anthropic_error(last_error):
        raise UpstreamBusyError("上游文案服务暂时繁忙，请稍后重试") from last_error
    if last_error:
        raise last_error
    raise ValueError("Claude 请求失败")


def _extract_message_text(message: Any) -> str:
    parts = []
    for block in getattr(message, 'content', []) or []:
        if getattr(block, 'type', '') == 'text' and getattr(block, 'text', ''):
            parts.append(block.text)
    text = "\n".join(part.strip() for part in parts if part and part.strip()).strip()
    if not text:
        raise ValueError('Claude 未返回可解析的文本内容')
    return text


def _extract_openai_text(payload: dict) -> str:
    choices = payload.get("choices") or []
    if not choices:
        raise ValueError("OpenAI 未返回 choices")
    message = choices[0].get("message") or {}
    content = message.get("content", "")
    if isinstance(content, str):
        text = content.strip()
    elif isinstance(content, list):
        text = "\n".join(
            item.get("text", "").strip()
            for item in content
            if isinstance(item, dict) and item.get("type") in {"text", "output_text"} and item.get("text")
        ).strip()
    else:
        text = ""
    if not text:
        raise ValueError("OpenAI 未返回可解析的文本内容")
    return text


def _extract_openai_responses_text(payload: dict) -> str:
    output_text = (payload.get("output_text") or "").strip()
    if output_text:
        return output_text

    texts = []
    for item in payload.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if not isinstance(content, dict):
                continue
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                texts.append(str(content.get("text")).strip())
    merged = "\n".join(part for part in texts if part).strip()
    if not merged:
        raise ValueError("OpenAI Responses API 未返回可解析的文本内容")
    return merged


def _extract_usage_from_openai_payload(payload: dict) -> dict:
    usage_obj = payload.get("usage") or {}
    return {
        "input_tokens": int(usage_obj.get("prompt_tokens", 0) or 0),
        "output_tokens": int(usage_obj.get("completion_tokens", 0) or 0),
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "web_search_calls": 0,
    }


def _extract_usage_from_glm_payload(payload: dict) -> dict:
    usage_obj = payload.get("usage") or {}
    web_search_calls = 0
    for choice in payload.get("choices") or []:
        if not isinstance(choice, dict):
            continue
        message = choice.get("message") or {}
        tool_calls = message.get("tool_calls") or []
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            if (tool_call.get("type") or "") == "web_search":
                web_search_calls += 1
    return {
        "input_tokens": int(usage_obj.get("prompt_tokens", 0) or 0),
        "output_tokens": int(usage_obj.get("completion_tokens", 0) or 0),
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "web_search_calls": web_search_calls,
    }


def _extract_usage_from_openai_responses_payload(payload: dict) -> dict:
    usage_obj = payload.get("usage") or {}
    input_tokens = 0
    output_tokens = 0
    web_search_calls = 0
    if isinstance(usage_obj.get("input_tokens"), int):
        input_tokens = int(usage_obj.get("input_tokens", 0) or 0)
    elif isinstance((usage_obj.get("input_tokens_details") or {}).get("total_tokens"), int):
        input_tokens = int((usage_obj.get("input_tokens_details") or {}).get("total_tokens", 0) or 0)
    if isinstance(usage_obj.get("output_tokens"), int):
        output_tokens = int(usage_obj.get("output_tokens", 0) or 0)
    elif isinstance((usage_obj.get("output_tokens_details") or {}).get("total_tokens"), int):
        output_tokens = int((usage_obj.get("output_tokens_details") or {}).get("total_tokens", 0) or 0)
    for item in payload.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        if (item.get("type") or "") == "web_search_call":
            web_search_calls += 1
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "web_search_calls": web_search_calls,
    }


def _has_expected_script_shape(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    if not isinstance(data.get("title"), str):
        return False
    if not isinstance(data.get("cover_title"), str):
        return False
    if not isinstance(data.get("social_post"), str):
        return False
    if not isinstance(data.get("segments"), list) or not data.get("segments"):
        return False
    for seg in data.get("segments", []):
        if not isinstance(seg, dict):
            return False
        if seg.get("type") not in {"digital_human", "material"}:
            return False
        for key in ("start", "end", "duration"):
            if not isinstance(seg.get(key), int):
                return False
        if not isinstance(seg.get("script"), str):
            return False
    return True


def _repair_schema_with_openai(raw_payload: dict, max_tokens: int, target_market: str = "cn", department_id: str = "real_estate") -> tuple[dict, dict]:
    api_key = _get_openai_api_key()
    if not api_key:
        raise OpenAIFallbackUnavailableError("未配置 OPENAI_API_KEY")
    repair_prompt = f"""
下面这份 JSON 不是 iHouse 系统需要的最终结构。请你在保留原始主题信息的前提下，把它重写成 iHouse 规定的严格 JSON 结构。

必须满足：
1. 顶层只允许包含：title, cover_title, total_duration, segments, social_post
2. segments 必须是数组，且每一段只允许是以下两种结构之一：
   - digital_human: type/start/end/duration/script/action
   - material: type/start/end/duration/script/material_keyword/material_search_keyword/material_desc
3. 开头和结尾必须是 digital_human
4. 数字全部使用整数
5. 只返回合法 JSON，不要解释

原始 JSON：
{json.dumps(raw_payload, ensure_ascii=False, indent=2)}
"""
    data, repair_usage = _request_json_from_openai(
        repair_prompt,
        max_tokens=max_tokens,
        target_market=target_market,
        department_id=department_id,
    )
    if not _has_expected_script_shape(data):
        raise ValueError("OpenAI schema repair 后仍未返回符合要求的脚本结构")
    return data, repair_usage


def _request_json_from_openai_chat(
    *,
    api_key: str,
    model_name: str,
    user_prompt: str,
    max_tokens: int,
    enable_web_search: bool,
    target_market: str,
    department_id: str,
) -> tuple[dict, dict]:
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT + _build_context_guidance(target_market, department_id)},
            {"role": "user", "content": user_prompt},
        ],
        "max_completion_tokens": max_tokens,
    }
    if enable_web_search:
        payload["model"] = "gpt-5-search-api"
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    if response.status_code >= 400:
        raise requests.HTTPError(response.text[:500], response=response)
    body = response.json()
    raw = _extract_openai_text(body)
    data, repair_usage = _parse_json_response(raw)
    usage = _merge_usage(_extract_usage_from_openai_payload(body), repair_usage)
    return data, usage


def _request_json_from_openai_responses(
    *,
    api_key: str,
    model_name: str,
    user_prompt: str,
    max_tokens: int,
    enable_web_search: bool,
    target_market: str,
    department_id: str,
) -> tuple[dict, dict]:
    payload = {
        "model": model_name,
        "input": user_prompt,
        "instructions": SYSTEM_PROMPT + _build_context_guidance(target_market, department_id),
        "max_output_tokens": max_tokens,
        "reasoning": {"effort": "minimal"},
        "text": {
            "format": {
                "type": "json_object"
            }
        },
    }
    if enable_web_search:
        payload["tools"] = [{"type": "web_search"}]
        payload["tool_choice"] = "auto"
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    if response.status_code >= 400:
        raise requests.HTTPError(response.text[:500], response=response)
    body = response.json()
    raw = _extract_openai_responses_text(body)
    data, repair_usage = _parse_json_response(raw)
    usage = _merge_usage(_extract_usage_from_openai_responses_payload(body), repair_usage)
    return data, usage


def _request_json_from_openai(user_prompt: str, max_tokens: int, enable_web_search: bool = False, target_market: str = "cn", department_id: str = "real_estate") -> tuple[dict, dict]:
    api_key = _get_openai_api_key()
    if not api_key:
        raise OpenAIFallbackUnavailableError("未配置 OPENAI_API_KEY")

    models_to_try = []
    primary_model = _get_openai_fallback_model()
    compat_model = _get_openai_compat_fallback_model()
    for model_name in [primary_model, compat_model]:
        if model_name and model_name not in models_to_try:
            models_to_try.append(model_name)
    attempts = max(1, int(os.getenv("OPENAI_RETRY_ATTEMPTS", "3")))
    base_delay = max(1.0, float(os.getenv("OPENAI_RETRY_BASE_DELAY_SECONDS", "2")))
    last_error = None
    for model_name in models_to_try:
        for attempt in range(1, attempts + 1):
            try:
                if model_name.lower().startswith("gpt-5"):
                    data, usage = _request_json_from_openai_responses(
                        api_key=api_key,
                        model_name=model_name,
                        user_prompt=user_prompt,
                        max_tokens=max_tokens,
                        enable_web_search=enable_web_search,
                        target_market=target_market,
                        department_id=department_id,
                    )
                else:
                    data, usage = _request_json_from_openai_chat(
                        api_key=api_key,
                        model_name=model_name,
                        user_prompt=user_prompt,
                        max_tokens=max_tokens,
                        enable_web_search=enable_web_search,
                        target_market=target_market,
                        department_id=department_id,
                    )
                if "生成视频文案" in user_prompt and not _has_expected_script_shape(data):
                    data, schema_usage = _repair_schema_with_openai(data, max_tokens=max_tokens, target_market=target_market, department_id=department_id)
                    usage = _merge_usage(usage, schema_usage)
                return data, usage
            except Exception as exc:
                last_error = exc
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                error_text = str(exc).lower()
                should_try_next_model = (
                    "openai 未返回可解析的文本内容" in str(exc)
                    or "responses api 未返回可解析的文本内容" in error_text
                    or "finish_reason" in error_text
                )
                if should_try_next_model:
                    break
                if status_code not in {408, 409, 429, 500, 502, 503, 504} and "timeout" not in error_text:
                    break
                if attempt >= attempts:
                    break
                time.sleep(base_delay * attempt)
    if last_error:
        raise last_error
    raise ValueError("OpenAI fallback 请求失败")


def _request_json_from_glm(user_prompt: str, max_tokens: int, enable_web_search: bool = False, target_market: str = "cn", department_id: str = "real_estate") -> tuple[dict, dict]:
    api_key = _get_glm_api_key()
    if not api_key:
        raise ValueError("未配置 ZHIPUAI_API_KEY")

    model_name = (os.getenv("GLM_MODEL") or "glm-5.1").strip() or "glm-5.1"
    effective_prompt = user_prompt
    usage_total: dict = {}

    if enable_web_search:
        search_payload = {
            "model": (os.getenv("GLM_WEB_SEARCH_MODEL") or "glm-4-air").strip() or "glm-4-air",
            "messages": [
                {
                    "role": "user",
                    "content": f"请围绕这个任务先做联网搜索，并输出一份供后续写作模型使用的事实摘要：{user_prompt}",
                }
            ],
            "max_tokens": 2200,
            "temperature": 0.3,
            "tools": [{
                "type": "web_search",
                "web_search": {
                    "enable": "True",
                    "search_engine": "search_pro",
                    "search_result": "True",
                    "search_prompt": "你是一位研究助手。请基于联网搜索{search_result}整理一份事实摘要，优先保留最新、权威、可验证的信息，并尽量附上来源日期。输出纯文本摘要，不要 JSON。",
                    "count": "6",
                    "search_recency_filter": "noLimit",
                    "content_size": "high",
                },
            }],
            "tool_choice": "auto",
        }
        search_response = requests.post(
            "https://open.bigmodel.cn/api/paas/v4/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=search_payload,
            timeout=180,
        )
        if search_response.status_code >= 400:
            raise requests.HTTPError(search_response.text[:500], response=search_response)
        search_body = search_response.json()
        search_raw = _extract_openai_text(search_body)
        usage_total = _merge_usage(usage_total, _extract_usage_from_glm_payload(search_body))
        effective_prompt = f"""{user_prompt}

下面是智谱联网搜索得到的实时资料摘要，请优先基于这些最新资料生成最终结果；如果摘要与常识冲突，以联网摘要为准，但仍需保持谨慎表述：

{search_raw}
"""

    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT + _build_context_guidance(target_market, department_id)},
            {"role": "user", "content": effective_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7,
    }
    response = requests.post(
        "https://open.bigmodel.cn/api/paas/v4/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    if response.status_code >= 400:
        raise requests.HTTPError(response.text[:500], response=response)
    body = response.json()
    raw = _extract_openai_text(body)
    data, repair_usage = _parse_json_response(raw)
    usage = _merge_usage(usage_total, _merge_usage(_extract_usage_from_glm_payload(body), repair_usage))
    if "生成视频文案" in user_prompt and not _has_expected_script_shape(data):
        data, schema_usage = _repair_schema_with_openai(data, max_tokens=max_tokens, target_market=target_market, department_id=department_id)
        usage = _merge_usage(usage, schema_usage)
    return data, usage



REPAIR_SYSTEM_PROMPT = """你是一个 JSON 修复助手。你的唯一任务是把用户给出的内容整理成合法 JSON。
不要补充解释，不要输出 markdown，只返回修复后的 JSON 本体。
所有字符串中的双引号都必须正确转义，确保输出可以被标准 json.loads 直接解析。
如果字符串内容里需要出现引号，请优先改成中文引号「」或『』，不要保留未转义的半角双引号。"""


def _extract_json_candidate(raw: str) -> str:
    text = _extract_json_text(raw)
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        return text[start:end + 1].strip()
    return text.strip()


def _attempt_simple_json_repairs(raw: str) -> dict | None:
    candidates = []
    candidate = raw.strip()
    if candidate:
        candidates.append(candidate)

    repaired = candidate
    repaired = repaired.replace('“', '"').replace('”', '"').replace('‘', "'").replace('’', "'")
    repaired = re.sub(r',(?=\s*[}\]])', '', repaired)
    repaired = ''.join(ch if (ord(ch) >= 32 or ch in '\n\r\t') else ' ' for ch in repaired)
    if repaired and repaired not in candidates:
        candidates.append(repaired)

    for item in candidates:
        try:
            return json.loads(item)
        except json.JSONDecodeError:
            continue
    return None


def _repair_json_with_claude(raw: str) -> tuple[dict, dict]:
    current = raw
    last_error = None
    usage_total = {}
    for attempt in range(3):
        repair_prompt = f"""
下面这段内容本来应该是一个 JSON，但当前格式有问题，无法被 json.loads 解析。
请你在不改变原始语义的前提下，把它修复成合法 JSON。
只返回 JSON 本体。

额外要求：
1. 所有字符串都必须符合 JSON 标准。
2. 字符串内部如果包含双引号，必须使用反斜杠转义。
3. 不要输出解释，不要输出代码块，不要输出省略号。
4. 让结果可以直接被 Python 的 json.loads 成功解析。

这是第 {attempt + 1} 次修复。
原始内容：
{current}
"""
        message = _create_message_with_retry(
            model='claude-sonnet-4-6',
            max_tokens=2600,
            messages=[{'role': 'user', 'content': repair_prompt}],
            system=REPAIR_SYSTEM_PROMPT,
        )
        usage_total = _merge_usage(usage_total, _extract_usage_from_message(message))
        repaired_text = _extract_json_candidate(_extract_message_text(message))
        try:
            return json.loads(repaired_text), usage_total
        except json.JSONDecodeError as exc:
            current = repaired_text
            last_error = exc
    raise ValueError(f'Claude 联网返回内容无法修复为合法 JSON: {last_error}')


def _parse_json_response(raw: str) -> tuple[dict, dict]:
    candidate = _extract_json_candidate(raw)
    try:
        return json.loads(candidate), {}
    except json.JSONDecodeError:
        repaired = _attempt_simple_json_repairs(candidate)
        if repaired is not None:
            return repaired, {}
        return _repair_json_with_claude(candidate)


def _request_json_from_claude(user_prompt: str, max_tokens: int, enable_web_search: bool, target_market: str = "cn", department_id: str = "real_estate") -> tuple[dict, dict]:
    prompts = [
        user_prompt,
        user_prompt + """

重要补充：
1. 你必须只返回合法 JSON，本次不要输出任何解释文字。
2. JSON 字符串内部不要出现未转义的半角双引号。
3. 如果内容里必须提到引号，请改用中文引号「」或『』。
4. 不要输出 markdown、代码块、注释或省略号。
""",
    ]
    last_error = None
    total_usage = {}
    for prompt in prompts:
        try:
            message = _create_message_with_retry(**_build_message_kwargs(prompt, max_tokens=max_tokens, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id))
            total_usage = _merge_usage(total_usage, _extract_usage_from_message(message))
            raw = _extract_message_text(message)
        except UpstreamBusyError as exc:
            if _get_openai_api_key():
                print(f"⚠️ Claude 当前过载，切换到 OpenAI {_get_openai_fallback_model()} 继续生成")
                return _request_json_from_openai(prompt, max_tokens=max_tokens, target_market=target_market, department_id=department_id)
            last_error = exc
            continue
        try:
            data, repair_usage = _parse_json_response(raw)
            return data, _merge_usage(total_usage, repair_usage)
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    raise ValueError('Claude 未返回可解析的 JSON 内容')


def _normalize_script_model_provider(provider: str | None) -> str:
    requested = str(provider or "").strip().lower()
    if requested in {SCRIPT_MODEL_CLAUDE, SCRIPT_MODEL_GLM, SCRIPT_MODEL_CHATGPT}:
        return requested
    return SCRIPT_MODEL_CLAUDE


def _request_json_by_provider(
    provider: str,
    user_prompt: str,
    *,
    max_tokens: int,
    enable_web_search: bool,
    target_market: str = "cn",
    department_id: str = "real_estate",
) -> tuple[dict, dict]:
    normalized = _normalize_script_model_provider(provider)
    if normalized == SCRIPT_MODEL_GLM:
        return _request_json_from_glm(user_prompt, max_tokens=max_tokens, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
    if normalized == SCRIPT_MODEL_CHATGPT:
        return _request_json_from_openai(user_prompt, max_tokens=max_tokens, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
    return _request_json_from_claude(user_prompt, max_tokens=max_tokens, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)

def _build_message_kwargs(user_prompt: str, max_tokens: int, enable_web_search: bool, target_market: str = "cn", department_id: str = "real_estate") -> dict:
    kwargs = {
        'model': 'claude-sonnet-4-6',
        'max_tokens': max_tokens,
        'messages': [{'role': 'user', 'content': user_prompt}],
        'system': SYSTEM_PROMPT + _build_context_guidance(target_market, department_id) + (WEB_SEARCH_GUIDANCE if enable_web_search else ''),
    }
    if enable_web_search:
        max_uses = max(1, int(os.getenv('ANTHROPIC_WEB_SEARCH_MAX_USES', '4')))
        kwargs['tools'] = [
            {
                'type': 'web_search_20250305',
                'name': 'web_search',
                'max_uses': max_uses,
            }
        ]
    return kwargs


def revise_script_segment(topic: str, script_data: dict, segment_index: int, instruction: str, enable_web_search: bool = False, target_market: str = "cn", department_id: str = "real_estate", provider: str = SCRIPT_MODEL_CLAUDE) -> dict:
    target = script_data.get('segments', [])[segment_index]
    segment_type = target.get('type', 'material')
    prompt = f"""
你要修改的是一条短视频脚本中的第 {segment_index + 1} 段。

选题：{topic}
整条脚本：
{json.dumps(script_data, ensure_ascii=False, indent=2)}

当前目标段：
{json.dumps(target, ensure_ascii=False, indent=2)}

用户修改要求：
{instruction}

请只返回这个段落修改后的 JSON 对象，不要返回数组，不要返回 markdown。
必须保留这些字段不变：type、start、end、duration。
如果是 digital_human，只返回字段：type/start/end/duration/script/action。
如果是 material，只返回字段：type/start/end/duration/script/material_keyword/material_search_keyword/material_desc。
要继续遵守原有规则：
- digital_human 的 action 只能是坐在台前可完成的动作与表情
- 不允许凭空道具、场景、背景元素
- material_desc 只描述素材画面本身
- material_keyword 要适合运营阅读并符合目标市场语言
- material_search_keyword 必须是英文且适合后续找素材
"""

    revised, usage = _request_json_by_provider(provider, prompt, max_tokens=1600, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
    if target_market == "cn" and _find_cn_marketing_hits(revised):
        safe_revised, safe_usage = _rewrite_segment_for_cn_safety(topic, script_data, segment_index, revised, enable_web_search, target_market, department_id, provider=provider)
        revised = safe_revised
        usage = _merge_usage(usage, safe_usage)
    for key in ('type', 'start', 'end', 'duration'):
        revised[key] = target.get(key)

    if segment_type == 'digital_human':
        return {
            'type': target.get('type'),
            'start': target.get('start'),
            'end': target.get('end'),
            'duration': target.get('duration'),
            'script': revised.get('script', target.get('script', '')),
            'action': revised.get('action', target.get('action', '')),
            '_meta': {'usage': usage},
        }

    return {
        'type': target.get('type'),
        'start': target.get('start'),
        'end': target.get('end'),
        'duration': target.get('duration'),
        'script': revised.get('script', target.get('script', '')),
        'material_keyword': revised.get('material_keyword', target.get('material_keyword', '')),
        'material_search_keyword': revised.get('material_search_keyword', target.get('material_search_keyword', '')),
        'material_desc': revised.get('material_desc', target.get('material_desc', '')),
        '_meta': {'usage': usage},
    }


def generate_script(topic: str, enable_web_search: bool = False, target_market: str = "cn", department_id: str = "real_estate", provider: str = SCRIPT_MODEL_CLAUDE) -> dict:
    """
    输入选题，生成完整视频文案
    """
    print(f"📝 正在生成文案：{topic}")
    print(f"🌏 目标市场：{target_market}｜部门：{department_id}")
    print(f"🤖 文案模型：{_normalize_script_model_provider(provider)}")
    if enable_web_search:
        print(f"🌐 已启用实时联网检索：{_normalize_script_model_provider(provider)}")

    prompt = f"请为以下选题生成视频文案：{topic}"
    data, usage = _request_json_by_provider(provider, prompt, max_tokens=4200, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
    if target_market == "cn" and _find_cn_marketing_hits(data):
        print("🛡️ 命中中国市场营销风险词，正在自动改写为小红书安全模式")
        safe_data, safe_usage = _rewrite_script_for_cn_safety(topic, data, enable_web_search, target_market, department_id, provider=provider)
        data = safe_data
        usage = _merge_usage(usage, safe_usage)
    data = _rebalance_segment_mix(data, target_market, department_id)
    data['_meta'] = {'usage': usage}
    print(f"✅ 文案生成完成，共 {len(data['segments'])} 段，总时长 {data['total_duration']} 秒")
    return data


if __name__ == '__main__':
    result = generate_script('为什么日本的房子是永久产权')
    print(json.dumps(result, ensure_ascii=False, indent=2))
