"""
文案生成模块
输入选题 → 输出完整播报稿+时间轴
"""

import json
import os
import re
from typing import Any

import anthropic
from dotenv import load_dotenv

load_dotenv(override=False)

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

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
3. 数字人和素材段落交替出现
4. 开头和结尾必须是数字人段落
5. social_post 只输出一份，必须面向当前目标市场，语气适合社交媒体发布
6. 所有数字必须是整数
7. digital_human 的 action 只能描述主播坐在台前即可完成的动作与表情，例如点头、微笑、自然眨眼、轻微摆头、表情认真、语气坚定等
8. digital_human 的 action 严禁出现任何凭空道具、场景、背景元素或夸张肢体动作，例如不能写手持计算器、指向图表、站起身、走动、在街头、在客厅等
9. material_desc 只描述素材画面本身应该出现什么内容，不要描述数字人主播，也不要写镜头外的设定
10. material_keyword 要跟随目标市场语言输出，给运营直接阅读
11. material_search_keyword 必须使用简洁准确的英文关键词，专门给素材库检索使用"""

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


def _build_context_guidance(target_market: str, department_id: str) -> str:
    market = TARGET_MARKET_RULES.get(target_market, TARGET_MARKET_RULES["cn"])
    department_rule = DEPARTMENT_RULES.get(department_id, "内容角度保持通用商业资讯表达。")
    return f"""

当前内容目标市场：{market['name']}
语言要求：{market['language_label']}
市场输出规则：{market['content_rules']}
当前业务部门：{department_id}
部门表达要求：{department_rule}
"""

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


def _extract_message_text(message: Any) -> str:
    parts = []
    for block in getattr(message, 'content', []) or []:
        if getattr(block, 'type', '') == 'text' and getattr(block, 'text', ''):
            parts.append(block.text)
    text = "\n".join(part.strip() for part in parts if part and part.strip()).strip()
    if not text:
        raise ValueError('Claude 未返回可解析的文本内容')
    return text



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
        message = client.messages.create(
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
        message = client.messages.create(**_build_message_kwargs(prompt, max_tokens=max_tokens, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id))
        total_usage = _merge_usage(total_usage, _extract_usage_from_message(message))
        raw = _extract_message_text(message)
        try:
            data, repair_usage = _parse_json_response(raw)
            return data, _merge_usage(total_usage, repair_usage)
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    raise ValueError('Claude 未返回可解析的 JSON 内容')

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


def revise_script_segment(topic: str, script_data: dict, segment_index: int, instruction: str, enable_web_search: bool = False, target_market: str = "cn", department_id: str = "real_estate") -> dict:
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

    revised, usage = _request_json_from_claude(prompt, max_tokens=1600, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
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


def generate_script(topic: str, enable_web_search: bool = False, target_market: str = "cn", department_id: str = "real_estate") -> dict:
    """
    输入选题，生成完整视频文案
    """
    print(f"📝 正在生成文案：{topic}")
    print(f"🌏 目标市场：{target_market}｜部门：{department_id}")
    if enable_web_search:
        print('🌐 已启用 Claude 实时联网检索')

    prompt = f"请为以下选题生成视频文案：{topic}"
    data, usage = _request_json_from_claude(prompt, max_tokens=4200, enable_web_search=enable_web_search, target_market=target_market, department_id=department_id)
    data['_meta'] = {'usage': usage}
    print(f"✅ 文案生成完成，共 {len(data['segments'])} 段，总时长 {data['total_duration']} 秒")
    return data


if __name__ == '__main__':
    result = generate_script('为什么日本的房子是永久产权')
    print(json.dumps(result, ensure_ascii=False, indent=2))
