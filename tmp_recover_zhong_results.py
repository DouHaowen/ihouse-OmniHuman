import json
from pathlib import Path


ROOT = Path("/app/output")
TARGETS = [
    ROOT / "1776135094_full_什么是外泌体，它对人体有什么作用，可以用",
    ROOT / "1776135910_full_什么是外泌体，它对人体有什么作用，可以用",
    ROOT / "1776135839_full_日本是怎么看待中国的",
]


def build_result(script: dict, owner_username: str = "zhong") -> dict:
    title = script.get("title") or script.get("topic") or ""
    result = dict(script)
    result.update(
        {
            "topic": title,
            "title": title,
            "cover_title": script.get("cover_title") or title,
            "owner_username": owner_username,
            "owner_display_name": owner_username,
            "owner_role": "user",
            "workflow_config": {
                "target_market": "cn",
                "department_id": "real_estate",
                "web_search_enabled": False,
                "voice_preset": {
                    "id": "mandarin_female",
                    "selected_speed": 1.1,
                },
                "avatar": {
                    "id": "avatar_test_0cd3d70a.png",
                },
            },
        }
    )
    return result


def main() -> None:
    for target in TARGETS:
        script_path = target / "script.json"
        result_path = target / "result.json"
        if not script_path.exists():
            print(f"missing script: {script_path}")
            continue
        script = json.loads(script_path.read_text(encoding="utf-8"))
        result = build_result(script)
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"wrote {result_path}")


if __name__ == "__main__":
    main()
