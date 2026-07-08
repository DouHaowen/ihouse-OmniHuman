"""给 InfiniteTalk API 服务端加"短音频补静音"，根治 <0.75s 片段触发
'length not satisfies frame nums' 断言、被上游无限重试的问题。幂等。"""
import re
import shutil
import sys
from pathlib import Path

SRV = Path("/home/saita/InfiniteTalk/infinitetalk_avatar_api_server.py")
src = SRV.read_text(encoding="utf-8")

if "_pad_audio_to_min_duration" in src:
    print("已打过补丁，跳过。")
    sys.exit(0)

HELPER = '''MIN_AUDIO_SECONDS = float(os.getenv("INFINITETALK_MIN_AUDIO_SECONDS", "2.0"))


def _pad_audio_to_min_duration(wav_path: Path, min_seconds: float = MIN_AUDIO_SECONDS) -> None:
    """过短音频（<~0.75s）会让 InfiniteTalk 的音频嵌入帧数不足 frame_num，
    触发 'length not satisfies frame nums' 断言并被上游无限重试。
    这里在末尾补静音，保证音频不短于 min_seconds，短片段也能正常生成。"""
    if min_seconds <= 0:
        return
    duration = _probe_media_duration_seconds(wav_path) or 0.0
    if duration <= 0 or duration >= min_seconds:
        return
    padded_path = wav_path.with_name(wav_path.stem + "_padded.wav")
    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", str(wav_path),
                "-af", "apad",
                "-t", "%.3f" % min_seconds,
                "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le",
                str(padded_path),
            ],
            capture_output=True, check=True, timeout=120,
        )
    except Exception as exc:
        print("[infinitetalk] audio pad failed (keep original): %r" % exc, flush=True)
        return
    os.replace(padded_path, wav_path)


def _normalize_audio_for_infinitetalk(source_path: Path) -> Path:'''

# 1) 在 _normalize_audio_for_infinitetalk 定义前插入 helper + 常量
anchor = "def _normalize_audio_for_infinitetalk(source_path: Path) -> Path:"
assert src.count(anchor) == 1, "找不到唯一的归一化函数定义"
src = src.replace(anchor, HELPER, 1)

# 2) 早返回分支(源已是 audio_norm.wav)也补齐
src = src.replace(
    "    if source_path.suffix.lower() == \".wav\" and source_path.name == target_path.name:\n"
    "        return source_path\n",
    "    if source_path.suffix.lower() == \".wav\" and source_path.name == target_path.name:\n"
    "        _pad_audio_to_min_duration(source_path)\n"
    "        return source_path\n",
    1,
)

# 3) 归一化末尾 return target_path 前补齐
#    该函数体内唯一的 "    return target_path"
assert src.count("\n    return target_path\n") == 1, "return target_path 不唯一，需人工确认"
src = src.replace(
    "\n    return target_path\n",
    "\n    _pad_audio_to_min_duration(target_path)\n    return target_path\n",
    1,
)

shutil.copy2(SRV, str(SRV) + ".bak_audiopad")
SRV.write_text(src, encoding="utf-8")
print("补丁完成。备份：%s.bak_audiopad" % SRV)

# 语法自检
import py_compile
py_compile.compile(str(SRV), doraise=True)
print("py_compile OK")
