import math
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

try:
    from faster_whisper import WhisperModel
except Exception:
    WhisperModel = None

WIDTH = 1080
HEIGHT = 1920
FPS = 25
VIDEO_CODEC = "libx264"
AUDIO_CODEC = "aac"
PRESET = "veryfast"
PIX_FMT = "yuv420p"
SUBTITLE_FONT = "Noto Sans CJK SC"
COVER_TITLE_FONT = "Noto Sans CJK SC"
COVER_TITLE_DURATION = 1.0 / FPS  # exactly 1 frame — cover visible as first frame, no pause

SUBTITLE_TEMPLATE_STYLES = {
    "classic": {
        "font": SUBTITLE_FONT,
        "size": 13,
        "primary": "&H00FFFFFF",
        "outline": "&H003C2A18",
        "back": "&H55000000",
        "border_style": 3,
        "outline_width": 1.6,
        "shadow": 0,
        "alignment": 2,
        "margin_v": 46,
        "margin_l": 68,
        "margin_r": 68,
    },
    "minimal": {
        "font": SUBTITLE_FONT,
        "size": 12,
        "primary": "&H00FAFAFA",
        "outline": "&H00342A22",
        "back": "&H22000000",
        "border_style": 1,
        "outline_width": 1.2,
        "shadow": 0.4,
        "alignment": 2,
        "margin_v": 40,
        "margin_l": 74,
        "margin_r": 74,
    },
    "bold": {
        "font": SUBTITLE_FONT,
        "size": 15,
        "primary": "&H00FFF7E8",
        "outline": "&H002A1808",
        "back": "&H660A3D3A",
        "border_style": 3,
        "outline_width": 2.0,
        "shadow": 0,
        "alignment": 2,
        "margin_v": 52,
        "margin_l": 72,
        "margin_r": 72,
    },
}

WHISPER_LANGUAGE_MAP = {
    "cn": "zh",
    "tw": "zh",
    "jp": "ja",
}


def _run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "ffmpeg failed").strip())


def _seconds(value) -> float:
    try:
        return max(0.1, float(value or 0))
    except Exception:
        return 0.1


def _format_srt_timestamp(seconds: float) -> str:
    total_ms = int(round(max(0.0, seconds) * 1000))
    hours = total_ms // 3600000
    minutes = (total_ms % 3600000) // 60000
    secs = (total_ms % 60000) // 1000
    millis = total_ms % 1000
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def _video_filter() -> str:
    return (
        f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,"
        f"pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=white,setsar=1"
    )


def _material_motion_filter(duration: float) -> str:
    drift_x = max(16, int((WIDTH * 0.12) // 2))
    drift_y = max(20, int((HEIGHT * 0.08) // 2))
    return (
        f"scale={int(WIDTH * 1.18)}:{int(HEIGHT * 1.18)}:force_original_aspect_ratio=increase,"
        f"crop={WIDTH}:{HEIGHT}:"
        f"x='(in_w-out_w)/2+{drift_x}*sin(t/({max(duration, 0.1):.3f}/2+0.35))':"
        f"y='(in_h-out_h)/2+{drift_y}*cos(t/({max(duration, 0.1):.3f}/2+0.55))',"
        f"setsar=1"
    )


def _subtitle_filter(subtitle_path: Path, template_id: str) -> str:
    escaped = subtitle_path.as_posix().replace('\\', '/').replace(':', r'\:').replace("'", r"\'")
    style_config = SUBTITLE_TEMPLATE_STYLES.get(template_id) or SUBTITLE_TEMPLATE_STYLES["classic"]
    style = (
        f"FontName={style_config['font']},"
        f"FontSize={style_config['size']},"
        f"PrimaryColour={style_config['primary']},"
        f"OutlineColour={style_config['outline']},"
        f"BackColour={style_config['back']},"
        f"BorderStyle={style_config['border_style']},"
        f"Outline={style_config['outline_width']},"
        f"Shadow={style_config['shadow']},"
        f"Alignment={style_config['alignment']},"
        f"MarginV={style_config['margin_v']},"
        f"MarginL={style_config['margin_l']},"
        f"MarginR={style_config['margin_r']}"
    )
    return f"subtitles='{escaped}':force_style='{style}'"


def _cover_title_drawtext(title: str) -> str:
    """Return a drawtext filter string that overlays the title on only the first frame."""
    title = (title or "").strip()
    if not title:
        return ""
    escaped_title = title.replace("'", "'\\''").replace(":", "\\:")
    font_path = ""
    fc_result = subprocess.run(
        ["fc-match", COVER_TITLE_FONT, "--format=%{file}"],
        capture_output=True, text=True,
    )
    if fc_result.returncode == 0 and fc_result.stdout.strip():
        font_path = fc_result.stdout.strip()
    fontfile_opt = f"fontfile={font_path}\\:" if font_path else ""
    frame_duration = 1.0 / FPS
    return (
        f"drawbox=x=0:y=ih*0.55:w=iw:h=ih*0.45:color=black@0.45:t=fill:"
        f"enable='lte(t,{frame_duration:.4f})',"
        f"drawtext={fontfile_opt}"
        f"text='{escaped_title}':"
        f"fontsize=52:"
        f"fontcolor=white:"
        f"borderw=3:bordercolor=black@0.6:"
        f"x=(w-text_w)/2:"
        f"y=h*0.68:"
        f"line_spacing=16:"
        f"enable='lte(t,{frame_duration:.4f})'"
    )


def _make_cover_image(video_path: Path, title: str, output_path: Path) -> None:
    """Extract first frame from video and overlay title text to create cover image."""
    # Extract raw first frame
    raw_frame = output_path.with_suffix(".raw.jpg")
    _run([
        "ffmpeg", "-y",
        "-i", str(video_path),
        "-frames:v", "1",
        "-q:v", "2",
        str(raw_frame),
    ])
    if not raw_frame.exists():
        return

    # Overlay: semi-transparent dark gradient at bottom + title text
    # Wrap long titles: split into lines of ~10 chars for vertical impact
    title = (title or "").strip()
    if not title:
        shutil.copy2(raw_frame, output_path)
        raw_frame.unlink(missing_ok=True)
        return

    escaped_title = title.replace("'", "'\\''").replace(":", "\\:")
    font_path = ""
    # Try to find the font file path
    fc_result = subprocess.run(
        ["fc-match", COVER_TITLE_FONT, "--format=%{file}"],
        capture_output=True, text=True,
    )
    if fc_result.returncode == 0 and fc_result.stdout.strip():
        font_path = fc_result.stdout.strip()

    fontfile_opt = f"fontfile={font_path}:" if font_path else ""

    # Dark gradient overlay at bottom half + centered title text
    vf = (
        # Dark gradient overlay on the lower portion
        f"drawbox=x=0:y=ih*0.55:w=iw:h=ih*0.45:color=black@0.45:t=fill,"
        # Main title text — large, bold, centered
        f"drawtext={fontfile_opt}"
        f"text='{escaped_title}':"
        f"fontsize=52:"
        f"fontcolor=white:"
        f"borderw=3:bordercolor=black@0.6:"
        f"x=(w-text_w)/2:"
        f"y=h*0.68:"
        f"line_spacing=16"
    )
    _run([
        "ffmpeg", "-y",
        "-i", str(raw_frame),
        "-vf", vf,
        "-q:v", "2",
        str(output_path),
    ])
    raw_frame.unlink(missing_ok=True)


def _make_title_card_video(cover_image: Path, duration: float, output_path: Path) -> None:
    """Create a silent video clip from the cover image (title card)."""
    _run([
        "ffmpeg", "-y",
        "-loop", "1",
        "-i", str(cover_image),
        "-t", f"{duration:.3f}",
        "-vf", f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,"
               f"pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1",
        "-r", str(FPS),
        "-c:v", VIDEO_CODEC,
        "-preset", PRESET,
        "-pix_fmt", PIX_FMT,
        str(output_path),
    ])


def _offset_srt(input_path: Path, offset_sec: float, output_path: Path) -> None:
    """Shift all SRT timestamps forward by offset_sec seconds."""
    content = input_path.read_text(encoding="utf-8")
    lines = content.split("\n")
    new_lines = []
    ts_re = re.compile(r"(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})")
    for line in lines:
        m = ts_re.match(line.strip())
        if m:
            start = _parse_srt_timestamp(m.group(1)) + offset_sec
            end = _parse_srt_timestamp(m.group(2)) + offset_sec
            new_lines.append(f"{_format_srt_timestamp(start)} --> {_format_srt_timestamp(end)}")
        else:
            new_lines.append(line)
    output_path.write_text("\n".join(new_lines), encoding="utf-8")


def _parse_srt_timestamp(ts: str) -> float:
    """Parse SRT timestamp '00:01:23,456' to seconds."""
    parts = ts.replace(",", ":").split(":")
    h, m, s, ms = int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3])
    return h * 3600 + m * 60 + s + ms / 1000.0


def _make_silent_segment(output_path: Path, duration: float) -> None:
    _run([
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=c=0xf6f2ec:s={WIDTH}x{HEIGHT}:d={duration}",
        "-r", str(FPS),
        "-c:v", VIDEO_CODEC,
        "-preset", PRESET,
        "-pix_fmt", PIX_FMT,
        str(output_path),
    ])


def _build_material_segment(images: list[str], duration: float, output_path: Path, transition_id: str = "fade") -> None:
    valid_images = [str(Path(p)) for p in images if p and Path(p).exists()]
    if not valid_images:
        _make_silent_segment(output_path, duration)
        return

    per_image = max(duration / len(valid_images), 0.3)
    cmd = ["ffmpeg", "-y"]
    filter_parts = []
    concat_inputs = []
    for idx, image in enumerate(valid_images):
        cmd += ["-loop", "1", "-t", f"{per_image:.3f}", "-i", image]
        motion = _material_motion_filter(per_image)
        transition_filter = ""
        if transition_id == "fade" and per_image > 0.9:
            fade_out_start = max(per_image - 0.45, 0.15)
            transition_filter = f",fade=t=in:st=0:d=0.28,fade=t=out:st={fade_out_start:.3f}:d=0.32"
        filter_parts.append(
            f"[{idx}:v]{motion}{transition_filter},trim=duration={per_image:.3f},setpts=PTS-STARTPTS[v{idx}]"
        )
        concat_inputs.append(f"[v{idx}]")
    filter_parts.append(f"{''.join(concat_inputs)}concat=n={len(valid_images)}:v=1:a=0[vout]")
    cmd += [
        "-filter_complex", ";".join(filter_parts),
        "-map", "[vout]",
        "-r", str(FPS),
        "-c:v", VIDEO_CODEC,
        "-preset", PRESET,
        "-pix_fmt", PIX_FMT,
        str(output_path),
    ]
    _run(cmd)


def _prepare_video_segment(video_path: str, duration: float, output_path: Path) -> None:
    if not video_path or not Path(video_path).exists():
        _make_silent_segment(output_path, duration)
        return
    _run([
        "ffmpeg", "-y",
        "-i", str(video_path),
        "-t", f"{duration:.3f}",
        "-vf", _video_filter(),
        "-an",
        "-r", str(FPS),
        "-c:v", VIDEO_CODEC,
        "-preset", PRESET,
        "-pix_fmt", PIX_FMT,
        str(output_path),
    ])


def _prepare_dh_segment_with_audio(video_path: str, output_path: Path) -> None:
    """Prepare a digital-human segment keeping its own audio track.

    Re-encodes video to the standard format while preserving the embedded audio,
    so lip-sync is guaranteed.
    """
    _run([
        "ffmpeg", "-y",
        "-i", str(video_path),
        "-vf", _video_filter(),
        "-r", str(FPS),
        "-c:v", VIDEO_CODEC,
        "-preset", PRESET,
        "-pix_fmt", PIX_FMT,
        "-c:a", AUDIO_CODEC,
        "-b:a", "192k",
        str(output_path),
    ])


def _prepare_material_segment_with_audio(
    images: list[str], audio_path: Path, output_path: Path, transition_id: str = "fade",
) -> None:
    """Build a material segment with its TTS audio muxed in."""
    duration = _get_audio_duration(audio_path)
    if duration <= 0:
        duration = 5.0
    # Build silent video at the exact audio duration
    silent_video = output_path.with_suffix(".silent.mp4")
    _build_material_segment(images, duration, silent_video, transition_id=transition_id)
    # Mux video + audio
    _run([
        "ffmpeg", "-y",
        "-i", str(silent_video),
        "-i", str(audio_path),
        "-c:v", "copy",
        "-c:a", AUDIO_CODEC,
        "-b:a", "192k",
        "-shortest",
        str(output_path),
    ])
    silent_video.unlink(missing_ok=True)


def _concat_media_from_list(file_list: list[Path], output_path: Path, media_type: str) -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt", encoding="utf-8") as fh:
        for path in file_list:
            fh.write(f"file '{path.as_posix()}'\n")
        list_path = Path(fh.name)
    try:
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(list_path)]
        if media_type == "video":
            cmd += ["-c", "copy", str(output_path)]
        else:
            cmd += ["-c:a", AUDIO_CODEC, "-b:a", "192k", str(output_path)]
        _run(cmd)
    finally:
        list_path.unlink(missing_ok=True)


def _split_subtitle_text(script: str) -> list[str]:
    text = re.sub(r"\s+", "", script or "").strip()
    if not text:
        return []
    parts = re.split(r"(?<=[，。！？；：,.!?;:])", text)
    chunks: list[str] = []
    current = ""
    max_len = 16
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if len(part) > max_len + 4:
            if current:
                chunks.append(current)
                current = ""
            start = 0
            while start < len(part):
                chunks.append(part[start:start + max_len])
                start += max_len
            continue
        if not current:
            current = part
        elif len(current) + len(part) <= max_len:
            current += part
        else:
            chunks.append(current)
            current = part
    if current:
        chunks.append(current)
    return chunks or [text]


def _subtitle_chunks_with_timing(text: str, start_sec: float, end_sec: float, max_len: int = 16) -> list[tuple[float, float, str]]:
    chunks = _split_subtitle_text(text)
    if not chunks:
        return []
    total = max(end_sec - start_sec, 0.6)
    weights = [max(1, len(chunk)) for chunk in chunks]
    weight_total = sum(weights) or len(chunks)
    min_duration = 0.7 if len(chunks) > 1 else total
    rows: list[tuple[float, float, str]] = []
    cursor = start_sec
    for idx, chunk in enumerate(chunks):
        ratio = weights[idx] / weight_total
        chunk_duration = total * ratio
        chunk_start = cursor
        chunk_end = cursor + chunk_duration
        if idx == len(chunks) - 1:
            chunk_end = end_sec
        if chunk_end - chunk_start < min_duration:
            chunk_end = min(end_sec, chunk_start + min_duration)
        if chunk_end <= chunk_start:
            chunk_end = chunk_start + 0.5
        rows.append((chunk_start, min(chunk_end, end_sec + 0.01), chunk))
        cursor = chunk_end
    return rows


# ---------------------------------------------------------------------------
# Forced alignment: per-segment word-level timestamps
# ---------------------------------------------------------------------------

_whisper_model_cache: dict = {}


def _get_audio_duration(audio_path: Path) -> float:
    """Get exact audio duration in seconds via ffprobe."""
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
         "-of", "csv=p=0", str(audio_path)],
        capture_output=True, text=True,
    )
    try:
        return float(result.stdout.strip())
    except (ValueError, AttributeError):
        return 0.0


def _get_whisper_model(size: str = "base"):
    """Return a cached WhisperModel instance."""
    if size not in _whisper_model_cache:
        _whisper_model_cache[size] = WhisperModel(size, device="cpu", compute_type="int8")
    return _whisper_model_cache[size]


def _word_timestamps_for_audio(audio_path: Path, language: str) -> list[tuple[float, float, str]]:
    """Run faster_whisper with word_timestamps on a single audio file.

    Returns list of (start, end, word_text) tuples.
    """
    model = _get_whisper_model("base")
    segments_iter, _ = model.transcribe(
        str(audio_path),
        language=language,
        word_timestamps=True,
        vad_filter=False,
        beam_size=3,
    )
    words: list[tuple[float, float, str]] = []
    for seg in segments_iter:
        for w in (seg.words or []):
            txt = (w.word or "").strip()
            if txt and w.end > w.start:
                words.append((w.start, w.end, txt))
    return words


def _map_chunks_to_word_timeline(
    chunks: list[str],
    words: list[tuple[float, float, str]],
    audio_duration: float,
) -> list[tuple[float, float, str]]:
    """Map original-text subtitle chunks to word-level timestamps.

    Strategy: build a character-position → time mapping from word timestamps,
    then look up each chunk's start/end time by its character position range.
    This uses the *acoustic* timing from Whisper but the *text* from the
    original script, so ASR errors don't affect displayed text.
    """
    if not chunks:
        return []
    if not words:
        # No word timestamps available — fall back to proportional split
        total_chars = sum(len(c) for c in chunks)
        rows: list[tuple[float, float, str]] = []
        cursor = 0.0
        for i, chunk in enumerate(chunks):
            ratio = len(chunk) / max(total_chars, 1)
            chunk_start = cursor
            chunk_end = cursor + audio_duration * ratio
            if i == len(chunks) - 1:
                chunk_end = audio_duration
            rows.append((chunk_start, chunk_end, chunk))
            cursor = chunk_end
        return rows

    # Build a list of (char_position, time) anchor points from word timestamps.
    # Each word covers some characters; we spread its time evenly across chars.
    anchors: list[tuple[int, float]] = []  # (cumulative_char_index, timestamp)
    char_pos = 0
    anchors.append((0, words[0][0]))  # speech start
    for w_start, w_end, w_text in words:
        n = max(len(w_text), 1)
        for ci in range(n):
            t = w_start + (w_end - w_start) * ci / n
            anchors.append((char_pos + ci, t))
        char_pos += n
    anchors.append((char_pos, words[-1][1]))  # speech end
    total_word_chars = char_pos

    # Total characters in original script
    total_script_chars = sum(len(c) for c in chunks)
    if total_script_chars == 0:
        return []

    def _time_at_char_pos(pos: int) -> float:
        """Interpolate time for a given character position in the script.

        Maps script char positions to word-timeline char positions proportionally,
        then interpolates between the nearest anchors.
        """
        # Map script position to word-timeline position
        mapped = pos / max(total_script_chars, 1) * total_word_chars
        # Find surrounding anchors
        prev_anchor = anchors[0]
        for anchor in anchors:
            if anchor[0] >= mapped:
                next_anchor = anchor
                break
            prev_anchor = anchor
        else:
            next_anchor = anchors[-1]
        # Interpolate
        span = next_anchor[0] - prev_anchor[0]
        if span <= 0:
            return prev_anchor[1]
        frac = (mapped - prev_anchor[0]) / span
        return prev_anchor[1] + frac * (next_anchor[1] - prev_anchor[1])

    rows = []
    char_cursor = 0
    for i, chunk in enumerate(chunks):
        chunk_start = _time_at_char_pos(char_cursor)
        char_cursor += len(chunk)
        chunk_end = _time_at_char_pos(char_cursor)
        if i == len(chunks) - 1:
            chunk_end = max(chunk_end, words[-1][1])
        # Ensure minimum display duration
        if chunk_end - chunk_start < 0.35:
            chunk_end = chunk_start + 0.35
        rows.append((chunk_start, min(chunk_end, audio_duration + 0.01), chunk))
    return rows


def _write_subtitles_forced_align(segments: list[dict], output_path: Path, target_market: str) -> bool:
    """Per-segment forced alignment using individual audio files.

    For each segment:
      1. Get real audio duration via ffprobe
      2. Run word-level Whisper on that segment's mp3
      3. Map original script chunks to word timestamps
      4. Offset by cumulative duration of prior segments
    """
    if WhisperModel is None:
        return False

    language = WHISPER_LANGUAGE_MAP.get(target_market or "cn", "zh")
    rows: list[str] = []
    subtitle_index = 1
    cumulative_offset = 0.0

    for seg in segments:
        script_text = (seg.get("script") or "").strip()
        # Prefer _align_audio (extracted from DH video or TTS mp3), fall back to audio_path
        audio_path_str = seg.get("_align_audio") or seg.get("audio_path") or ""
        audio_path = Path(audio_path_str) if audio_path_str else None

        if not script_text or not audio_path or not audio_path.exists():
            if audio_path and audio_path.exists():
                cumulative_offset += _get_audio_duration(audio_path)
            continue

        seg_duration = _get_audio_duration(audio_path)
        if seg_duration <= 0:
            continue

        try:
            words = _word_timestamps_for_audio(audio_path, language)
        except Exception:
            words = []

        chunks = _split_subtitle_text(script_text)
        if not chunks:
            cumulative_offset += seg_duration
            continue

        timed_chunks = _map_chunks_to_word_timeline(chunks, words, seg_duration)

        for chunk_start, chunk_end, chunk_text in timed_chunks:
            abs_start = cumulative_offset + chunk_start
            abs_end = cumulative_offset + chunk_end
            rows.extend([
                str(subtitle_index),
                f"{_format_srt_timestamp(abs_start)} --> {_format_srt_timestamp(abs_end)}",
                chunk_text,
                "",
            ])
            subtitle_index += 1

        cumulative_offset += seg_duration

    if subtitle_index == 1:
        return False
    output_path.write_text("\n".join(rows).strip() + "\n", encoding="utf-8")
    return True


def _write_subtitles_from_transcript(audio_path: Path, script_segments: list[dict], output_path: Path, target_market: str) -> bool:
    """Legacy fallback: whole-audio transcript alignment (kept for compatibility)."""
    if WhisperModel is None:
        return False
    try:
        model = _get_whisper_model("base")
        language = WHISPER_LANGUAGE_MAP.get(target_market or "cn", "zh")
        transcript_segments, _ = model.transcribe(str(audio_path), language=language, vad_filter=False, beam_size=1)
        transcript_rows = []
        for seg in transcript_segments:
            start = float(getattr(seg, 'start', 0.0) or 0.0)
            end = float(getattr(seg, 'end', 0.0) or 0.0)
            text = (getattr(seg, 'text', '') or '').strip()
            if text and end > start:
                transcript_rows.append((start, end, text))
        if not transcript_rows:
            return False

        rows = []
        subtitle_index = 1
        for seg in script_segments:
            script_text = (seg.get("script") or "").strip()
            if not script_text:
                continue
            seg_start = _seconds(seg.get("start"))
            seg_end = _seconds(seg.get("end"))
            overlaps = [item for item in transcript_rows if item[1] > seg_start and item[0] < seg_end]
            actual_start = overlaps[0][0] if overlaps else seg_start
            actual_end = overlaps[-1][1] if overlaps else seg_end
            if actual_end <= actual_start:
                actual_start, actual_end = seg_start, max(seg_end, seg_start + 0.6)
            for chunk_start, chunk_end, chunk_text in _subtitle_chunks_with_timing(script_text, actual_start, actual_end):
                rows.extend([str(subtitle_index), f"{_format_srt_timestamp(chunk_start)} --> {_format_srt_timestamp(chunk_end)}", chunk_text, ""])
                subtitle_index += 1
        if subtitle_index == 1:
            return False
        output_path.write_text("\n".join(rows).strip() + "\n", encoding="utf-8")
        return True
    except Exception:
        return False


def _write_subtitles(segments: list[dict], output_path: Path) -> None:
    lines = []
    subtitle_index = 1
    for seg in segments:
        script = (seg.get("script") or "").strip()
        if not script:
            continue
        chunks = _split_subtitle_text(script)
        start_sec = _seconds(seg.get("start"))
        end_sec = _seconds(seg.get("end"))
        total = max(end_sec - start_sec, 0.6)
        chunk_duration = total / max(len(chunks), 1)
        min_duration = 0.9 if len(chunks) > 1 else total
        for idx, chunk in enumerate(chunks):
            chunk_start = start_sec + idx * chunk_duration
            chunk_end = start_sec + (idx + 1) * chunk_duration
            if idx == len(chunks) - 1:
                chunk_end = end_sec
            if chunk_end - chunk_start < min_duration:
                chunk_end = min(end_sec, chunk_start + min_duration)
            if chunk_end <= chunk_start:
                chunk_end = chunk_start + 0.6
            start = _format_srt_timestamp(chunk_start)
            end = _format_srt_timestamp(min(chunk_end, end_sec + 0.01))
            lines.extend([str(subtitle_index), f"{start} --> {end}", chunk, ""])
            subtitle_index += 1
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def compose_history_video(output_dir: str, result: dict, transition_id: str = "fade", subtitle_template_id: str = "classic") -> dict:
    output_root = Path(output_dir)
    if not output_root.exists():
        raise RuntimeError("输出目录不存在")

    work_dir = Path(tempfile.mkdtemp(prefix="ihouse_compose_"))
    build_dir = output_root / "final_edit"
    build_dir.mkdir(parents=True, exist_ok=True)

    try:
        # ── Per-segment: build video (silent) + prepare audio (uniform m4a) ──
        # DH segments → audio extracted from DH video (lip-sync baked in)
        # Material segments → TTS mp3
        # All audio normalized to aac m4a so concat doesn't break.
        segment_video_files: list[Path] = []
        segment_audio_files: list[Path] = []
        subtitle_audio_segments: list[dict] = []

        for idx, seg in enumerate(result.get("segments", []), start=1):
            video_path_str = seg.get("video_path") or ""
            dh_video = Path(video_path_str) if video_path_str else None
            tts_audio_path = seg.get("audio_path") or ""
            tts_audio = Path(tts_audio_path) if tts_audio_path else None
            is_dh = seg.get("type") == "digital_human" and dh_video and dh_video.exists()

            seg_audio = work_dir / f"audio_{idx:02d}.m4a"

            if is_dh:
                # Extract audio from DH video → uniform m4a
                _run([
                    "ffmpeg", "-y", "-i", str(dh_video),
                    "-vn", "-c:a", AUDIO_CODEC, "-b:a", "192k",
                    str(seg_audio),
                ])
            elif tts_audio and tts_audio.exists():
                # Convert TTS mp3 → uniform m4a
                _run([
                    "ffmpeg", "-y", "-i", str(tts_audio),
                    "-c:a", AUDIO_CODEC, "-b:a", "192k",
                    str(seg_audio),
                ])
            else:
                seg_audio = None

            # Get duration from the actual audio we'll use
            if seg_audio and seg_audio.exists():
                duration = _get_audio_duration(seg_audio)
                if duration <= 0:
                    duration = _seconds(seg.get("duration"))
                segment_audio_files.append(seg_audio)
                subtitle_audio_segments.append({**seg, "_align_audio": str(seg_audio)})
            else:
                duration = _seconds(seg.get("duration"))

            # Build silent video at the exact audio duration
            segment_video = work_dir / f"video_{idx:02d}.mp4"
            if is_dh:
                _prepare_video_segment(str(dh_video), duration, segment_video)
            else:
                _build_material_segment(
                    seg.get("material_paths", []) or [], duration, segment_video,
                    transition_id=transition_id,
                )
            segment_video_files.append(segment_video)

        if not segment_video_files:
            raise RuntimeError("没有可用的段落视频")
        if not segment_audio_files:
            raise RuntimeError("没有可用的配音文件")

        merged_video = work_dir / "visual_track.mp4"
        merged_audio = work_dir / "voice_track.m4a"
        subtitle_file = work_dir / "timeline_subtitles.srt"
        final_video = build_dir / "final_video.mp4"
        cover_image = build_dir / "cover.jpg"
        stored_subtitle = build_dir / "timeline_subtitles.srt"
        target_market = ((result.get("workflow_config") or {}).get("target_market") or "cn")

        # ── Cover title text ──
        cover_title = (
            result.get("cover_title")
            or (result.get("script") or {}).get("cover_title")
            or result.get("title")
            or (result.get("script") or {}).get("title")
            or ""
        )

        # ── Concat video track and audio track ──
        _concat_media_from_list(segment_video_files, merged_video, "video")
        _concat_media_from_list(segment_audio_files, merged_audio, "audio")

        # ── Subtitle alignment ──
        used_forced = _write_subtitles_forced_align(subtitle_audio_segments, subtitle_file, target_market)
        if not used_forced:
            used_transcript = _write_subtitles_from_transcript(merged_audio, result.get("segments", []), subtitle_file, target_market)
            if not used_transcript:
                _write_subtitles(result.get("segments", []), subtitle_file)
        shutil.copy2(subtitle_file, stored_subtitle)

        # ── Build video filter: cover title on first frame + subtitles ──
        sub_filter = _subtitle_filter(subtitle_file, subtitle_template_id)
        vf = sub_filter
        title_text = (cover_title or "").strip()
        if title_text:
            title_vf = _cover_title_drawtext(title_text)
            if title_vf:
                vf = f"{title_vf},{sub_filter}"

        # ── Mux video + audio + burn subtitles + cover title ──
        _run([
            "ffmpeg", "-y",
            "-i", str(merged_video),
            "-i", str(merged_audio),
            "-vf", vf,
            "-c:v", VIDEO_CODEC,
            "-preset", PRESET,
            "-pix_fmt", PIX_FMT,
            "-c:a", AUDIO_CODEC,
            "-shortest",
            "-movflags", "+faststart",
            str(final_video),
        ])

        # ── Extract first frame of final video as cover image ──
        if final_video.exists():
            _run([
                "ffmpeg", "-y",
                "-i", str(final_video),
                "-frames:v", "1",
                "-q:v", "2",
                str(cover_image),
            ])

        return {
            "final_video_path": str(final_video),
            "cover_image_path": str(cover_image),
            "subtitle_path": str(stored_subtitle),
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
