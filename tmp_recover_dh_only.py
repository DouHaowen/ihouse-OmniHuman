from pathlib import Path

import app
from generate_digital_human import generate_digital_human_video
from tos_uploader import upload_file_and_get_url

OUTPUT_DIR = Path("/app/output/1775725774_full_日本是众所周知的地震大国他们的房子都是")
AVATAR_ID = "avatar_test_0cd3d70a.png"


def main():
    avatar_option = app._get_avatar_option(AVATAR_ID)
    if not avatar_option:
        raise RuntimeError(f"找不到主播图: {AVATAR_ID}")

    image_url = upload_file_and_get_url(avatar_option["image_path"], key_prefix="full/image")
    print("avatar", AVATAR_ID, image_url, flush=True)

    script_data = app.json.loads((OUTPUT_DIR / "script.json").read_text())
    for index, seg in enumerate(script_data.get("segments", [])):
        if seg.get("type") != "digital_human":
            continue

        video_output = OUTPUT_DIR / "digital_human" / f"dh_{index:02d}.mp4"
        if video_output.exists():
            print("reuse", video_output, flush=True)
            continue

        audio_path = OUTPUT_DIR / "audio" / f"segment_{index:02d}_{seg.get('type', '')}.mp3"
        if not audio_path.exists():
            raise RuntimeError(f"缺少音频文件: {audio_path}")

        audio_url = upload_file_and_get_url(str(audio_path), key_prefix="full/audio")
        print("generate", index, seg.get("script", "")[:24], flush=True)
        generate_digital_human_video(
            image_url=image_url,
            audio_url=audio_url,
            output_path=str(video_output),
            prompt=app._combine_prompt(avatar_option.get("style_prompt", ""), seg.get("action", "")),
        )
        print("saved", video_output, flush=True)

    print("done", flush=True)


if __name__ == "__main__":
    main()
