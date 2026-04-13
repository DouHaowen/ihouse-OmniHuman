from pathlib import Path

import app
from generate_audio import generate_audio

TEXT = "日本平均每年发生地震超过一千次，是全球地震最频繁的国家之一。但你有没有想过，日本人住的房子，到底是怎么扛住这些地震的？"


def main():
    voice_preset = app._get_voice_preset("mandarin_male", "cn")
    voice_id = voice_preset.get("voice_id")
    output_path = Path("/app/output/_voice_checks/segment00_mandarin_male.mp3")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"voice_id={voice_id}", flush=True)
    generate_audio(
        TEXT,
        str(output_path),
        voice=voice_id,
        speed=float(voice_preset.get("default_speed", 1.1)),
        volume=float(voice_preset.get("default_volume", 1.0)),
    )
    print(f"saved={output_path}", flush=True)


if __name__ == "__main__":
    main()
