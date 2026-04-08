# iHouse 视频自动化生产系统

## 功能
输入一个选题 → 自动生成：
- 完整播报文案 + 时间轴
- 数字人配音音频（每段）
- 数字人视频（每段）
- 素材图片（每个素材段落）
- 小红书 + Facebook 发布文案

## 环境准备

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置API密钥
复制 `.env.example` 为 `.env`，填入你的密钥：
```bash
cp .env.example .env
```

编辑 `.env`：
```
VOLC_ACCESS_KEY=你的火山引擎AccessKey
VOLC_SECRET_KEY=你的火山引擎SecretKey
ANTHROPIC_API_KEY=你的Claude APIKey
MINIMAX_API_KEY=你的MiniMaxAPIKey
PEXELS_API_KEY=你的Pexels APIKey
DIGITAL_HUMAN_IMAGE=./assets/anchor.jpg
TTS_VOICE=Chinese (Mandarin)_Warm_Bestie
```

### 3. 准备数字人图片
将主播图片放到 `./assets/anchor.jpg`
（即梦生成的女主播图片）

## 使用方法

```bash
# 运行完整流水线
python main.py "为什么日本的房子是永久产权"

# 更多示例
python main.py "一家五口移居日本，签证怎么规划"
python main.py "日本经营管理签证2025年新规解读"
python main.py "为什么越来越多台湾人选择移居日本"
```

## 输出文件结构

```
output/
└── {时间戳}_{选题}/
    ├── script_readable.txt   ← 完整文案+时间轴（人工检查用）
    ├── social_posts.txt      ← 小红书+Facebook发布文案
    ├── script.json           ← 原始数据
    ├── result.json           ← 完整生产结果
    ├── audio/                ← 配音音频（每段一个mp3）
    ├── digital_human/        ← 数字人视频（每段一个mp4）
    └── materials/            ← 素材图片（每个素材段落）
```

## 导入剪映

1. 打开剪映
2. 导入 `digital_human/` 下的所有 mp4（按编号顺序）
3. 导入 `materials/` 下的图片插入对应时间段
4. 参考 `script_readable.txt` 的时间轴调整顺序
5. 导入 `audio/` 下的完整配音音频到主音轨
6. 加字幕、加背景音乐，完成发布

## API申请指引

| API | 地址 | 用途 |
|-----|------|------|
| 火山引擎 | volcengine.com | OmniHuman数字人 + TTS |
| Claude | console.anthropic.com | 文案生成 |
| MiniMax | minimaxi.com | Speech TTS配音 |
| Pexels | pexels.com/api | 免费素材图片 |
