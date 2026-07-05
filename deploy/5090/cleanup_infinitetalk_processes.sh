#!/usr/bin/env bash
set -euo pipefail

pkill -TERM -f 'uvicorn infinitetalk_avatar_api_server:app.*--port 8893' || true
pkill -TERM -f 'generate_infinitetalk.py.*api_jobs' || true
sleep 3
pkill -KILL -f 'uvicorn infinitetalk_avatar_api_server:app.*--port 8893' || true
pkill -KILL -f 'generate_infinitetalk.py.*api_jobs' || true
