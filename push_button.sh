#!/bin/bash

# 1. 패치노트 디스코드 알림 스크립트 실행
echo "📡 디스코드 패치노트 전송 중..."
python alert.py

# 스크립트 실패 시 push 중단
if [ $? -ne 0 ]; then
  echo "❌ 패치노트 전송 실패 또는 upstream 오류. push를 중단합니다."
  exit 1
fi

# 2. 현재 브랜치 자동 감지
branch=$(git rev-parse --abbrev-ref HEAD)

# 3. git push 실행
echo "🚀 git push origin $branch 실행 중..."
git push -u origin "$branch"

