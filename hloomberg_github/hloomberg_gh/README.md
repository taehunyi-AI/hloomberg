# HLOOMBERG TERMINAL — GitHub Pages 배포

## 배포 방법 (10분)

### 1. GitHub Repository 생성
https://github.com/new
- Repository name: `hloomberg`
- Public 선택 (Pages 무료 사용)
- Create repository

### 2. 파일 업로드
이 폴더의 파일 전체를 업로드:
```
hloomberg/
  .github/workflows/refresh.yml
  index.html
  refresh.py
  README.md
```

GitHub 웹에서: Add file → Upload files → 폴더째 드래그

### 3. API Key 등록
Repository → Settings → Secrets and variables → Actions → New repository secret
```
Name:   ANTHROPIC_API_KEY
Secret: sk-ant-api03-df2AHbc...
```

### 4. GitHub Pages 활성화
Repository → Settings → Pages
- Source: Deploy from a branch
- Branch: main / (root)
- Save

### 5. Actions 활성화 확인
Repository → Actions 탭 → "HLOOMBERG Refresh" 워크플로우 확인
최초 1회 수동 실행: Run workflow 버튼 클릭

### 6. iPhone Chrome 접속
```
https://[GitHub사용자명].github.io/hloomberg
```

## 동작 방식
```
GitHub Actions (5분마다 cron)
  → refresh.py 실행
      ├─ Yahoo Finance (21개 티커)
      ├─ Naver fallback (KR주식, USDKRW)
      ├─ Stooq fallback (NIKKEI)
      └─ Claude API (분석)
  → index.html 업데이트
  → git commit & push
  → GitHub Pages 자동 배포
  → iPhone: 5분마다 자동 새로고침
```

## 비용
- GitHub: 무료 (Actions 2,000분/월 무료)
- 5분마다 실행 = 월 ~300분 사용
- Claude API: 실행당 ~$0.01
