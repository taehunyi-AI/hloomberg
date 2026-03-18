#!/usr/bin/env python3
"""
HLOOMBERG TERMINAL — refresh.py
GitHub Actions에서 5분마다 실행
수집: 시세(Yahoo/Naver/Stooq) + 뉴스(RSS/네이버/다음) + 공시(DART) + 리서치(네이버금융) + AI분석(Claude)
패치: hloomberg.html 마커 치환
"""
import os, json, re, time, html as htmlmod
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
GROQ_KEY      = os.environ.get('GROQ_API_KEY', '')
DART_KEY      = os.environ.get('DART_API_KEY', '')
GH_PAT        = os.environ.get('GH_PAT', '')
GITHUB_REPO   = os.environ.get('GITHUB_REPOSITORY', '')

# Groq 모델 (항상 Groq 사용)
GROQ_MODEL_HIGH = 'llama-3.3-70b-versatile'  # 고품질 분석
GROQ_MODEL_FAST = 'llama-3.1-8b-instant'     # 빠른 요약/추출
HTML_FILE     = 'hloomberg.html'

# AI_MODE: full=전체AI(KST 08:00), partial=종합+TOP10(KST 20:00), ''=없음(매 5분)
AI_MODE    = os.environ.get('AI_MODE', '').strip().lower()

# 최초 실행 감지: hloomberg.html에 데이터가 없으면 full 모드 강제
def _is_first_run():
    try:
        with open(HTML_FILE, encoding='utf-8') as f:
            html = f.read()
        # TICKS=[] 이면 데이터 없는 초기 상태
        return 'const TICKS=[];' in html or 'const TICKS=[]' in html
    except:
        return True

if not AI_MODE and _is_first_run():
    AI_MODE = 'full'
    print('[최초실행 감지] AI_MODE=full 강제 적용')

STOCK_MODE = (AI_MODE in ('full','stock'))    # 상세분석: full + stock 모드
AI_FULL    = (AI_MODE == 'full')             # 이슈/원자재/TOP10: full만
AI_PARTIAL = (AI_MODE in ('full','partial')) # 종합분석/TOP10: full+partial

KST = timezone(timedelta(hours=9))
NOW = datetime.now(KST)
TS  = NOW.strftime('%Y-%m-%d %H:%M KST')
TS_SHORT = NOW.strftime('%m/%d %H:%M')
AI_HOURLY = NOW.minute in range(5, 12)  # :07분 cron 기준 ±5분 허용

# 주말 감지 (0=월 ... 6=일)
IS_WEEKEND = NOW.weekday() >= 5  # 토(5), 일(6)
# 직전 영업일 계산 (주말이면 금요일 기준)
def last_biz_day(dt, n=1):
    d = dt
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return d

ANALYSIS_BASE = '전주 금요일 마감 데이터 기준 분석' if IS_WEEKEND else '당일 실시간 데이터 기준 분석'
DATA_BGN_DATE = last_biz_day(NOW, 5).strftime('%Y%m%d')  # 최근 5영업일 시작

SESS = requests.Session()
SESS.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0',
    'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
})

mode_label = f'[{AI_MODE.upper()} MODE]' if AI_MODE else ''
print(f'[{TS}] HLOOMBERG refresh start {mode_label}')

# ─────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────
def HE(s):
    """HTML escape"""
    return htmlmod.escape(str(s or ''))

def JE(s):
    """JS string escape (single-quote safe)"""
    return str(s or '').replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n').replace('\r', '')

def safe_get(url, timeout=8, headers=None, referer=None):
    try:
        h = {'Referer': referer} if referer else {}
        if headers:
            h.update(headers)
        r = SESS.get(url, timeout=timeout, headers=h)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f'  FAIL {url[:60]}: {e}')
        return None

def fmt_time(dt):
    if not dt:
        return ''
    diff = NOW - dt.replace(tzinfo=KST) if dt.tzinfo is None else NOW - dt
    m = int(diff.total_seconds() / 60)
    if m < 1: return '방금'
    if m < 60: return f'{m}분전'
    h = m // 60
    if h < 24: return f'{h}시간전'
    return f'{m//1440}일전'

# ─────────────────────────────────────────
# 1. 시세 수집
# ─────────────────────────────────────────

# KIS REST API 설정
KIS_APP_KEY    = os.environ.get('KIS_APP_KEY', '')
KIS_APP_SECRET = os.environ.get('KIS_APP_SECRET', '')
KIS_TOKEN_FILE = '/tmp/kis_token.json'   # 로컬 캐시 (fallback용)
KIS_BASE_URL   = 'https://openapi.koreainvestment.com:9443'

PRICE_DATA = {}   # ← Yahoo 수집 전 KIS가 먼저 채움
TICKERS = {
    'KOSPI':   '^KS11',   'KOSDAQ':  '^KQ11',
    'BRENT':   'BZ=F',    'WTI':     'CL=F',
    'GOLD':    'GC=F',    'NATGAS':  'NG=F',
    'SP500':   '^GSPC',   'NIKKEI':  '^N225',
    'VIX':     '^VIX',    'UST10':   '^TNX',
    'USDKRW':  'USDKRW=X',
    'SILVER':  'SI=F',    'COPPER':  'HG=F',
    'LITHIUM': 'LIT',
    'URA':     'URA',
}
TICK_META = {
    'KOSPI':   {'l':'KOSPI',    'u':'',  'dp':0},
    'KOSDAQ':  {'l':'KOSDAQ',   'u':'',  'dp':2},
    'BRENT':   {'l':'BRENT',    'u':'$', 'dp':2},
    'WTI':     {'l':'WTI',      'u':'$', 'dp':2},
    'GOLD':    {'l':'GOLD',     'u':'$', 'dp':0},
    'NATGAS':  {'l':'NAT GAS',  'u':'$', 'dp':3},
    'SP500':   {'l':'S&P500',   'u':'',  'dp':2},
    'NIKKEI':  {'l':'NIKKEI',   'u':'',  'dp':0},
    'VIX':     {'l':'VIX',      'u':'',  'dp':2},
    'UST10':   {'l':'10Y UST',  'u':'',  'dp':3},
    'USDKRW':  {'l':'USD/KRW',  'u':'',  'dp':2},
    'SILVER':  {'l':'SILVER',   'u':'$', 'dp':2},
    'COPPER':  {'l':'COPPER',   'u':'$', 'dp':3},
    'LITHIUM': {'l':'LITHIUM',  'u':'$', 'dp':2},
    'URA':     {'l':'URANIUM',  'u':'$', 'dp':2},
}




def _save_secret(name, value):
    """GitHub Secrets에 값 저장 (PyNaCl 필요)"""
    if not GH_PAT or not GITHUB_REPO:
        return False
    try:
        import base64
        from nacl import encoding, public as nacl_public
        # Public key 조회
        r = SESS.get(
            f'https://api.github.com/repos/{GITHUB_REPO}/actions/secrets/public-key',
            headers={'Authorization': f'token {GH_PAT}', 'Accept': 'application/vnd.github+json'},
            timeout=10
        )
        if not r.ok:
            print(f'  Secrets public-key 조회 실패: {r.status_code}')
            return False
        pk_data = r.json()
        # 암호화
        pk = nacl_public.PublicKey(pk_data['key'].encode(), encoding.Base64Encoder)
        box = nacl_public.SealedBox(pk)
        encrypted = base64.b64encode(box.encrypt(value.encode())).decode()
        # 저장
        r2 = SESS.put(
            f'https://api.github.com/repos/{GITHUB_REPO}/actions/secrets/{name}',
            headers={'Authorization': f'token {GH_PAT}', 'Accept': 'application/vnd.github+json'},
            json={'encrypted_value': encrypted, 'key_id': pk_data['key_id']},
            timeout=10
        )
        if r2.ok:
            print(f'  Secret {name} 저장 OK')
            return True
        else:
            print(f'  Secret {name} 저장 실패: {r2.status_code}')
            return False
    except ImportError:
        print('  PyNaCl 없음 — pip install PyNaCl')
        return False
    except Exception as e:
        print(f'  Secret 저장 오류: {e}')
        return False


def _load_secret_token():
    """GitHub Secrets에서 KIS 토큰 로드 후 유효성 검사"""
    token   = os.environ.get('KIS_ACCESS_TOKEN', '')
    expired = os.environ.get('KIS_TOKEN_EXPIRED', '')
    if not token or not expired:
        return None
    try:
        expire_dt = datetime.fromisoformat(expired)
        expire_dt = expire_dt.replace(tzinfo=KST) if expire_dt.tzinfo is None else expire_dt
        if NOW < expire_dt - timedelta(hours=1):
            print(f'  KIS 토큰 재사용 [Secrets] (만료: {expire_dt.strftime("%m/%d %H:%M")})')
            # /tmp 파일에도 캐시 (하위 함수 호환)
            with open(KIS_TOKEN_FILE, 'w') as f:
                json.dump({'access_token': token, 'access_token_token_expired': expired}, f)
            return token
    except Exception as e:
        print(f'  Secrets 토큰 파싱 오류: {e}')
    return None


def _load_file_token():
    """/tmp 캐시 파일에서 KIS 토큰 로드"""
    try:
        with open(KIS_TOKEN_FILE, 'r') as f:
            cached = json.load(f)
        expire_dt = datetime.fromisoformat(cached.get('access_token_token_expired', '2000-01-01'))
        expire_dt = expire_dt.replace(tzinfo=KST) if expire_dt.tzinfo is None else expire_dt
        if NOW < expire_dt - timedelta(hours=1):
            print(f'  KIS 토큰 재사용 [파일캐시] (만료: {expire_dt.strftime("%m/%d %H:%M")})')
            return cached['access_token']
    except Exception:
        pass
    return None


def kis_get_token():
    """KIS access_token 관리
    우선순위: 1) GitHub Secrets → 2) /tmp 파일캐시 → 3) 신규 발급 + Secrets 저장
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return None

    # 1순위: GitHub Secrets (컨테이너 재시작과 무관하게 영구 재사용)
    token = _load_secret_token()
    if token:
        return token

    # 2순위: /tmp 파일캐시 (같은 컨테이너 내 재실행)
    token = _load_file_token()
    if token:
        return token

    # 3순위: 신규 발급
    try:
        resp = SESS.post(
            f'{KIS_BASE_URL}/oauth2/tokenP',
            json={'grant_type': 'client_credentials',
                  'appkey': KIS_APP_KEY, 'appsecret': KIS_APP_SECRET},
            timeout=10
        )
        if not resp.ok:
            print(f'  KIS 토큰 발급 실패: {resp.status_code}')
            return None
        data = resp.json()
        token = data.get('access_token')
        expired = data.get('access_token_token_expired', '')
        if not token:
            print(f'  KIS 토큰 없음: {data}')
            return None
        # /tmp 파일 저장
        with open(KIS_TOKEN_FILE, 'w') as f:
            json.dump(data, f)
        print(f'  KIS 토큰 신규 발급 OK — Secrets에 저장 중...')
        # GitHub Secrets에 저장 (다음 실행부터 재사용)
        _save_secret('KIS_ACCESS_TOKEN', token)
        _save_secret('KIS_TOKEN_EXPIRED', expired)
        return token
    except Exception as e:
        print(f'  KIS 토큰 발급 오류: {e}')
        return None


def fetch_kis_price(code, token):
    """KIS REST — 국내주식 현재가 조회 (TR: FHKST01010100)"""
    try:
        headers = {
            'Content-Type':   'application/json; charset=utf-8',
            'authorization':  f'Bearer {token}',
            'appkey':         KIS_APP_KEY,
            'appsecret':      KIS_APP_SECRET,
            'tr_id':          'FHKST01010100',
            'custtype':       'P',
        }
        params = {
            'FID_COND_MRKT_DIV_CODE': 'J',
            'FID_INPUT_ISCD':         code,
        }
        r = SESS.get(
            f'{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price',
            headers=headers, params=params, timeout=6
        )
        if not r.ok:
            print(f'    KIS 종목 {code} HTTP {r.status_code}')
            return None
        j = r.json()
        rt = j.get('rt_cd', '')
        if rt != '0':
            msg = j.get('msg1', '')[:50]
            print(f'    KIS 종목 {code} 오류: rt_cd={rt} {msg}')
            return None
        d = j.get('output', {})
        p  = float(d.get('stck_prpr', 0) or 0)
        c  = float(d.get('prdy_ctrt', 0) or 0)
        p0 = float(d.get('stck_sdpr', 0) or 0)
        if p > 0:
            return {'p': p, 'c': c, 'p0': p0, 'src': 'KIS'}
        # KIS 가격 0 → Yahoo Finance fallback (.KS → .KQ 순서로 시도)
        _yahoo_tried = []
        for suffix in ('.KS', '.KQ'):
            sym = code + suffix
            try:
                r2 = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}',
                                  headers={'User-Agent':'Mozilla/5.0'}, timeout=5)
                if r2.ok:
                    meta = r2.json().get('chart',{}).get('result',[{}])[0].get('meta',{})
                    p = meta.get('regularMarketPrice', 0)
                    pc = meta.get('chartPreviousClose', p)
                    c = round((p - pc) / pc * 100, 2) if pc else 0
                    if p > 0:
                        print(f'    KIS 종목 {code} → Yahoo fallback({suffix}): {p:,}원 ({c:+.2f}%)')
                        return {'p': p, 'c': c, 'src': 'Yahoo'}
                    _yahoo_tried.append(f'{suffix}:p=0')
                else:
                    _yahoo_tried.append(f'{suffix}:HTTP{r2.status_code}')
            except Exception as _ye:
                _yahoo_tried.append(f'{suffix}:{str(_ye)[:30]}')
        print(f'    KIS 종목 {code} 가격 0 — KIS:stck_prpr=0, Yahoo:{_yahoo_tried}')
    except Exception as e:
        print(f'    KIS 종목 {code} 예외: {e}')
    return None


def fetch_kis_index(market_code, token):
    """KIS REST — 지수 현재가 조회 (KOSPI/KOSDAQ)"""
    try:
        headers = {
            'Content-Type':  'application/json',
            'authorization': f'Bearer {token}',
            'appkey':        KIS_APP_KEY,
            'appsecret':     KIS_APP_SECRET,
            'tr_id':         'FHPUP02100000',   # 국내업종지수
        }
        params = {
            'FID_COND_MRKT_DIV_CODE': 'U',
            'FID_INPUT_ISCD': market_code,      # 0001=KOSPI, 1001=KOSDAQ
        }
        r = SESS.get(
            f'{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-index-price',
            headers=headers, params=params, timeout=6
        )
        if not r.ok: return None
        d = r.json().get('output', {})
        p = float(d.get('bstp_nmix_prpr', 0) or 0)   # 현재 지수
        c = float(d.get('bstp_nmix_prdy_ctrt', 0) or 0)  # 전일대비율
        if p > 0:
            return {'p': p, 'c': c, 'src': 'KIS'}
    except Exception as e:
        pass
    return None

# KIS 시세 데이터 (종목코드 → price dict)
kis_stock_data = {}



# 원자재 90일 차트 데이터 수집
CMDTY_CHART_SYMS = {
    'BRENT': 'BZ=F', 'WTI': 'CL=F', 'GOLD': 'GC=F',
    'NATGAS': 'NG=F', 'SILVER': 'SI=F', 'COPPER': 'HG=F',
}
cmdty_chart = {}


def kis_get(url_path, tr_id, params, token):
    """KIS REST GET 공통 헬퍼"""
    try:
        headers = {
            'Content-Type':  'application/json; charset=utf-8',
            'authorization': f'Bearer {token}',
            'appkey':        KIS_APP_KEY,
            'appsecret':     KIS_APP_SECRET,
            'tr_id':         tr_id,
            'custtype':      'P',
        }
        r = SESS.get(f'{KIS_BASE_URL}{url_path}', headers=headers, params=params, timeout=8)
        if not r.ok: return None
        j = r.json()
        if j.get('rt_cd') != '0': return None
        return j
    except: return None

def fetch_foreign_institution(token, mkt='0001', cls='1'):
    """1순위: 외국인/기관 순매수 TOP10
    cls: 1=외국인, 2=기관계, 0=전체
    mkt: 0001=코스피, 1001=코스닥
    """
    j = kis_get(
        '/uapi/domestic-stock/v1/quotations/foreign-institution-total',
        'FHPTJ04400000',
        {'FID_COND_MRKT_DIV_CODE':'V','FID_COND_SCR_DIV_CODE':'16449',
         'FID_INPUT_ISCD':mkt,'FID_DIV_CLS_CODE':'1',   # 1=금액정렬
         'FID_RANK_SORT_CLS_CODE':'0','FID_ETC_CLS_CODE':cls},
        token
    )
    if not j: return []
    result = []
    for item in j.get('output', [])[:10]:
        try:
            result.append({
                'name': item.get('hts_kor_isnm','').strip(),
                'code': item.get('mksc_shrn_iscd','').strip(),
                'net':  int(str(item.get('ntby_qty',0) or 0).replace(',','')),
                'net_amt': int(str(item.get('ntby_tr_pbmn',0) or 0).replace(',','')),
            })
        except: pass
    return [x for x in result if x['name']]

def fetch_fluctuation_rank(token, sort='0'):
    """2순위: 등락률 상위 TOP20"""
    # kis_get 대신 직접 호출 (파라미터 대문자 강제)
    try:
        headers = {
            'Content-Type':  'application/json; charset=utf-8',
            'authorization': f'Bearer {token}',
            'appkey':        KIS_APP_KEY,
            'appsecret':     KIS_APP_SECRET,
            'tr_id':         'FHPST01700000',
            'custtype':      'P',
        }
        params = {
            'FID_COND_MRKT_DIV_CODE':  'J',
            'FID_COND_SCR_DIV_CODE':   '20170',
            'FID_INPUT_ISCD':          '0000',
            'FID_RANK_SORT_CLS_CODE':  '0',
            'FID_INPUT_CNT_1':         '20',
            'FID_PRC_CLS_CODE':        '0',
            'FID_INPUT_PRICE_1':       '1000',
            'FID_INPUT_PRICE_2':       '500000',
            'FID_VOL_CNT':             '10000',
            'FID_TRGT_CLS_CODE':       '0',
            'FID_TRGT_EXLS_CLS_CODE':  '0',
            'FID_DIV_CLS_CODE':        '0',
            'FID_RSFL_RATE1':          '0',
            'FID_RSFL_RATE2':          '30',
        }
        r = SESS.get(f'{KIS_BASE_URL}/uapi/domestic-stock/v1/ranking/fluctuation',
                     headers=headers, params=params, timeout=8)
        if not r.ok:
            print(f'    등락률 HTTP {r.status_code}')
            return []
        j = r.json()
        if j.get('rt_cd') != '0':
            print(f'    등락률 오류: {j.get("rt_cd")} {j.get("msg1","")[:40]}')
            return []
        result = []
        for item in j.get('output', []):
            try:
                result.append({
                    'name':  item.get('hts_kor_isnm','').strip(),
                    'code':  item.get('stck_shrn_iscd','').strip(),
                    'rate':  float(item.get('prdy_ctrt','0') or 0),
                    'price': int(str(item.get('stck_prpr','0') or 0).replace(',','')),
                    'vol':   int(str(item.get('acml_vol','0') or 0).replace(',','')),
                })
            except: pass
        return [x for x in result if x['name'] and x['code']]
    except Exception as e:
        print(f'    등락률 예외: {e}')
        return []

def fetch_volume_rank(token):
    """3순위: 거래량 상위 TOP20"""
    j = kis_get(
        '/uapi/domestic-stock/v1/quotations/volume-rank',
        'FHPST01710000',
        {'FID_COND_MRKT_DIV_CODE':'J','FID_COND_SCR_DIV_CODE':'20171',
         'FID_INPUT_ISCD':'0000','FID_DIV_CLS_CODE':'0',
         'FID_BLNG_CLS_CODE':'0','FID_TRGT_CLS_CODE':'111111111',
         'FID_TRGT_EXLS_CLS_CODE':'0000000000',
         'FID_INPUT_PRICE_1':'1000','FID_INPUT_PRICE_2':'1000000',
         'FID_VOL_CNT':'50000','FID_INPUT_DATE_1':''},
        token
    )
    if not j: return []
    result = []
    for item in j.get('output', [])[:20]:
        try:
            result.append({
                'name': item.get('hts_kor_isnm','').strip(),
                'code': item.get('mksc_shrn_iscd','').strip(),
                'vol':  int(item.get('acml_vol','0').replace(',','') or 0),
                'rate': float(item.get('prdy_ctrt','0') or 0),
                'price': int(item.get('stck_prpr','0').replace(',','') or 0),
            })
        except: pass
    return [x for x in result if x['name']]

def fetch_kis_ohlcv(code, token, days=90):
    """4순위: KIS 국내주식 기간별시세 (일봉 OHLCV)"""
    from datetime import datetime, timedelta
    end   = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=days+30)).strftime('%Y%m%d')
    j = kis_get(
        '/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice',
        'FHKST03010100',
        {'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code,
         'FID_INPUT_DATE_1':start,'FID_INPUT_DATE_2':end,
         'FID_PERIOD_DIV_CODE':'D','FID_ORG_ADJ_PRC':'0'},
        token
    )
    if not j: return None
    rows = j.get('output2', [])
    if not rows: return None
    ohlcv = []
    for item in reversed(rows):
        try:
            c = int(item.get('stck_clpr','0') or 0)
            if c <= 0: continue
            ohlcv.append({
                'd': item.get('stck_bsop_date','')[:4][2:] + '/' + item.get('stck_bsop_date','')[4:6] + '/' + item.get('stck_bsop_date','')[6:],
                'o': int(item.get('stck_oprc','0') or 0),
                'h': int(item.get('stck_hgpr','0') or 0),
                'l': int(item.get('stck_lwpr','0') or 0),
                'c': c,
                'v': int(item.get('acml_vol','0').replace(',','') or 0),
            })
        except: pass
    if len(ohlcv) < 10: return None
    # 기술지표 계산 (기존 fetch_ohlcv 로직 재사용)
    cl = [x['c'] for x in ohlcv]
    def ma(arr, n):
        return [round(sum(arr[max(0,i-n+1):i+1])/min(i+1,n)) if i>=n-1 else None for i in range(len(arr))]
    ma5=ma(cl,5); ma20=ma(cl,20); ma60=ma(cl,60)
    rsi_arr=[None]*14
    if len(cl)>14:
        g=l2=0
        for i in range(1,15): d=cl[i]-cl[i-1]; g+=max(d,0); l2+=max(-d,0)
        ag,al=g/14,l2/14
        rsi_arr.append(round(100-100/(1+ag/al),1) if al>0 else 100)
        for i in range(15,len(cl)):
            d=cl[i]-cl[i-1]; ag=(ag*13+max(d,0))/14; al=(al*13+max(-d,0))/14
            rsi_arr.append(round(100-100/(1+ag/al),1) if al>0 else 100)
    bbu=[]; bbl=[]
    for i in range(len(cl)):
        if i<19: bbu.append(None); bbl.append(None)
        else:
            seg=cl[i-19:i+1]; m=sum(seg)/20; std=(sum((x-m)**2 for x in seg)/20)**0.5
            bbu.append(round(m+2*std)); bbl.append(round(m-2*std))
    for i,bar in enumerate(ohlcv):
        bar['ma5']=ma5[i]; bar['ma20']=ma20[i]; bar['ma60']=ma60[i]
        bar['rsi']=rsi_arr[i] if i<len(rsi_arr) else None
        bar['bbu']=bbu[i]; bar['bbl']=bbl[i]
    return ohlcv[-days:]

def fetch_investor_by_stock(code, token):
    """5순위: 종목별 외국인/기관 투자자 동향"""
    j = kis_get(
        '/uapi/domestic-stock/v1/quotations/inquire-investor',
        'FHKST01010900',
        {'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code},
        token
    )
    if not j: return {}
    d = {}
    for item in j.get('output', []):
        tp = item.get('invst_nm','').strip()
        if tp in ('외국인','기관합계','개인'):
            try:
                d[tp] = {
                    'net': int(item.get('ntby_qty','0').replace(',','') or 0),
                    'buy': int(item.get('seln_vol','0').replace(',','') or 0),
                    'sell': int(item.get('shnu_vol','0').replace(',','') or 0),
                }
            except: pass
    return d

def fetch_finance_ratio(code, token):
    """6순위: 재무비율 (PER/PBR/ROE/EPS 등)"""
    j = kis_get(
        '/uapi/domestic-stock/v1/finance/financial-ratio',
        'FHKST66430300',
        {'FID_DIV_CLS_CODE':'0','bstp_cls_code':code,'FID_INPUT_ISCD':code},
        token
    )
    if not j: return {}
    rows = j.get('output', [])
    if not rows: return {}
    r = rows[0]
    try:
        return {
            'per':  r.get('per',''),
            'pbr':  r.get('pbr',''),
            'roe':  r.get('roe',''),
            'eps':  r.get('eps',''),
            'bps':  r.get('bps',''),
            'dps':  r.get('dps',''),
        }
    except: return {}

def fetch_yahoo(sym):
    for q in ('query1', 'query2'):
        try:
            url = f'https://{q}.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}?interval=1d&range=5d'
            r = SESS.get(url, timeout=8, headers={'Referer':'https://finance.yahoo.com','Accept':'application/json'})
            if not r.ok: continue
            closes = [v for v in r.json()['chart']['result'][0]['indicators']['quote'][0]['close'] if v is not None]
            if not closes: continue
            p, p0 = closes[-1], (closes[-2] if len(closes)>=2 else closes[-1])
            return {'p': p, 'c': (p-p0)/p0*100 if p0 else 0, 'src': 'Yahoo'}
        except:
            time.sleep(0.2)
    return None

def fetch_naver_price(sym):
    try:
        if sym == '^KS11':   url = 'https://m.stock.naver.com/api/index/KOSPI/basic'
        elif sym == '^KQ11': url = 'https://m.stock.naver.com/api/index/KOSDAQ/basic'
        elif sym == 'USDKRW=X': url = 'https://m.stock.naver.com/api/stock/USDKRW/basic'
        elif sym.endswith('.KS'): url = f'https://m.stock.naver.com/api/stock/{sym[:-3]}/basic'
        else: return None
        r = SESS.get(url, timeout=6, headers={'Referer':'https://finance.naver.com'})
        if not r.ok: return None
        j = r.json()
        p = float(str(j.get('closePrice',0)).replace(',',''))
        c = float(re.sub(r'[^0-9.\-]','', str(j.get('fluctuationsRatio',0))) or '0')
        return {'p': p, 'c': c, 'src': 'Naver'} if p > 0 else None
    except: return None

def fetch_stooq(sym):
    try:
        sym_map = {'^N225': '^nkx', '^GSPC': '^spx', 'BZ=F': 'bz.f', 'CL=F': 'cl.f', 'GC=F': 'gc.f'}
        s = sym_map.get(sym)
        if not s: return None
        r = SESS.get(f'https://stooq.com/q/d/l/?s={s}&i=d', timeout=6)
        if not r.ok: return None
        rows = [l for l in r.text.strip().split('\n') if l and l[0].isdigit()]
        if len(rows) < 2: return None
        p, p0 = float(rows[-1].split(',')[4]), float(rows[-2].split(',')[4])
        return {'p': p, 'c': (p-p0)/p0*100 if p0 else 0, 'src': 'Stooq'} if p > 0 else None
    except: return None

def get_price(key, sym):
    for fn in (fetch_yahoo, fetch_naver_price, fetch_stooq):
        r = fn(sym)
        if r: return r
    return None

kis_token = None
if KIS_APP_KEY and KIS_APP_SECRET:
    print(f'\n[KIS] 토큰 확인...')
    kis_token = kis_get_token()

if kis_token:
    print(f'[KIS] 국내 시세 수집...')
    # KOSPI 지수
    r = fetch_kis_index('0001', kis_token)
    if r:
        PRICE_DATA['KOSPI'] = r
        print(f"  OK  KOSPI       {r['p']:>10.2f}  ({'+' if r['c']>=0 else ''}{r['c']:.2f}%)  [KIS]")
    time.sleep(0.05)
    # KOSDAQ 지수
    r = fetch_kis_index('1001', kis_token)
    if r:
        PRICE_DATA['KOSDAQ'] = r
        print(f"  OK  KOSDAQ      {r['p']:>10.2f}  ({'+' if r['c']>=0 else ''}{r['c']:.2f}%)  [KIS]")
    time.sleep(0.05)
    # 스윙종목 9개
    stock_code_map = {
        '005930': 'KIS_005930', '000660': 'KIS_000660',
        '012450': 'KIS_012450', '079550': 'KIS_079550',
        '015760': 'KIS_015760', '010950': 'KIS_010950',
        '008730': 'KIS_008730', '036460': 'KIS_036460',
        '207940': 'KIS_207940',
    }
    for code, key in stock_code_map.items():
        r = fetch_kis_price(code, kis_token)
        if r:
            kis_stock_data[code] = r
            print(f"  OK  {code}  {r['p']:>10.0f}원  ({'+' if r['c']>=0 else ''}{r['c']:.2f}%)  [KIS]")
        time.sleep(0.05)
    print(f'  KIS 완료: 지수 + {len(kis_stock_data)}개 종목')

    # ── 1순위: 외국인/기관 순매수 TOP10
    print('[KIS 수급] 외국인/기관 순매수 수집...')
    foreign_buy  = fetch_foreign_institution(kis_token, '0001', '1')   # 코스피 외국인
    institution_buy = fetch_foreign_institution(kis_token, '0001', '2') # 코스피 기관
    print(f'  외국인 순매수: {len(foreign_buy)}개 / 기관 순매수: {len(institution_buy)}개')
    time.sleep(0.3)

    # ── 2순위: 등락률 상위
    print('[KIS 순위] 등락률 상위 수집...')
    fluctuation_rank = fetch_fluctuation_rank(kis_token)
    print(f'  등락률 상위: {len(fluctuation_rank)}개')
    time.sleep(0.3)

    # ── 3순위: 거래량 상위
    print('[KIS 순위] 거래량 상위 수집...')
    volume_rank_data = fetch_volume_rank(kis_token)
    print(f'  거래량 상위: {len(volume_rank_data)}개')
    time.sleep(0.3)

else:
    if KIS_APP_KEY:
        print('[KIS] 토큰 발급 실패 — Yahoo fallback')
    else:
        print('[KIS] KIS_APP_KEY 미설정 — Yahoo fallback')
    foreign_buy = []; institution_buy = []; fluctuation_rank = []; volume_rank_data = []


print(f'\n[시세] {len(TICKERS)}개 병렬 수집...')
def _fetch_ticker(item):
    name, sym = item
    if name in PRICE_DATA:
        return name, None
    return name, get_price(name, sym)

_ticker_items = [(n,s) for n,s in TICKERS.items() if n not in PRICE_DATA]
with ThreadPoolExecutor(max_workers=8) as ex:
    for name, res in ex.map(_fetch_ticker, _ticker_items):
        if res:
            PRICE_DATA[name] = res
            sg = '+' if res['c'] >= 0 else ''
            print(f"  OK  {name:<10} {res['p']:>12.2f}  ({sg}{res['c']:.2f}%)  [{res['src']}]")
        else:
            if name not in PRICE_DATA:
                print(f"  FAIL {name}")
print(f'  → {len(PRICE_DATA)}/{len(TICKERS)} 수신')

def fmt_price(v, key):
    dp = TICK_META.get(key, {}).get('dp', 2)
    if dp == 0: return f'{round(v):,}'
    return f'{v:,.{dp}f}'

# TICKS JS 배열 생성 (KIS 지수 우선 반영)
def make_ticks_js():
    lines = ['const TICKS=[']
    for k, meta in TICK_META.items():
        d = PRICE_DATA.get(k)
        if not d: continue
        p = fmt_price(d['p'], k)
        c = d['c']
        sg = '+' if c >= 0 else ''
        cl = 'up' if c >= 0 else 'dn'
        cv = f'{sg}{abs(c):.2f}%'
        src_badge = ' [KIS]' if d.get('src') == 'KIS' else ''
        lines.append(f"  {{k:'{k}',l:'{meta['l']}',v:'{meta['u']}{p}',c:'{cv}',cl:'{cl}'}},")
    lines.append('];')
    return '\n'.join(lines)

# KIS 종목 데이터 JS 패치용
def make_kis_prices_js(data):
    """종목코드 → 현재가/등락률 JS 객체. HTML에 토큰 노출 없음."""
    if not data:
        return '\nconst KIS_PRICES={};\n'
    lines = ['const KIS_PRICES={']
    for code, d in data.items():
        p = round(d['p'])
        c = round(d['c'], 2)
        cl = 'up' if c >= 0 else 'dn'
        sg = '+' if c >= 0 else ''
        lines.append(f"  '{code}':{{p:{p},c:{c},cl:'{cl}',cv:'{sg}{abs(c):.2f}%'}},")
    lines.append('};')
    return '\n' + '\n'.join(lines) + '\n'

def make_stocks_js(top10):
    """AI 선정 TOP10 → STOCKS JS 배열"""
    if not top10:
        return '\nconst STOCKS=[];\n'
    JE = lambda s: str(s).replace('\\','\\\\').replace("'","\\'").replace('\n','\\n')
    lines = ['const STOCKS=[']
    for s in top10:
        name  = JE(s.get('name',''))
        code  = JE(s.get('code',''))
        mkt   = JE(s.get('mkt','KOSPI'))
        th    = JE(s.get('th',''))
        tc    = JE(s.get('tc','tsm'))
        act   = JE(s.get('act','관심'))
        ac    = JE(s.get('ac','awa'))
        risk  = int(s.get('risk', 3))
        desc  = JE(s.get('desc',''))
        lines.append(f"  {{name:'{name}',code:'{code}',mkt:'{mkt}',th:'{th}',tc:'{tc}',act:'{act}',ac:'{ac}',risk:{risk},desc:'{desc}'}},")
    lines.append('];')
    return '\n' + '\n'.join(lines) + '\n'

def make_supply_demand_js(foreign_buy, institution_buy, fluctuation_rank, volume_rank_data):
    """수급/순위 데이터 → JS"""
    def fmt_list(lst, max_n=10):
        return json.dumps(lst[:max_n], ensure_ascii=False)
    lines = [
        f'const KIS_FOREIGN_BUY={fmt_list(foreign_buy)};',
        f'const KIS_INSTITUTION_BUY={fmt_list(institution_buy)};',
        f'const KIS_FLUCTUATION={fmt_list(fluctuation_rank)};',
        f'const KIS_VOLUME_RANK={fmt_list(volume_rank_data)};',
    ]
    return '\n' + '\n'.join(lines) + '\n'


# KIS 담당 종목 (KOSPI/KOSDAQ 지수 + 국내 스윙종목)
KIS_STOCK_CODES = {
    'KOSPI':    ('U', 'FID_COND_MRKT_DIV_CODE', '0001'),  # 지수 특수처리
    'KOSDAQ':   ('U', 'FID_COND_MRKT_DIV_CODE', '1001'),
    '005930':   'SAMSUNG',    # 삼성전자
    '000660':   'SKHYNIX',    # SK하이닉스
    '012450':   'HANWHA_AE',  # 한화에어로
    '079550':   'LIG',        # LIG넥스원
    '015760':   'KEPCO',      # 한국전력
    '010950':   'SOIL',       # S-Oil
    '008730':   'KARBON',     # 한국카본
    '036460':   'KOGAS',      # 한국가스공사
    '207940':   'SAMSUNGBIO', # 삼성바이오
}

def fetch_chart_data(sym, days=90):
    for q in ('query1', 'query2'):
        try:
            url = f'https://{q}.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}?interval=1d&range={days}d'
            r = SESS.get(url, timeout=8, headers={'Referer':'https://finance.yahoo.com','Accept':'application/json'})
            if not r.ok: continue
            j = r.json()['chart']['result'][0]
            timestamps = j['timestamp']
            closes = j['indicators']['quote'][0]['close']
            pts = []
            for ts, c in zip(timestamps, closes):
                if c is not None and c > 0:  # null 및 0 제거
                    dt = datetime.fromtimestamp(ts, tz=KST).strftime('%m/%d')
                    pts.append({'d': dt, 'v': round(float(c), 2)})
            return pts  # 실제 수신된 데이터만 반환
        except: pass
    return []

print('\n[원자재 차트] 90일 병렬 수집...')
def _fetch_cmdty(item):
    key, sym = item
    return key, fetch_chart_data(sym, 90)
with ThreadPoolExecutor(max_workers=6) as ex:
    for key, pts in ex.map(_fetch_cmdty, CMDTY_CHART_SYMS.items()):
        if pts:
            cmdty_chart[key] = pts
            print(f'  OK  {key}: {len(pts)}일')
        else:
            print(f'  FAIL {key}')

# ─────────────────────────────────────────
# 종목 OHLCV + 기술지표
# ─────────────────────────────────────────
STOCK_TICKERS = {
    '삼성전자':   '005930.KS', 'SK하이닉스':  '000660.KS',
    '삼성바이오': '207940.KS', '한화에어로':  '012450.KS',
    'LIG넥스원':  '079550.KS', '한국전력':    '015760.KS',
    'S-Oil':      '010950.KS', '한국카본':    '008730.KQ',
    '한국가스공사':'036460.KS',
}
stock_charts = {}

def fetch_ohlcv(sym, days=90):
    for q in ('query1', 'query2'):
        try:
            url = f'https://{q}.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}?interval=1d&range={days}d'
            r = SESS.get(url, timeout=10, headers={'Referer':'https://finance.yahoo.com','Accept':'application/json'})
            if not r.ok: continue
            j = r.json()['chart']['result'][0]
            ts_list = j['timestamp']
            qd = j['indicators']['quote'][0]
            ohlcv = []
            for i, ts in enumerate(ts_list):
                o = qd['open'][i] if i < len(qd.get('open',[])) else None
                h = qd['high'][i] if i < len(qd.get('high',[])) else None
                l = qd['low'][i]  if i < len(qd.get('low',[])) else None
                c = qd['close'][i] if i < len(qd.get('close',[])) else None
                v = qd['volume'][i] if i < len(qd.get('volume',[])) else 0
                if all(x is not None and x > 0 for x in [o,h,l,c]):
                    dt = datetime.fromtimestamp(ts, tz=KST).strftime('%m/%d')
                    ohlcv.append({'d':dt,'o':round(float(o)),'h':round(float(h)),'l':round(float(l)),'c':round(float(c)),'v':int(v or 0)})
            if len(ohlcv) < 10: return None
            cl = [x['c'] for x in ohlcv]
            # MA
            def ma(arr, n):
                return [round(sum(arr[max(0,i-n+1):i+1])/min(i+1,n)) if i >= n-1 else None for i in range(len(arr))]
            ma5=ma(cl,5); ma20=ma(cl,20); ma60=ma(cl,60)
            # RSI14
            rsi_arr=[None]*14
            if len(cl)>14:
                g=l2=0
                for i in range(1,15):
                    d=cl[i]-cl[i-1]; g+=max(d,0); l2+=max(-d,0)
                ag,al=g/14,l2/14
                rsi_arr.append(round(100-100/(1+ag/al),1) if al>0 else 100)
                for i in range(15,len(cl)):
                    d=cl[i]-cl[i-1]; ag=(ag*13+max(d,0))/14; al=(al*13+max(-d,0))/14
                    rsi_arr.append(round(100-100/(1+ag/al),1) if al>0 else 100)
            # Bollinger(20)
            bbu=[]; bbl=[]
            for i in range(len(cl)):
                if i<19: bbu.append(None); bbl.append(None)
                else:
                    seg=cl[i-19:i+1]; m=sum(seg)/20; std=(sum((x-m)**2 for x in seg)/20)**0.5
                    bbu.append(round(m+2*std)); bbl.append(round(m-2*std))
            for i,bar in enumerate(ohlcv):
                bar['ma5']=ma5[i]; bar['ma20']=ma20[i]; bar['ma60']=ma60[i]
                bar['rsi']=rsi_arr[i] if i<len(rsi_arr) else None
                bar['bbu']=bbu[i]; bar['bbl']=bbl[i]
            return ohlcv
        except: pass
    return None

print('\n[종목차트] OHLCV + 기술지표 병렬 수집...')
def _fetch_ohlcv_item(item):
    name, ticker = item
    code = ticker.split('.')[0]
    if kis_token:
        data = fetch_kis_ohlcv(code, kis_token, 90)
        if data:
            return name, data, 'KIS'
    data = fetch_ohlcv(ticker, 90)
    return name, data, 'Yahoo'
with ThreadPoolExecutor(max_workers=5) as ex:
    for name, data, src in ex.map(_fetch_ohlcv_item, STOCK_TICKERS.items()):
        if data:
            stock_charts[name] = data
            print(f'  OK  {name}: {len(data)}일  [{src}]')
        else:
            print(f'  FAIL {name}')

# ─────────────────────────────────────────
# FRED API — 미국 경제지표
# ─────────────────────────────────────────
FRED_KEY = os.environ.get('FRED_API_KEY', '')
fred_data = {}

FRED_SERIES = {
    'FED_RATE':  {'id':'FEDFUNDS',        'name':'미국 기준금리',   'unit':'%',  'yoy':False},
    'CPI_YOY':   {'id':'CPIAUCSL',        'name':'미국 CPI(YoY)',  'unit':'%',  'yoy':True},   # 지수 → YoY 계산
    'UNRATE':    {'id':'UNRATE',          'name':'미국 실업률',     'unit':'%',  'yoy':False},
    'GDP_QOQ':   {'id':'GDPC1',            'name':'미국 실질GDP',   'unit':'B$', 'yoy':True },  # YoY 성장률
    'US_PMI':    {'id':'INDPRO',          'name':'미국 산업생산', 'unit':'',   'yoy':True},
    'US10Y':     {'id':'DGS10',           'name':'미국 10Y 국채',  'unit':'%',  'yoy':False},
    'DXY':       {'id':'DTWEXBGS',        'name':'달러인덱스',      'unit':'',   'yoy':False},
    'PCE':       {'id':'PCEPI',           'name':'미국 PCE(YoY)',  'unit':'%',  'yoy':True},   # 지수 → YoY 계산
}

def fetch_fred(series_id, limit=2, yoy=False):
    if not FRED_KEY: return None
    try:
        lim = 14 if yoy else limit   # YoY 계산 시 13개월 필요
        url = (f'https://api.stlouisfed.org/fred/series/observations'
               f'?series_id={series_id}&api_key={FRED_KEY}'
               f'&file_type=json&sort_order=desc&limit={lim}')
        r = SESS.get(url, timeout=8)
        if not r.ok: return None
        obs = r.json().get('observations', [])
        vals = [(o['date'], float(o['value'])) for o in obs if o['value'] != '.']
        if not vals: return None
        if yoy and len(vals) >= 13:
            # YoY = (최신값 - 1년전값) / 1년전값 * 100
            latest_val   = vals[0][1]
            year_ago_val = vals[12][1]
            yoy_pct = round((latest_val - year_ago_val) / year_ago_val * 100, 2) if year_ago_val else 0
            prev_val     = vals[1][1]
            year_ago_prev= vals[13][1] if len(vals) > 13 else year_ago_val
            prev_yoy     = round((prev_val - year_ago_prev) / year_ago_prev * 100, 2) if year_ago_prev else 0
            return [(vals[0][0], yoy_pct), (vals[1][0], prev_yoy)]
        return vals[:2]
    except: return None

print('\n[FRED] 미국 경제지표 병렬 수집...')
def _fetch_fred_item(item):
    key, meta = item
    vals = fetch_fred(meta['id'], yoy=meta.get('yoy', False))
    return key, meta, vals
with ThreadPoolExecutor(max_workers=8) as ex:
    for key, meta, vals in ex.map(_fetch_fred_item, FRED_SERIES.items()):
        if vals:
            latest = vals[0]
            prev   = vals[1] if len(vals) > 1 else None
            chg    = round(latest[1] - prev[1], 2) if prev else 0
            fred_data[key] = {
                'name': meta['name'], 'unit': meta['unit'],
                'val':  round(latest[1], 2), 'date': latest[0],
                'chg':  chg, 'prev': round(prev[1], 2) if prev else None
            }
            print(f'  OK  {key}: {latest[1]}{meta["unit"]} ({latest[0]})')
        else:
            print(f'  SKIP {key} (키 없음)')

# ─────────────────────────────────────────
# 한국은행 OpenAPI — 한국 경제지표
# ─────────────────────────────────────────
BOK_KEY = os.environ.get('BOK_API_KEY', '')
bok_data = {}

BOK_SERIES = {
    # cycle D=일별, M=월별, Q=분기별
    'KR_RATE':  {'stat':'722Y001', 'item':'0101000', 'cycle':'D', 'name':'한국 기준금리',  'unit':'%'},
    'KR_CPI':   {'stat':'901Y009', 'item':'0',       'cycle':'M', 'name':'한국 CPI',      'unit':''},
    'KR_BOP':   {'stat':'301Y013', 'item':'',        'cycle':'M', 'name':'한국 국제수지',  'unit':'백만달러'},
    'KR_M2':    {'stat':'161Y006', 'item':'',       'cycle':'M', 'name':'통화량 M2',     'unit':'십억원'},
}

def fetch_bok(stat_code, item_code, cycle='M'):
    if not BOK_KEY:
        return None
    try:
        if cycle == 'D':
            end   = NOW.strftime('%Y%m%d')
            start = (NOW - timedelta(days=30)).strftime('%Y%m%d')
        elif cycle == 'Q':
            end   = f"{NOW.year}Q{(NOW.month-1)//3+1}"
            start = f"{NOW.year-2}Q1"
        else:  # M
            end   = NOW.strftime('%Y%m')
            start = (NOW - timedelta(days=180)).strftime('%Y%m')
        url = (f'https://ecos.bok.or.kr/api/StatisticSearch/{BOK_KEY}/json/kr/1/10/'
               f'{stat_code}/{cycle}/{start}/{end}'
               + (f'/{item_code}' if item_code else ''))
        r = SESS.get(url, timeout=10)
        if not r.ok: return None
        data = r.json()
        if 'RESULT' in data:
            code = data['RESULT'].get('CODE','')
            print(f'  BOK API 오류 [{stat_code}]: {code} — {data["RESULT"].get("MESSAGE","")[:50]}')
            return None
        rows = data.get('StatisticSearch', {}).get('row', [])
        vals = [(row['TIME'], float(row['DATA_VALUE'])) for row in rows if row.get('DATA_VALUE')]
        return sorted(vals, reverse=True) if vals else None
    except Exception as e:
        err = str(e)
        if '403' in err or 'Proxy' in err or 'proxy' in err:
            return 'BLOCKED'
        return None

print('\n[BOK] 한국 경제지표 수집...')
bok_blocked = False
for key, meta in BOK_SERIES.items():
    if bok_blocked:
        print(f'  SKIP {key} (네트워크 차단)')
        continue
    vals = fetch_bok(meta['stat'], meta['item'], meta.get('cycle','M'))
    if vals == 'BLOCKED':
        bok_blocked = True
        print(f'  SKIP {key} (ecos.bok.or.kr 네트워크 차단 — GitHub Actions IP 제한)')
    elif vals:
        latest = vals[0]
        prev   = vals[1] if len(vals) > 1 else None
        chg    = round(latest[1] - prev[1], 2) if prev else 0
        bok_data[key] = {
            'name': meta['name'], 'unit': meta['unit'],
            'val':  round(latest[1], 2), 'date': latest[0],
            'chg':  chg, 'prev': round(prev[1], 2) if prev else None
        }
        print(f'  OK  {key}: {latest[1]}{meta["unit"]} ({latest[0]})')
    else:
        print(f'  SKIP {key} (데이터 없음)')
    time.sleep(0.1)

# ─────────────────────────────────────────
# 섹터 히트맵 — 섹터 ETF 시세
# ─────────────────────────────────────────
SECTOR_ETFS = {
    '반도체':   {'sym':'091160.KS', 'etf':'KODEX 반도체'},
    '2차전지':  {'sym':'305720.KS', 'etf':'KODEX 2차전지산업'},
    '방산':     {'sym':'459580.KS', 'etf':'KODEX K-방산'},
    '바이오':   {'sym':'244580.KS', 'etf':'KODEX 바이오'},
    '자동차':   {'sym':'091180.KS', 'etf':'KODEX 자동차'},
    '금융':     {'sym':'091170.KS', 'etf':'KODEX 은행'},
    '에너지':   {'sym':'102970.KS', 'etf':'KODEX 에너지화학'},
    'IT':       {'sym':'153130.KS', 'etf':'KODEX IT'},
    '건설':     {'sym':'104520.KS', 'etf':'KODEX 건설'},
    '소비재':   {'sym':'228790.KS', 'etf':'KODEX 필수소비재'},
}

sector_data = {}

def fetch_sector_price(sym):
    for q in ('query1','query2'):
        try:
            url = f'https://{q}.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}?interval=1d&range=5d'
            r = SESS.get(url, timeout=8, headers={'Referer':'https://finance.yahoo.com'})
            if not r.ok: continue
            res = r.json()['chart']['result'][0]
            closes = [c for c in res['indicators']['quote'][0].get('close',[]) if c]
            if len(closes) >= 2:
                prev = closes[-2]; curr = closes[-1]
                return {'price': round(curr), 'chg': round((curr-prev)/prev*100, 2)}
        except: pass
    return None

print('\n[섹터] 히트맵 병렬 수집...')
def _fetch_sector_item(item):
    sector, meta = item
    return sector, meta, fetch_sector_price(meta['sym'])
with ThreadPoolExecutor(max_workers=10) as ex:
    for sector, meta, d in ex.map(_fetch_sector_item, SECTOR_ETFS.items()):
        if d:
            sector_data[sector] = {**d, 'etf': meta['etf']}
            print(f'  OK  {sector}: {d["price"]} ({d["chg"]:+.2f}%)')
        else:
            print(f'  FAIL {sector}')

# ─────────────────────────────────────────
# 2. 뉴스 수집
# ─────────────────────────────────────────

# ── 국내 RSS: 경제·증시·기업실적·정부정책 전문 피드
KR_RSS = [
    # ── 한국경제 (4개)
    ('https://www.hankyung.com/feed/economy',       '한국경제',    'tk'),
    ('https://www.hankyung.com/feed/finance',       '한경증권',    'tk'),
    ('https://www.hankyung.com/feed/it',            '한경IT',      'te'),
    ('https://www.hankyung.com/feed/international', '한경국제',    'tk'),
    # ── 매일경제 (4개) — 올바른 섹션 URL로 교체
    ('https://www.mk.co.kr/rss/30100041/',          '매경경제',    'tk'),  # 경제
    ('https://www.mk.co.kr/rss/50200011/',          '매경증권',    'tk'),  # 증권 (수정)
    ('https://www.mk.co.kr/rss/30300018/',          '매경국제',    'tk'),  # 국제
    # ── 서울경제
    ('https://www.sedaily.com/RSS/Economy',         '서울경제',    'tk'),
    # ── 연합뉴스 (정부발표·속보)
    ('https://www.yna.co.kr/rss/economy.xml',       '연합뉴스',    'tk'),
    # ── 전자신문 (IT·반도체)
    ('https://rss.etnews.com/Section902.xml',       '전자신문',    'te'),
    # ── 아시아경제
    ('https://www.asiae.co.kr/rss/economy.htm',     '아시아경제',  'tk'),
    # ── 이데일리
    ('https://rss.etoday.co.kr/eto/market_news.xml',      '이투데이마켓', 'tk'),
    ('https://rss.etoday.co.kr/eto/finance_news.xml',     '이투데이금융', 'tk'),
    ('https://rss.etoday.co.kr/eto/economy_news.xml',     '이투데이경제', 'tk'),
    # ── 뉴스핌 (증권·금융·글로벌 수급 특화)
    ('http://rss.newspim.com/news/category/105',    '뉴스핌증권',  'tk'),  # 증권·금융
    ('http://rss.newspim.com/news/category/103',    '뉴스핌경제',  'tk'),  # 경제
    ('http://rss.newspim.com/news/category/107',    '뉴스핌글로벌','tk'),  # 글로벌
    # ── 파이낸셜뉴스 (금융·산업)
    ('http://www.efnews.co.kr/rss/S1N4.xml',        '파이낸셜금융','tk'),  # 금융
    ('http://www.efnews.co.kr/rss/S1N3.xml',        '파이낸셜산업','tk'),  # 산업
]

# ── 국내 Google News: 경제·기업·정부정책 키워드
KR_GNEWS = [
    ('한국 기준금리 금통위 한국은행 통화정책',    'tk'),
    ('삼성전자 SK하이닉스 반도체 실적 수출',      'te'),
    ('한화에어로스페이스 방산 수출 LIG넥스원',    'tk'),
    ('한국 수출 무역수지 경상수지 경제성장',      'tk'),
    ('기업 실적 영업이익 매출 분기 발표',         'tk'),
    ('정부 정책 규제 산업부 기재부 발표',         'tk'),
    ('외국인 기관 순매수 수급 KOSPI KOSDAQ',     'tk'),
    ('원달러 환율 외환 달러 강세 약세',           'tk'),
    ('LNG 에너지 유가 정유 원유 천연가스',        'tn'),
    ('반도체 AI 데이터센터 전력 배터리',          'te'),
]

# ── 해외 RSS: 경제·금융·국제정세 전문 매체
GL_RSS = [
    ('https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114', 'CNBC Economy','미국',  'tm'),
    ('https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069',  'CNBC Markets','미국',  'tm'),
    ('https://feeds.marketwatch.com/marketwatch/topstories/',                                'MarketWatch', '미국',  'tm'),
    ('https://feeds.a.dj.com/rss/RSSMarketsMain.xml',                                       'WSJ Markets', '미국',  'te'),
    ('https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml',                                     'WSJ Business','미국',  'te'),
    ('https://feeds.bloomberg.com/markets/news.rss',                                        'Bloomberg',   '글로벌','te'),
    ('https://feeds.bbci.co.uk/news/business/rss.xml',                                      'BBC Business','유럽',  'te'),
    ('https://feeds.bbci.co.uk/news/world/rss.xml',                                         'BBC World',   '국제',  'te'),
    ('https://www.ft.com/rss/home',                                                         'FT',          '유럽',  'te'),  # 수정
    ('https://www.aljazeera.com/xml/rss/all.xml',                                           'Al Jazeera',  '중동',  'tw'),
    ('https://www.middleeasteye.net/rss',                                                   'ME Eye',      '중동',  'tw'),
    ('https://www.timesofisrael.com/feed/',                                                 'ToI',         '중동',  'tw'),
    ('https://www.scmp.com/rss/91/feed',                                                    'SCMP',        '아시아','tg'),
    ('https://asia.nikkei.com/rss/feed/nar',                                                'Nikkei Asia', '아시아','tg'),
    ('https://economictimes.indiatimes.com/rssfeedstopstories.cms',                         'ET India',    '아시아','tg'),
]

# ── 해외 Google News: 경제·국제정세 핵심 키워드
GL_GNEWS = [
    ('Iran war oil Hormuz Middle East attack sanctions',     '중동',  'tw'),
    ('crude oil WTI Brent OPEC production cut supply',       '에너지','tn'),
    ('Federal Reserve interest rate inflation CPI jobs',     '경제',  'te'),
    ('China economy trade war tariff export GDP growth',     '중국',  'tg'),
    ('US treasury yield bond dollar index DXY',              '금융',  'tm'),
    ('corporate earnings revenue profit outlook quarterly',  '기업',  'tm'),
    ('geopolitical risk sanctions conflict government policy','국제',  'te'),
    ('semiconductor AI chip demand supply TSMC NVIDIA',      'IT',    'te'),
]

# ── 필터링: 비경제·비정세 제외 키워드
BLACKLIST_KW = [
    # ── 연예·방송·문화
    '연예','드라마','영화','K-pop','아이돌','가수','배우','뮤지컬','공연','콘서트',
    '예능','오디션','시상식','팬미팅','컴백','데뷔','앨범','뮤직비디오',
    '유튜버','인플루언서','틱톡','팬덤','굿즈',
    'BTS','아이유','블랙핑크','손흥민','류현진','오타니',
    # ── 스포츠
    '스포츠','축구','야구','농구','골프','올림픽','월드컵',
    '경기결과','우승','준우승','득점','홈런','MVP','국가대표',
    # ── 사건사고·사회면
    '살인','화재','지진','홍수','재난','범죄','마약',
    '이혼','결혼','열애','결별','열애설','불륜','스캔들',
    '실종','익사','충돌','붕괴','침수',
    '체포','구속','기소','수감','출소','가석방',
    '성범죄','폭행','협박',
    # ── 투자무관 정치
    '탄핵','대선','총선','당대표','여당','야당',
    # ── 외국 정치·사회
    '최고지도자','동성애','동성혼','성소수자','LGBT',
    '교황','성직자','왕실','왕자','공주',
    '납치','인질','포로','테러범',
    # ── 군사 르포 (방산기업·정책 아닌 현장묘사)
    '항공모함','선실','잠자는 군인','병사들','군인들',
    # ── 동물·자연 잡뉴스
    '곰 습격','아찔','목격담','깜짝 포착','깜짝 등장',
    '강아지','고양이','반려동물','펫',
    # ── 건강·미용·생활
    '다이어트','성형','피부미용','건강식품',
    '생리','출산휴가','육아','임산부','보육',
    '별자리','운세','타로','풍수','귀신','UFO',
    '맛집','여행','관광','숙박','항공권','요리','레시피',
    # ── 젠더·사회 이슈 (경제무관)
    '젠더','페미','혐오','성차별',
    # ── 홍보성·협회
    '홈쇼핑','협회장','판로','지원사업','박람회','전시회',
    '수상','퇴임','인사발령','감사패','고향사랑기부제',
    # ── 영문 비경제
    'celebrity','entertainment','sports','football','basketball','cricket',
    'hollywood','oscar','grammy','divorce','wedding','scandal','concert',
    'BTS','blackpink','kpop',
    'maternity','menstrual','gender','court ruling','verdict','lifestyle',
    'gay','lesbian','transgender','lgbtq','same-sex','homosexual',
]

def is_relevant(title):
    """경제·기업실적·정부정책·국제정세 관련 뉴스만 통과"""
    t_lower = title.lower()
    for kw in BLACKLIST_KW:
        if kw.lower() in t_lower:
            return False
    return True

def parse_rss(url, src, tag, tc, max_items=4):
    r = safe_get(url, timeout=8)
    if not r: return []
    try:
        root = ET.fromstring(r.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        items = root.findall('.//item') or root.findall('.//atom:entry', ns)
        out = []
        for it in items[:max_items]:
            title = (it.findtext('title') or it.findtext('atom:title', namespaces=ns) or '').strip()
            link  = (it.findtext('link')  or it.findtext('atom:link', namespaces=ns) or '').strip()
            pub   = it.findtext('pubDate') or it.findtext('dc:date') or ''
            title = htmlmod.unescape(title)
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(pub)
            except:
                try: dt = datetime.fromisoformat(pub[:19])
                except: dt = NOW
            out.append({'title': title, 'link': link, 'src': src, 'tag': tag, 'tc': tc, 'time': fmt_time(dt), 'stamp': dt.timestamp() if hasattr(dt,'timestamp') else 0})
        return out
    except Exception as e:
        print(f'  RSS parse fail {url[:50]}: {e}')
        return []

def fetch_gnews(query, tag, tc, lang='ko', gl='KR', max_items=5):
    q = requests.utils.quote(query)
    ceid = f'{gl}:{lang}'
    url = f'https://news.google.com/rss/search?q={q}&hl={lang}&gl={gl}&ceid={ceid}'
    return parse_rss(url, 'Google뉴스', tag, tc, max_items)

def fetch_naver_news():
    """네이버 금융 증시뉴스 scraping"""
    out = []
    urls = [
        ('https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258', '주식'),
        ('https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=261', '투자'),
    ]
    for url, cat in urls:
        r = safe_get(url, referer='https://finance.naver.com')
        if not r: continue
        try:
            soup = BeautifulSoup(r.content, 'html.parser')
            for a in soup.select('.articleSubject a, .articleTitle a')[:5]:
                title = a.get_text(strip=True)
                href  = a.get('href','')
                if href.startswith('/'): href = 'https://finance.naver.com' + href
                if title:
                    out.append({'title': title, 'link': href, 'src': f'네이버금융({cat})', 'tag': '한국', 'tc': 'tk', 'time': '방금', 'stamp': NOW.timestamp()})
        except Exception as e:
            print(f'  Naver news parse fail: {e}')
    return out

def fetch_daum_news():
    """다음 금융 뉴스 — API 불안정으로 Google News 대체"""
    return []  # GL_GNEWS/KR_GNEWS로 커버

print(f'\n[뉴스] 수집 시작...')

# 국내뉴스
kr_news = []
seen_kr = set()
# RSS
def _fetch_kr_rss(item):
    url, src, tc = item
    return parse_rss(url, src, '한국', tc, 4)
with ThreadPoolExecutor(max_workers=10) as ex:
    for items in ex.map(_fetch_kr_rss, KR_RSS):
        for it in items:
            if not is_relevant(it['title']): continue
            k = it['title'][:20]
            if k not in seen_kr: seen_kr.add(k); kr_news.append(it)
# Google News
def _fetch_kr_gnews(item):
    q, tc = item
    return fetch_gnews(q, '한국', tc, 'ko', 'KR', 5)
with ThreadPoolExecutor(max_workers=8) as ex:
    for items in ex.map(_fetch_kr_gnews, KR_GNEWS):
        for it in items:
            if not is_relevant(it['title']): continue
            k = it['title'][:20]
            if k not in seen_kr: seen_kr.add(k); kr_news.append(it)
# 네이버
for it in fetch_naver_news():
    if not is_relevant(it['title']): continue
    k = it['title'][:20]
    if k not in seen_kr: seen_kr.add(k); kr_news.append(it)
# 다음
for it in fetch_daum_news():
    if not is_relevant(it['title']): continue
    k = it['title'][:20]
    if k not in seen_kr: seen_kr.add(k); kr_news.append(it)

# ─────────────────────────────────────────
# 키워드 가중치 선별
# ─────────────────────────────────────────
KW_TIER1 = ['삼성전자','SK하이닉스','LG에너지솔루션','삼성바이오로직스',
             '현대차','기아','셀트리온','POSCO홀딩스','KB금융','신한지주']
KW_TIER2 = ['한화에어로스페이스','LIG넥스원','현대로템','한국항공우주',
             '두산에너빌리티','HD현대일렉트릭','고려아연','LS','KMW','에코프로']
KW_TIER3 = ['유가','원유','WTI','브렌트','환율','달러','원달러',
             '금','구리','LNG','이란','중동','OPEC']
# 경제·실적·정책 보너스
KW_ECON  = ['실적','영업이익','매출','순이익','분기','반기','연간','잠정',
             '기준금리','금통위','한국은행','수출','무역수지','경상수지',
             '정부','정책','규제','발표','공시','계약','수주','투자','인수']

def score_news(item):
    t = item.get('title','')
    s = item.get('stamp', 0)
    for kw in KW_TIER1:
        if kw in t: s += 7200; break
    for kw in KW_TIER2:
        if kw in t: s += 5400; break
    for kw in KW_TIER3:
        if kw in t: s += 3600; break
    for kw in KW_ECON:
        if kw in t: s += 1800; break
    return s

kr_news.sort(key=score_news, reverse=True)
kr_news = kr_news[:10]
print(f'  국내뉴스: {len(kr_news)}건 (키워드 가중치 적용)')

# 해외뉴스
# 해외뉴스 — 필터 + 관련도 점수 정렬
gl_news = []
seen_gl = set()

# 해외 경제·정세 관련 키워드 (가중치)
GL_SCORE_KW = [
    ('oil','brent','wti','opec','crude','energy','gas','iran','hormuz'),  # 에너지/중동
    ('fed','rate','inflation','cpi','gdp','recession','economy','treasury'),  # 미국경제
    ('china','trade','tariff','sanction','war','conflict','geopolit'),  # 지정학
    ('earnings','revenue','profit','ipo','merger','acquisition'),  # 기업실적
    ('semiconductor','chip','ai','nvidia','tsmc','korea','samsung'),  # IT
]

def score_gl(item):
    t = item['title'].lower()
    s = item.get('stamp', 0)
    for tier, kws in enumerate(GL_SCORE_KW):
        for kw in kws:
            if kw in t:
                s += (5 - tier) * 1800  # 에너지>경제>지정학>기업>IT 순 가중
                break
    return s

def _fetch_gl_rss(item):
    url, src, tag, tc = item
    return parse_rss(url, src, tag, tc, 3)
with ThreadPoolExecutor(max_workers=10) as ex:
    for items in ex.map(_fetch_gl_rss, GL_RSS):
        for it in items:
            if not is_relevant(it['title']): continue
            k = it['title'][:20]
            if k not in seen_gl: seen_gl.add(k); gl_news.append(it)
def _fetch_gl_gnews(item):
    q, tag, tc = item
    return fetch_gnews(q, tag, tc, 'en', 'US', 5)
with ThreadPoolExecutor(max_workers=8) as ex:
    for items in ex.map(_fetch_gl_gnews, GL_GNEWS):
        for it in items:
            if not is_relevant(it['title']): continue
            k = it['title'][:20]
            if k not in seen_gl: seen_gl.add(k); gl_news.append(it)

gl_news.sort(key=score_gl, reverse=True)
gl_news = gl_news[:10]
print(f'  해외뉴스: {len(gl_news)}건 (관련도+최신 정렬)')

# 해외뉴스 제목 한글 번역 (Haiku)# ─────────────────────────────────────────
# 3. DART 공시 수집
# ─────────────────────────────────────────
print(f'\n[공시] DART 수집...')
dart_items = []

def fetch_dart_list():
    """OpenDART API - 최근 5영업일 공시"""
    today = NOW.strftime('%Y%m%d')
    url = f'https://opendart.fss.or.kr/api/list.json?crtfc_key={DART_KEY}&bgn_de={DATA_BGN_DATE}&end_de={today}&page_count=40'
    r = safe_get(url)
    if not r: return []
    try:
        data = r.json()
        if data.get('status') != '000':
            print(f'  DART API status: {data.get("status")} {data.get("message","")}')
            # 당일 공시가 없으면 최근 3일로 확장
            bgn = (NOW - timedelta(days=3)).strftime('%Y%m%d')
            url2 = f'https://opendart.fss.or.kr/api/list.json?crtfc_key={DART_KEY}&bgn_de={bgn}&end_de={today}&page_count=30'
            r2 = safe_get(url2)
            if not r2: return []
            data = r2.json()
        items = data.get('list', [])
        out = []
        for it in items:
            rcept_no = it.get('rcept_no','')
            link = f'https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}' if rcept_no else ''
            out.append({
                'title': it.get('report_nm',''),
                'corp':  it.get('corp_name',''),
                'date':  it.get('rcept_dt','')[:8] if it.get('rcept_dt') else '',
                'type':  it.get('pblntf_ty',''),
                'link':  link,
            })
        return out
    except Exception as e:
        print(f'  DART parse fail: {e}')
        return []

def fetch_krx_kind():
    """KRX KIND RSS - 공식 공시"""
    r = safe_get('https://kind.krx.co.kr/rss/todaydisclosure.do')
    if not r: return []
    try:
        root = ET.fromstring(r.content)
        out = []
        for item in root.findall('.//item')[:20]:
            title  = (item.findtext('title') or '').strip()
            link   = (item.findtext('link')  or '').strip()
            desc   = item.findtext('description') or ''
            # corp name from description
            corp = ''
            m = re.search(r'\[(.+?)\]', desc or title)
            if m: corp = m.group(1)
            out.append({'title': title, 'corp': corp, 'date': TS_SHORT, 'type': 'KRX', 'link': link})
        return out
    except Exception as e:
        print(f'  KIND RSS fail: {e}')
        return []

dart_items = fetch_dart_list() if DART_KEY else []
if not dart_items:
    if not DART_KEY:
        print('  DART_API_KEY 없음 — KIND RSS fallback')
    dart_items = fetch_krx_kind()
# 키워드 가중치 적용 후 상위 10건
def score_dart(item):
    t = item.get('title','') + item.get('corp','')
    s = 0
    for kw in KW_TIER1:
        if kw in t: s += 7200; break
    for kw in KW_TIER2:
        if kw in t: s += 5400; break
    return s
dart_items.sort(key=score_dart, reverse=True)
dart_items = dart_items[:10]
print(f'  공시: {len(dart_items)}건 (키워드 가중치 적용)')

# ─────────────────────────────────────────
# 4. 리서치 리포트 수집 (네이버 금융 통합)
# ─────────────────────────────────────────
print(f'\n[리서치] 네이버 금융 리서치 수집...')
research_items = []

FIRM_MAP = {
    '삼성': '삼성증권', '미래에셋': '미래에셋', '키움': '키움증권',
    '한국투자': '한국투자증권', '한투': '한국투자증권',
    '신한': '신한투자증권', '대신': '대신증권',
    'NH': 'NH투자증권', 'KB': 'KB증권', '하나': '하나증권',
    '유안타': '유안타증권', 'SK': 'SK증권', '메리츠': '메리츠증권',
    '유진': '유진투자증권', '현대차': '현대차증권',
}

def parse_firm(name):
    for k, v in FIRM_MAP.items():
        if k in name: return v
    return name[:10] if name else '증권사'

def fetch_naver_research():
    """네이버 금융 리서치 - 기업분석 리포트"""
    pages = [
        'https://finance.naver.com/research/company_list.naver',
        'https://finance.naver.com/research/invest_list.naver',
    ]
    out = []
    for url in pages:
        r = safe_get(url, referer='https://finance.naver.com')
        if not r: continue
        try:
            soup = BeautifulSoup(r.content, 'html.parser')
            rows = soup.select('table.type_1 tr, table.tbl_type tr')
            for row in rows[:25]:
                tds = row.select('td')
                if len(tds) < 3: continue
                # 종목명, 리포트제목, 증권사, 날짜
                title_td = tds[1] if len(tds) > 2 else tds[0]
                title_a  = title_td.select_one('a')
                if not title_a: continue
                title = title_a.get_text(strip=True)
                href  = title_a.get('href','')
                if href.startswith('/'): href = 'https://finance.naver.com' + href
                stock = tds[0].get_text(strip=True) if tds else ''
                firm  = tds[2].get_text(strip=True) if len(tds)>2 else ''
                date  = tds[-1].get_text(strip=True) if tds else ''
                if title and len(title) > 3:
                    out.append({
                        'title': title,
                        'stock': stock,
                        'firm':  parse_firm(firm),
                        'date':  date,
                        'link':  href,
                    })
        except Exception as e:
            print(f'  Research parse fail {url}: {e}')
        time.sleep(0.2)
    return out[:30]

research_items = fetch_naver_research()
print(f'  리서치: {len(research_items)}건')

# ─────────────────────────────────────────
# 5. Claude AI 분석
# ─────────────────────────────────────────
print(f'\n[AI] Claude 분석...')
ai_sections = {'full': '', 'bias': '', 'forecast': '', 'picks': '', 'risk': ''}
ai_ts = ''
global_issues = []
domestic_issues = []

def parse_issues_json(text):
    clean = re.sub(r'^```(?:json)?', '', text.strip()).rstrip('`').strip()
    # [ ] 배열 추출
    s1, s2 = clean.find('['), clean.rfind(']')
    if s1 >= 0 and s2 > s1:
        clean = clean[s1:s2+1]
    else:
        # 배열 없으면 전체로 시도
        pass
    # 1차 파싱
    try:
        result = json.loads(clean)
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict) and x.get('title')]
    except:
        pass
    # 2차: 마지막 완전한 객체까지 복구
    lc = clean.rfind('},')
    if lc > 0:
        try:
            result = json.loads(clean[:lc+1]+']')
            if isinstance(result, list):
                return [x for x in result if isinstance(x, dict) and x.get('title')]
        except:
            pass
    # 3차: 중간 잘린 마지막 객체 제거
    lc2 = clean.rfind('{')
    if lc2 > 0:
        try:
            result = json.loads(clean[:lc2].rstrip(',').rstrip() + ']')
            if isinstance(result, list):
                return [x for x in result if isinstance(x, dict) and x.get('title')]
        except:
            pass
    print(f'  parse_issues_json 실패 (raw 앞 200자): {clean[:200]}')
    return []


# Groq TPM 슬라이딩 윈도우 (무료 티어 90% = 5,400 TPM 목표)
# Groq는 '지난 60초 누적 토큰' 기준으로 측정 (슬라이딩 윈도우)
# TokenBucket(충전 속도 기반)은 이 구조와 맞지 않아 429 발생 → 슬라이딩 윈도우로 교체
import collections as _col

class _SlidingWindow:
    # Dev Tier 98% 목표
    # llama-3.3-70b: 300,000 TPM × 98% = 294,000
    # llama-3.1-8b:  200,000 TPM × 98% = 196,000
    TPM_70B   = 294000
    TPM_8B    = 196000
    WINDOW    = 60.0    # 슬라이딩 윈도우 60초

    def __init__(self):
        self._history_70b = []  # [(abs_time, tokens), ...]
        self._history_8b  = []

    def consume(self, tokens, model='70b'):
        import time as _t
        history  = self._history_70b if '70b' in model else self._history_8b
        tpm_limit = self.TPM_70B    if '70b' in model else self.TPM_8B
        while True:
            now = _t.time()
            # 윈도우 밖 항목 제거
            history[:] = [(ts, tok) for ts, tok in history if ts > now - self.WINDOW]
            recent = sum(tok for _, tok in history)
            if recent + tokens <= tpm_limit:
                history.append((now, tokens))
                return
            oldest = min(ts for ts, _ in history)
            wait = oldest + self.WINDOW - now + 0.5
            print(f'    [TPM-{model}] {recent:,}+{tokens:,}>{tpm_limit:,} → {wait:.1f}초 대기')
            _t.sleep(wait)

    def reset(self):
        self._history_70b.clear()
        self._history_8b.clear()

_bucket = _SlidingWindow()

def call_groq(model, system, user, max_tokens=3000):
    """Groq API 전용 호출 — TokenBucket으로 TPM 90% 유지"""
    if not GROQ_KEY:
        raise Exception('No GROQ_API_KEY')
    # 토큰 추정: 입력(chars/4) + 출력(max_tokens) → 버킷 소비
    est_tokens = len(system) // 4 + len(user) // 4 + max_tokens
    _bucket.consume(est_tokens, model)
    resp = requests.post(
        'https://api.groq.com/openai/v1/chat/completions',
        headers={'Authorization': f'Bearer {GROQ_KEY}', 'Content-Type': 'application/json'},
        json={
            'model': model,
            'max_tokens': max_tokens,
            'messages': [
                {'role': 'system', 'content': system},
                {'role': 'user',   'content': user},
            ],
        },
        timeout=120,
    )
    if resp.status_code == 429:
        retry_after = int(resp.headers.get('retry-after', 30))
        print(f'    Groq 429 — {retry_after}초 대기 후 버킷 리셋')
        time.sleep(retry_after)
        _bucket.reset()  # 슬라이딩 윈도우 초기화
        return ''  # 이번 호출은 건너뜀
    if not resp.ok:
        raise Exception(f'Groq HTTP {resp.status_code}: {resp.text[:200]}')
    raw = resp.json()['choices'][0]['message']['content'].strip()
    # 실제 토큰 사용량 로그 (슬라이딩 윈도우는 est_tokens 기준 예약 유지)
    usage = resp.json().get('usage', {})
    actual = usage.get('total_tokens', 0)
    if actual > 0:
        pass  # 실제값 확인용 — 슬라이딩 윈도우는 예약 기반으로 유지
    return re.sub(r'^```(?:json)?', '', raw).rstrip('`').strip()

# alias — 기존 call_claude/call_ai 호출 호환
def call_ai(model, system, user, max_tokens=3000):
    groq_model = GROQ_MODEL_HIGH if 'sonnet' in model else GROQ_MODEL_FAST
    return call_groq(groq_model, system, user, max_tokens)

def call_claude(model, system, user, max_tokens=3000):
    return call_ai(model, system, user, max_tokens)

def extract_json_array(text):
    """JSON 배열/객체 모두 처리 — 자동 복구 6단계.
    처리: 코드블록, // 주석, trailing comma, 작은따옴표,
          제어문자, 불완전 배열, 객체→배열 변환, 객체단위 파싱.
    """
    if not text:
        return None

    def _try(s):
        try: return json.loads(s)
        except: return None

    def _clean(s):
        s = re.sub(r'```(?:json)?\s*', '', s).strip().rstrip('`').strip()
        s = re.sub(r'//[^\n]*', '', s)          # // 주석 제거
        s = re.sub(r',\s*([}\]])', r'\1', s)   # trailing comma
        s = re.sub(r'[\x00-\x1f\x7f]', ' ', s) # 제어문자
        return s

    def _clean_quotes(s):
        # JSON string 내부 작은따옴표 → 제거 ('text' → text)
        return re.sub(r"'([^']*?)'", r'\1', s)

    # 1단계: 원본 직접 파싱
    r = _try(text)
    if isinstance(r, list): return r
    if isinstance(r, dict): pass  # 5단계에서 처리

    # 2단계: 기본 클리닝
    t = _clean(text)
    r = _try(t)
    if isinstance(r, list): return r

    # 3단계: 작은따옴표 처리
    t2 = _clean_quotes(t)
    r = _try(t2)
    if isinstance(r, list): return r

    # 4단계: 배열 구간 추출 후 파싱
    for src in (t2, t, text):
        s1, s2 = src.find('['), src.rfind(']')
        if s1 >= 0 and s2 > s1:
            r = _try(src[s1:s2+1])
            if isinstance(r, list): return r
            # 불완전 배열 복구
            cand = src[s1:s2+1]
            lb = cand.rfind('},')
            if lb > 0:
                r = _try(cand[:lb+1] + ']')
                if isinstance(r, list): return r
        # [ 있고 ] 없는 불완전 배열
        if s1 >= 0 and s2 < s1:
            cand = src[s1:]
            for sep in ('},', '}'):
                lb = cand.rfind(sep)
                if lb > 0:
                    r = _try(cand[:lb+1] + ']')
                    if isinstance(r, list): return r

    # 5단계: 객체 형태 → 배열 변환
    for src in (t2, t):
        o1, o2 = src.find('{'), src.rfind('}')
        if o1 >= 0 and o2 > o1:
            obj = _try(src[o1:o2+1])
            if isinstance(obj, dict):
                result = []
                for k, v in obj.items():
                    if isinstance(v, dict):
                        item = {'name': k}; item.update(v); result.append(item)
                    elif isinstance(v, list):
                        result.extend(v)
                if result: return result
                return [obj]

    # 6단계: 마지막 수단 — 객체 단위 개별 파싱
    results = []
    for src in (t2, t):
        for m in re.finditer(r'\{[^{}]+\}', src, re.DOTALL):
            r = _try(m.group())
            if r and isinstance(r, dict) and len(r) >= 2:
                results.append(r)
        if results: return results

    return None
def translate_titles(items):
    to_tr = [(i, n) for i, n in enumerate(items) if not re.search(r'[가-힣]', n['title'])]
    if not to_tr or not GROQ_KEY: return
    try:
        titles_str = '\n'.join([f"{i+1}. {n['title']}" for i, (_, n) in enumerate(to_tr)])
        result = call_claude('claude-haiku-4-5-20251001',
            '영문 뉴스 제목을 한국어로 번역. 번호 유지, 번역문만 출력.',
            f'번역:\n{titles_str}', 1000)
        for line in result.strip().split('\n'):
            m = re.match(r'^(\d+)\.\s*(.+)', line.strip())
            if m:
                idx = int(m.group(1)) - 1
                if 0 <= idx < len(to_tr):
                    orig_idx = to_tr[idx][0]
                    items[orig_idx]['titleKo'] = m.group(2).strip()
        print(f'  해외뉴스 제목 번역: {len(to_tr)}건')
    except Exception as e:
        print(f'  번역 FAIL: {e}')

if GROQ_KEY and AI_FULL:
    translate_titles(gl_news)


if GROQ_KEY and AI_PARTIAL:
    # 시세 요약
    price_str = ' | '.join([f"{k}:{fmt_price(v['p'],k)}({'+' if v['c']>=0 else ''}{v['c']:.2f}%)" for k,v in PRICE_DATA.items()])
    # 뉴스 요약 (상위 30개)
    kr_str = '\n'.join([f"{i+1}. [{n['src']}] {n['title']}" for i,n in enumerate(kr_news[:20])]) or '뉴스 없음'
    kr_str_step1 = '\n'.join([f"{i+1}. [{n['src']}] {n['title']}" for i,n in enumerate(kr_news[:15])]) or '뉴스 없음'
    gl_str = '\n'.join([f"{i+1}. [{n['tag']}/{n['src']}] {n['title']}" for i,n in enumerate(gl_news[:15])])
    # 공시 요약
    dart_str = '\n'.join([f"{d['corp']}: {d['title']}" for d in dart_items[:10]]) or '없음'
    # 리서치 요약
    research_str = '\n'.join([f"{r['firm']} - {r['title']}({r['stock']})" for r in research_items[:10]]) or '없음'

    combined_prompt = (
        f'[분석기준] {ANALYSIS_BASE}\n\n'
        f'[시세]\n{price_str}\n\n'
        f'[국내뉴스]\n{kr_str}\n\n'
        f'[해외뉴스]\n{gl_str}\n\n'
        f'[공시]\n{dart_str}\n\n'
        f'[리서치]\n{research_str}'
    )

    # 5-1. AI 종합분석 (4섹션)
    try:
        print('  [1/3] 종합분석...')
        t = call_claude(
            'claude-sonnet-4-20250514',
            '한국 증시 전문 애널리스트 겸 글로벌 매크로 전략가. 한국어. 문어체로 작성. 존댓말 사용 금지. 각 섹션 최소 3~5문장 이상 구체적으로 작성.',
            combined_prompt + '\n\n아래 4개 섹션을 각각 구체적 수치와 근거를 포함해 상세히 분석:\n\n'
            '### BIAS\n'
            '뉴스 편향/오류 분석: 어떤 뉴스가 과장되거나 축소됐는지, 놓친 관점은 무엇인지, 시장이 어떤 부분을 오독하고 있는지 분석\n\n'
            '### FORECAST\n'
            '시장 전망: KOSPI/KOSDAQ 단기(1주) 방향성, 주요 지지/저항선, 관심 섹터, 환율 및 유가 전망, 다음 주 주요 이벤트\n\n'
            '### PICKS\n'
            '추천종목 3~5개: 각 종목별 추천 이유, 진입가격대, 목표가, 손절가 명시\n\n'
            '### RISK\n'
            '핵심 리스크: 상위 3개 리스크 요인, 각 리스크의 발생 가능성(%)과 시장 충격 크기, 대응 전략',
            4000
        )
        parts = re.split(r'###\s*', t)
        full_html = ''
        SECTION_ICONS = {'BIAS':'🔍','FORECAST':'📈','PICKS':'🎯','RISK':'⚠️'}
        SECTION_LABELS = {'BIAS':'뉴스 편향/오류 분석','FORECAST':'시장 전망','PICKS':'추천 종목','RISK':'핵심 리스크 경고'}
        for p in parts:
            if not p.strip(): continue
            lines = p.strip().split('\n')
            header = lines[0].strip()
            body   = '\n'.join(lines[1:]).strip()
            # 헤더 아이콘 및 라벨 매핑
            hkey = header.upper().split()[0] if header else ''
            icon  = SECTION_ICONS.get(hkey, '📋')
            label = SECTION_LABELS.get(hkey, header)
            # 본문 포맷팅
            body = body.replace('**','').replace('*','')
            body = re.sub(r'^- ', '• ', body, flags=re.MULTILINE)
            # 단락 구분 — 빈줄 기준으로 <p> 태그 생성
            paras = [pp.strip() for pp in body.split('\n\n') if pp.strip()]
            body_html = ''.join(f'<p>{HE(pp)}</p>' for pp in paras) if paras else HE(body)
            section_html = (
                f'<div class="ai-section">'
                f'<h3>{icon} {HE(label)}</h3>'
                f'<div class="ai-body">{body_html}</div>'
                f'</div>\n'
            )
            full_html += section_html
            first_line = header.lower()
            if 'bias' in first_line or '편향' in first_line:
                ai_sections['bias']     = body_html
            elif 'forecast' in first_line or '전망' in first_line:
                ai_sections['forecast'] = body_html
            elif 'picks' in first_line or '추천' in first_line:
                ai_sections['picks']    = body_html
            elif 'risk' in first_line or '리스크' in first_line:
                ai_sections['risk']     = body_html
        ai_sections['full'] = full_html
        ai_ts = TS_SHORT
        print(f'  종합분석 OK ({len(full_html)} chars)')
    except Exception as e:
        print(f'  종합분석 FAIL: {e}')

    # 5-2. 이슈분석 (글로벌 5개)
    issue_prompt = (
        '각 이슈는 JSON 객체: {"title":"제목","impact":"상/중/하","summary":"2문장요약",'
        '"scenarioA":"낙관시나리오+확률","scenarioB":"비관시나리오+확률",'
        '"drivers":["변수1","변수2","변수3"],"action":"투자시사점"}\n'
        'JSON 배열만 출력. 다른 텍스트 없이. [] 로 감싸서.'
    )
    if AI_FULL:
        try:
            print('  [2/4] 글로벌 이슈분석...')
            gt = call_claude('claude-sonnet-4-20250514', '글로벌 매크로 전략가. 한국어. 문어체로 작성. 존댓말 사용 금지.',
                f'[시세]\n{price_str}\n[해외뉴스]\n{gl_str}\n\n글로벌 금융시장 핵심 이슈 10개 선정.\n{issue_prompt}', 4000)
            global_issues = parse_issues_json(gt)
            print(f'  글로벌 이슈: {len(global_issues)}개')
        except Exception as e:
            print(f'  글로벌 이슈 FAIL: {e}')

        try:
            print('  [3/4] 국내 이슈분석...')
            dt = call_claude('claude-sonnet-4-20250514', '한국 증시 전문 애널리스트. 한국어. 문어체로 작성. 존댓말 사용 금지.',
                f'[시세]\n{price_str}\n[국내뉴스]\n{kr_str}\n[공시]\n{dart_str}\n\n한국 증시/경제 핵심 이슈 10개 선정.\n{issue_prompt}', 4000)
            domestic_issues = parse_issues_json(dt)
            print(f'  국내 이슈: {len(domestic_issues)}개')
        except Exception as e:
            print(f'  국내 이슈 FAIL: {e}')

    # 5-3. 원자재 AI 코멘트 (11개 전체, 매 실행)
    CMDTY_AI_LIST = [
        ('BRENT',   'Brent 원유(ICE)',   '중동전쟁·OPEC·달러인덱스·EIA재고 관점에서 단기 방향성 분석'),
        ('WTI',     'WTI 원유(NYMEX)',   'Brent 스프레드·미국 셰일생산·재고 관점 분석'),
        ('NATGAS',  '천연가스(HH)',       'LNG 수요·기상·재고·한국가스공사 영향 분석'),
        ('GOLD',    '금(COMEX)',          '안전자산 수요·실질금리·달러·중앙은행 매입 관점 분석'),
        ('SILVER',  '은(COMEX)',          '금/은 비율·산업수요(태양광)·투자수요 분석'),
        ('COPPER',  '구리(COMEX)',        'AI 데이터센터·EV·중국경기·공급부족 관점 분석'),
        ('LITHIUM', '리튬',               'EV 수요 둔화·중국 과잉공급·포스코퓨처엠 영향 분석'),
        ('URA',     '우라늄(URA ETF)',    'SMR·원전 르네상스·두산에너빌리티·공급제약 분석'),
    ]
    cmdty_ai = {}
    if AI_FULL:
        try:
            print('  [4/4] 원자재 AI 코멘트...')
            cmdty_prompt = '\n'.join([
                f"- {name}({sym}): 현재가={PRICE_DATA.get(sym,{}).get('p','N/A')} | {desc}"
                for sym, name, desc in CMDTY_AI_LIST
            ])
            cmdty_resp = call_claude(
                'claude-sonnet-4-20250514',
                '글로벌 원자재 전문 애널리스트. 한국어. 문어체로 작성. 존댓말 사용 금지. 각 원자재별 단기 방향성·매매전략·한국 관련주 영향을 2~3문장으로.',
                f'[현재시세]\n{price_str}\n[해외뉴스요약]\n{gl_str[:800]}\n\n'
                f'아래 원자재 각각에 대해 JSON 배열로 분석:\n{cmdty_prompt}\n\n'
                f'출력형식: [{{"sym":"BRENT","comment":"분석내용","direction":"상승/하락/횡보","kr_stocks":"관련주"}},...]\n'
                f'JSON만 출력, 다른 텍스트 없이.',
                3000
            )
            # JSON 파싱
            raw = re.sub(r'^```(?:json)?', '', cmdty_resp).rstrip('`').strip()
            cmdty_list = json.loads(raw)
            for item in cmdty_list:
                sym = item.get('sym','')
                if sym:
                    cmdty_ai[sym] = {
                        'comment':   item.get('comment',''),
                        'direction': item.get('direction',''),
                        'kr_stocks': item.get('kr_stocks',''),
                        'ts':        TS_SHORT
                    }
            print(f'  원자재 AI: {len(cmdty_ai)}개')
        except Exception as e:
            print(f'  원자재 AI FAIL: {e}')

    # 5-4. 스윙종목 AI TOP10 선정 — 패턴A 4단계 체인드 리서치
    # Step1: 뉴스 시그널 추출 → Step2: 후보 20개 선별 → Step3: 리스크 필터링 → Step4: 매매전략 확정
    swing_quick = {}
    swing_top10 = []
    if AI_FULL:
        try:
            print('  [5/4] 패턴A 체인드 리서치 시작...')

            # KIS 실시간 + PRICE_DATA fallback (장 마감 대비)
            _price_combined = {}
            for _s in swing_top10:
                _c = _s.get('code', '')
                if _c in (kis_stock_data or {}):
                    _price_combined[_c] = kis_stock_data[_c]
                elif _c in PRICE_DATA:
                    _price_combined[_c] = PRICE_DATA[_c]
            if not _price_combined and kis_stock_data:
                _price_combined = kis_stock_data
            kis_price_str = ' | '.join([
                f"{code}:{int(d['p']):,}원({'+' if d['c']>=0 else ''}{d['c']:.2f}%)"
                for code, d in _price_combined.items()
            ]) if _price_combined else '시세 없음 — 목표가 비율 기준 제시'

            theme_guide = (
                'tc: tsm=반도체/IT/AI, tdf=방산/우주, ten=에너지/정유, '
                'trv=역발상/가치, tlg=LNG/소재, tph=제약/바이오\n'
                'ac: aby=매수/분할매수, aho=보유, awa=관심/대기, awn=주의/회피'
            )

            issue_summary = '\n'.join([
                f"- {i.get('title','')} [{i.get('impact','')}]"
                for i in (global_issues + domestic_issues)[:10]
            ])

            # ── Step 1: 뉴스·이슈에서 종목별 시그널 추출
            print('    Step1: 뉴스 시그널 추출...')
            step1_resp = call_claude(
                'claude-haiku-4-5-20251001',
                '한국주식 시그널 추출 전문가. 한국어. JSON만 출력. 존댓말 금지.',
                '\n'.join([
                    f'[국내뉴스]\n' + '\n'.join([f"{i+1}. [{n['src']}] {n['title']}" for i,n in enumerate(kr_news[:15])]) +
                    f'\n\n[글로벌 이슈]\n{issue_summary}\n\n'
                    f'[현재시세]\n{price_str}\n\n'
                    '뉴스와 이슈에서 종목별 매수/매도 시그널 추출. 명시적 언급 종목 우선, 섹터 수혜 포함.\n\n'
                    '출력형식(JSON 배열 — 마크다운 없이 JSON만):'
                    '[{"name":"종목명","code":"123456","signal":"매수/매도/중립",'
                    '"reason":"시그널 근거 1문장","sector":"섹터명","urgency":"고/중/저"},...]'
                ]),
                2000
            )
            step1_signals = extract_json_array(step1_resp) or []
            if not step1_signals and step1_resp:
                print(f'    Step1 파싱 실패 — 응답 앞 200자: {step1_resp[:200]}')
            print(f'    Step1 완료: {len(step1_signals)}개 시그널')

            # ── Step 2: 시그널 + KIS 수급 결합, 후보 20개 선별
            print('    Step2: 후보 20개 선별...')
            step1_str = '\n'.join([
                f"- {s.get('name','')}({s.get('code','')}) {s.get('signal','')} [{s.get('urgency','')}]: {s.get('reason','')}"
                for s in step1_signals[:30]
            ]) or '시그널 없음'

            foreign_str = '\n'.join([
                f"  외국인순매수: {f.get('name','')} {f.get('net_amt',0):,}백만원"
                for f in foreign_buy[:5]
            ]) if foreign_buy else '데이터 없음'
            inst_str = '\n'.join([
                f"  기관순매수: {f.get('name','')} {f.get('net_amt',0):,}백만원"
                for f in institution_buy[:5]
            ]) if institution_buy else '데이터 없음'
            fluct_str = '\n'.join([
                f"  {r.get('name','')} {r.get('rate',0):+.1f}%"
                for r in fluctuation_rank[:10]
            ]) if fluctuation_rank else '데이터 없음'

            # KIS DB 기준 종목 마스터 구성 (Step2 프롬프트 주입용)
            _code_db = {}
            for _src in [foreign_buy, institution_buy, fluctuation_rank, volume_rank_data]:
                for _item in (_src or []):
                    _n = _item.get('name','').strip()
                    _c = str(_item.get('code',''))
                    if _n and _c and _c != '123456':
                        _code_db[_n] = _c
            # kis_stock_data 코드도 추가
            for _c, _d in (kis_stock_data or {}).items():
                _code_db[_d.get('name', '')] = _c
            # 퍼지 매칭용 키 DB (공백·하이픈·점 제거 소문자)
            import re as _re_fz
            def _fz(s): return _re_fz.sub(r'[\s\-\.]', '', s).lower()
            _code_db_fz = {_fz(k): (k, v) for k, v in _code_db.items()}
            print(f'    [DB] KIS 마스터 종목: {len(_code_db)}개')

            step2_resp = call_claude(
                'claude-sonnet-4-20250514',
                '한국주식 포트폴리오 매니저. 한국어. JSON만 출력. 문어체로 작성. 존댓말 사용 금지.',
                '\n'.join(filter(None, [
                    f'[Step1 시그널]\n{step1_str}',
                    f'[KIS 실시간 시세]\n{kis_price_str}',
                    f'[외국인 순매수 (실제코드 포함)]\n' + '\n'.join([
                        f"  {f.get('name','')}({f.get('code','?')}) {f.get('net_amt',0):,}백만원"
                        for f in foreign_buy[:10]
                    ]) if foreign_buy else None,
                    f'[기관 순매수 (실제코드 포함)]\n' + '\n'.join([
                        f"  {f.get('name','')}({f.get('code','?')}) {f.get('net_amt',0):,}백만원"
                        for f in institution_buy[:10]
                    ]) if institution_buy else None,
                    f'[등락률 상위 (실제코드 포함)]\n' + '\n'.join([
                        f"  {r.get('name','')}({r.get('code','?')}) {r.get('rate',0):+.1f}%"
                        for r in fluctuation_rank[:15]
                    ]) if fluctuation_rank else None,
                    # 선택 가능 종목 전체 목록 (이름·코드 DB 기준)
                    '[선택 가능 종목 목록 — 아래 목록에서만 선택]\n' +
                    '\n'.join([f'  {nm}({cd})' for nm, cd in sorted(_code_db.items())[:60]]),
                    '⚠️ 규칙: name과 code는 반드시 위 목록의 이름·코드 그대로 사용. 목록에 없는 종목 제외.',
                    '시그널 강도·수급·모멘텀 종합해 스윙 후보 20개 선별. 중복 제거, 섹터 분산 고려.',
                    '출력형식(JSON 배열):',
                    '[{"name":"삼성전자","code":"005930","mkt":"KOSPI",'
                    '"score":85,"signal_strength":"강","supply_demand":"긍정",'
                    '"sector":"반도체","reason":"선별 근거"},...]',
                ])),
                2500
            )
            step2_candidates = extract_json_array(step2_resp) or []
            # 방법2: name으로 실제 코드 역조회 보정 + 더미코드(123456) 제거
            _fixed = []  # _code_db는 Step2 호출 전에 구성됨
            # 전각문자 → 반각 정규화 (AI가 ＳＯｉｌ 형태로 출력하는 경우 대응)
            def _normalize(s):
                result = []
                for c in s:
                    code = ord(c)
                    if 0xFF01 <= code <= 0xFF5E:   # 전각→반각
                        result.append(chr(code - 0xFEE0))
                    elif 0x3040 <= code <= 0x30FF: # 히라가나/카타카나 제거
                        continue
                    else:
                        result.append(c)
                return ''.join(result).strip()
            for _s in step2_candidates:
                _s['name'] = _normalize(_s.get('name', ''))
            _DUMMY_CODES = {'123456', '없음', 'None', '', '0'}
            import re as _re2
            for _s in step2_candidates:
                _nm = _s.get('name', '').strip()
                _cd = str(_s.get('code', '')).strip()
                # 더미코드면 DB에서 실제코드 역조회
                if _cd in _DUMMY_CODES and _nm in _code_db:
                    _s['code'] = _code_db[_nm]
                    _cd = _s['code']
                    print(f'    코드 보정: {_nm} → {_cd}')
                # 6자리 숫자 검증
                if not _re2.fullmatch(r'\d{6}', _cd):
                    print(f'    제거(형식오류): {_nm}({_cd})')
                    continue
                # 마스터DB 검증: _code_db에 없는 종목 제거 (잘못된 코드 원천 차단)
                _db_code = _code_db.get(_nm)
                if not _db_code:
                    # 퍼지 매칭 시도 (전각→반각 후 하이픈/공백 차이 허용)
                    _fz_match = _code_db_fz.get(_fz(_nm))
                    if _fz_match:
                        _db_name, _db_code = _fz_match
                        print(f'    퍼지매칭: {_nm} → {_db_name}({_db_code})')
                        _s['name'] = _db_name
                        _s['code'] = _db_code
                if _db_code and _db_code != _cd:
                    # DB에 같은 종목명이 있는데 코드가 다르면 DB 코드로 보정
                    print(f'    코드 보정(DB): {_nm} {_cd} → {_db_code}')
                    _s['code'] = _db_code
                elif not _db_code:
                    # DB에도 없고 퍼지매칭도 실패 → 제거
                    print(f'    제거(DB미등록): {_nm}({_cd})')
                    continue
                _fixed.append(_s)
            step2_candidates = _fixed
            print(f'    Step2 완료: {len(step2_candidates)}개 후보')
            for _s in step2_candidates[:3]:
                print(f'      → {_s.get("name","?")}({_s.get("code","?")}) score={_s.get("score","?")}')
            if not step2_candidates and step2_resp:
                print(f'    Step2 응답 앞 200자: {step2_resp[:200]}')

            # ── Step 3: 리스크 필터링 → TOP10 확정
            print('    Step3: 리스크 필터링 → TOP10 확정...')
            step2_str = '\n'.join([
                f"- {c.get('name','')}({c.get('code','')}/{c.get('mkt','')}) "
                f"점수:{c.get('score',0)} 시그널:{c.get('signal_strength','')} "
                f"수급:{c.get('supply_demand','')} | {c.get('reason','')}"
                for c in step2_candidates[:20]
            ]) or '후보 없음'

            vix_val = PRICE_DATA.get('VIX', {}).get('p', 0)
            brent_val = PRICE_DATA.get('BRENT', {}).get('p', 0)
            usdkrw_val = PRICE_DATA.get('USDKRW', {}).get('p', 0)

            step3_resp = call_claude(
                'claude-sonnet-4-20250514',
                '한국주식 리스크 매니저. 한국어. JSON만 출력. 문어체로 작성. 존댓말 사용 금지.',
                f'[Step2 후보]\n{step2_str}\n\n'
                f'[현재 매크로]\n'
                f'BRENT:{brent_val} USD/KRW:{usdkrw_val} VIX:{vix_val}\n\n'
                f'[AI 리스크 분석]\n{ai_sections.get("risk","없음")[:300]}\n\n'
                f'리스크 필터링 기준:\n'
                f'1. VIX 20 초과시 고위험(risk 4-5) 종목 제외\n'
                f'2. 동일 섹터 최대 3개\n'
                f'3. 수급 부정 + 시그널 약 종목 우선 제거\n'
                f'4. 최종 TOP10 순위순 정렬\n\n'
                f'{theme_guide}\n\n'
                f'출력형식(JSON 배열):\n'
                f'[{{"name":"삼성전자","code":"005930","mkt":"KOSPI","th":"반도체","tc":"tsm",'
                f'"act":"분할매수","ac":"aby","risk":2,"desc":"체인 분석 근거 1문장."}},...]',
                2500
            )
            top10_list = extract_json_array(step3_resp) or []
            # 유효 종목코드 검증: 6자리 숫자만 허용 (더미코드 제거)
            import re as _re
            swing_top10 = [
                x for x in top10_list
                if isinstance(x, dict) and x.get('name')
                and _re.fullmatch(r'\d{6}', str(x.get('code','')))
            ][:10]
            print(f'    Step3 완료: TOP{len(swing_top10)} 확정')
            for _s in swing_top10[:3]:
                print(f'      → {_s.get("name","?")}({_s.get("code","?")}) {_s.get("mkt","?")}')
            if not swing_top10 and step3_resp:
                print(f'    Step3 응답 앞 200자: {step3_resp[:200]}')

            # ── Step 4: 종목별 매매전략 생성 (목표가/손절가)
            print('    Step4: 매매전략 생성...')
            top10_str = '\n'.join([
                f"- {s['name']}({s.get('code','')}/{s.get('mkt','')}) "
                f"의견:{s.get('act','')} 리스크:{s.get('risk',3)} | {s.get('desc','')}"
                for s in swing_top10
            ])
            step4_resp = call_claude(
                'claude-haiku-4-5-20251001',
                '한국주식 트레이딩 전략가. 한국어. JSON만 출력. 문어체로 작성. 존댓말 사용 금지.',
                f'[확정 TOP10]\n{top10_str}\n\n'
                f'[KIS 실시간 시세]\n{kis_price_str}\n\n'
                f'현재가 기준 실현 가능한 목표가/손절가 제시. 시세 없으면 일반적 변동폭(±5~10%) 기준으로 제시.\n\n'
                f'출력형식(JSON 배열):\n'
                f'[{{"name":"삼성전자","signal":"매수/관망/매도",'
                f'"reason":"1줄 근거","target":75000,"stop":68000}},...]',
                2000
            )
            step4_list = extract_json_array(step4_resp) or []

            # Step4 디버그
            print(f'    Step4 응답 앞 300자: {step4_resp[:300]}')
            print(f'    Step4 파싱: {len(step4_list)}개')
            # swing_quick 구성
            for item in step4_list:
                nm = item.get('name', '')
                if nm:
                    def _to_int(v):
                        try: return int(str(v).replace(',','').replace(' ',''))
                        except: return 0
                    swing_quick[nm] = {
                        'signal': item.get('signal', ''),
                        'reason': item.get('reason', ''),
                        'target': _to_int(item.get('target', 0)),
                        'stop':   _to_int(item.get('stop', 0)),
                        'ts':     TS_SHORT
                    }

            # swing_top10 desc에 목표가/손절가 보강
            quick_map = {item.get('name', ''): item for item in step4_list}
            for s in swing_top10:
                nm = s.get('name', '')
                if nm in quick_map:
                    q = quick_map[nm]
                    tgt = q.get('target', 0)
                    stp = q.get('stop', 0)
                    if tgt:
                        s['desc'] = s.get('desc', '') + f' 목표:{tgt:,} 손절:{stp:,}'

            print(f'  패턴A 완료: TOP{len(swing_top10)} / 신호 {len(swing_quick)}개')

        except Exception as e:
            print(f'  패턴A 스윙 FAIL: {e} — 건너뜀')
            swing_top10 = []

else:
    print('  GROQ_API_KEY 없음 — AI 분석 스킵')
    cmdty_ai = {}
    swing_quick = {}
    swing_top10 = []

# ─────────────────────────────────────────
# 5-2. 뉴스/공시 요약 (새 항목만, 중복 스킵)
# ─────────────────────────────────────────
kr_news_summaries = {}
gl_news_summaries = {}
dart_summaries    = {}

if GROQ_KEY:
    # 캐시 로드 — summaries_cache.json (repo 루트, git commit으로 영속 보장)
    SUMMARIES_CACHE_FILE = 'summaries_cache.json'  # repo 루트 — git commit으로 영속 보장
    _sc = {}
    try:
        with open(SUMMARIES_CACHE_FILE, encoding='utf-8') as f:
            _sc = json.load(f)
        print(f'  캐시 파일 로드: {_sc.get("_meta","?")}')
    except Exception:
        print('  캐시 파일 없음 — 신규 생성')
    kr_news_summaries = _sc.get('kr_news', {})
    gl_news_summaries = _sc.get('gl_news', {})
    dart_summaries    = _sc.get('dart',    {})
    print(f'  캐시 로드: 국내뉴스={len(kr_news_summaries)} 해외뉴스={len(gl_news_summaries)} 공시={len(dart_summaries)}')

    def summarize_news(items, existing_cache, label, system_prompt, max_new=10):
        cache = dict(existing_cache)
        new_count = 0
        sys_prompt = system_prompt + ' 마크다운 헤더(###,##,#) 절대 사용 금지. 단락 구분은 빈 줄로만. 문어체로 작성.'

        # 캐시 히트율 계산 — 현재 뉴스 중 이미 캐시된 비율
        # 캐시 키: URL 있으면 URL 해시, 없으면 제목 앞 40자
        def _cache_key(n):
            return (n.get('link') or n['title'])[:80]
        keys = [_cache_key(n) for n in items[:max_new]]
        hit_count = sum(1 for k in keys if k in cache)
        hit_rate = hit_count / len(keys) if keys else 0

        # 히트율 출력 — 항상 미캐시 항목은 요약 생성
        if hit_rate >= 0.7:
            print(f'  {label}: 캐시 히트 {hit_count}/{len(keys)} ({hit_rate:.0%}) — 미캐시만 추가 요약')
        
        for n in items[:max_new]:
            key = (n.get('link') or n['title'])[:80]
            if key in cache:
                continue
            try:
                t = call_claude('claude-haiku-4-5-20251001', sys_prompt,
                    f"제목: {n['title']}\n출처: {n.get('src','')}\n태그: {n.get('tag','')}", 1000)
                t = re.sub(r'^#{1,4}\s*(.+)$', r'<strong>\1</strong>', t, flags=re.MULTILINE)
                t = t.replace('**','').replace('*','')
                t = re.sub(r'^- ', '• ', t, flags=re.MULTILINE)
                paras = [p.strip() for p in t.strip().split('\n\n') if p.strip()]
                t_html = ''.join([
                    f'<p style="margin:0 0 10px 0;color:var(--blue);font-weight:600">{p}</p>'
                    if p.startswith('<strong>') else
                    f'<p style="margin:0 0 10px 0">{HE(p)}</p>'
                    for p in paras
                ])
                cache[key] = {'html': t_html, 'ts': TS_SHORT}
                new_count += 1
            except Exception as e:
                print(f'  요약 FAIL [{label}] {n["title"][:20]}: {e}')
        print(f'  {label}: 신규 {new_count}건 요약 (캐시 총 {len(cache)}건, 히트율 {hit_rate:.0%})')
        return cache

    def summarize_dart(items, existing_cache, max_new=10):
        cache = dict(existing_cache)
        new_count = 0

        # 캐시 히트율 계산 (키: link||title[:80])
        keys = [(d.get('link') or d['title'])[:80] for d in items[:max_new]]
        hit_count = sum(1 for k in keys if k in cache)
        hit_rate = hit_count / len(keys) if keys else 0
        if hit_rate >= 0.7:
            print(f'  공시요약: 캐시 히트 {hit_count}/{len(keys)} ({hit_rate:.0%}) — 미캐시만 추가 요약')

        for d in items[:max_new]:
            key = (d.get('link') or d['title'])[:80]
            if key in cache:
                continue
            try:
                t = call_claude('claude-sonnet-4-20250514', '금융공시 전문가. 한국어. 문어체로 작성. 존댓말 사용 금지. 투자자 관점 핵심 요약. 마크다운 헤더(###,##) 사용 금지. 단락으로만 작성.',
                    f"공시: {d['title']}\n기업: {d.get('corp','')}\n유형: {d.get('type','')}", 800)
                t = re.sub(r'^#{1,4}\s*', '', t, flags=re.MULTILINE)
                t = t.replace('**','').replace('*','')
                t = re.sub(r'^- ', '• ', t, flags=re.MULTILINE)
                t_html = HE(t.strip())
                cache[key] = {'html': t_html, 'ts': TS_SHORT}
                new_count += 1
                time.sleep(0.3)
            except Exception as e:
                print(f'  공시요약 FAIL {d["title"][:20]}: {e}')
        print(f'  공시요약: 신규 {new_count}건 (캐시 총 {len(cache)}건)')
        return cache

    if GROQ_KEY and AI_FULL:
        print(f'\n[뉴스/공시 요약] 10건 전체 처리 (Haiku 고속)...')
    # 기존 캐시에서 ### 마크다운이 남아있는 항목 제거 (재생성 대상)
    def clean_cache(cache):
        return {k: v for k, v in cache.items() if '###' not in v.get('html','') and '##' not in v.get('html','')}
    kr_news_summaries = clean_cache(kr_news_summaries)
    gl_news_summaries = clean_cache(gl_news_summaries)
    dart_summaries    = clean_cache(dart_summaries)

    kr_news_summaries = summarize_news(kr_news, kr_news_summaries, '국내뉴스',
        '한국 금융/경제 뉴스 전문 기자. 한국어. 문어체로 작성. 존댓말 사용 금지. 마크다운 헤더 절대 금지. 배경/핵심/시장영향/투자시사점 4단락 완성.', max_new=10)
    gl_news_summaries = summarize_news(gl_news, gl_news_summaries, '해외뉴스',
        '글로벌 금융/경제 뉴스 전문 기자. 한국어. 문어체로 작성. 존댓말 사용 금지. 마크다운 헤더 절대 금지. 배경/핵심/시장영향/투자시사점 4단락 완성.', max_new=10)
    dart_summaries    = summarize_dart(dart_items, dart_summaries, max_new=10)

# ─────────────────────────────────────────
# 6. HTML 패치
# ─────────────────────────────────────────
print(f'\n[패치] {HTML_FILE} 패치 중...')

def patch(html, s_marker, e_marker, content):
    si = html.find(s_marker)
    ei = html.find(e_marker)
    if si >= 0 and ei > si:
        return html[:si] + s_marker + content + e_marker + html[ei+len(e_marker):]
    print(f'  WARNING: 마커 없음: {s_marker}')
    return html

with open(HTML_FILE, encoding='utf-8') as f:
    html = f.read()

# ── 타임스탬프
ts_unix = int(NOW.timestamp() * 1000)
html = patch(html, '<!-- ##TS_S## -->', '<!-- ##TS_E## -->', f'🔄 {TS} · {len(kr_news)}국내 · {len(gl_news)}해외 · {len(dart_items)}공시')
html = re.sub(r'id="refresh-ts-bar"', f'id="refresh-ts-bar" data-ts="{ts_unix}"', html)
html = re.sub(r'<!-- ##TS_SHORT_S## -->.*?<!-- ##TS_SHORT_E## -->', f'<!-- ##TS_SHORT_S## -->{TS_SHORT}<!-- ##TS_SHORT_E## -->', html)

# ── 시세 티커
html = patch(html, '// ##TICKS_S##', '// ##TICKS_E##', '\n' + make_ticks_js() + '\n')
html = patch(html, '// ##KIS_PRICES_S##', '// ##KIS_PRICES_E##', make_kis_prices_js(kis_stock_data))

# AI TOP10 스윙종목 패치 (선정됐을 때만 교체)
if swing_top10:
    # TOP10 종목 중 KIS 미수집 코드 추가 조회
    if kis_token:
        for s in swing_top10:
            code = s.get('code','')
            if code and code not in kis_stock_data:
                r = fetch_kis_price(code, kis_token)
                if r:
                    kis_stock_data[code] = r
                    print(f'  KIS 추가수집 {code}: {int(r["p"]):,}원')
                time.sleep(0.05)
        # 5순위: 종목별 외국인/기관 투자자 동향 수집
        investor_data = {}
        for s in swing_top10[:5]:   # 상위 5개만 (API 호출 제한)
            code = s.get('code','')
            if code:
                inv = fetch_investor_by_stock(code, kis_token)
                if inv:
                    investor_data[code] = inv
                time.sleep(0.1)
        print(f'  투자자동향: {len(investor_data)}개 종목')
        # KIS_PRICES 재패치 (TOP10 코드 포함)
        html = patch(html, '// ##KIS_PRICES_S##', '// ##KIS_PRICES_E##', make_kis_prices_js(kis_stock_data))
    else:
        investor_data = {}
    html = patch(html, '// ##STOCKS_S##', '// ##STOCKS_E##', make_stocks_js(swing_top10))
    print(f'  STOCKS: AI TOP{len(swing_top10)} 패치 완료')
else:
    print(f'  STOCKS: AI 선정 실패 → 기존 유지')
print(f'  TICKS: {len(PRICE_DATA)}개')

# ── 수급/순위 JS 패치 (1,2,3순위)
html = patch(html, '// ##SUPPLY_DEMAND_S##', '// ##SUPPLY_DEMAND_E##',
             make_supply_demand_js(foreign_buy, institution_buy, fluctuation_rank, volume_rank_data))
print(f'  수급: 외국인{len(foreign_buy)} 기관{len(institution_buy)} 등락률{len(fluctuation_rank)} 거래량{len(volume_rank_data)}')

# ── 국내뉴스 HTML (리스트 렌더)
def news_list_html(items, id_prefix, onclick_fn):
    h = ''
    for i, n in enumerate(items):
        h += (f'<div class="li" onclick="{onclick_fn}(\'{id_prefix}\',{i})" id="{id_prefix}n-{i}">'
              f'<div class="li-tag"><span class="tag {HE(n["tc"])}">{HE(n["tag"])}</span>'
              f'<span class="li-time">{HE(n["time"])} · {HE(n["src"])}</span></div>'
              f'<div class="li-title">{HE(n["title"])}</div></div>')
    h += f'<div class="news-ts">{len(items)}건 · {TS_SHORT}</div>'
    return h

html = patch(html, '<!-- ##KR_NEWS_S## -->', '<!-- ##KR_NEWS_E## -->', '\n' + news_list_html(kr_news, 'kr', 'showNews') + '\n')
html = patch(html, '<!-- ##GL_NEWS_S## -->', '<!-- ##GL_NEWS_E## -->', '\n' + news_list_html(gl_news, 'gl', 'showNews') + '\n')
print(f'  KR_NEWS: {len(kr_news)}건  GL_NEWS: {len(gl_news)}건')

# ── 공시 HTML
def dart_list_html(items):
    h = ''
    for i, d in enumerate(items):
        h += (f'<div class="li" onclick="showDart({i})" id="dart-{i}">'
              f'<div class="li-tag"><span class="tag td">공시</span>'
              f'<span class="li-time">{HE(d["date"])}</span></div>'
              f'<div class="li-title"><span class="dart-corp">{HE(d["corp"])}</span> {HE(d["title"])}</div></div>')
    if not items: h = '<div style="padding:20px;text-align:center;color:var(--txt3);font-size:12px">오늘 공시 없음</div>'
    return h

html = patch(html, '<!-- ##DART_S## -->', '<!-- ##DART_E## -->', '\n' + dart_list_html(dart_items) + '\n')
print(f'  DART: {len(dart_items)}건')

# ── 리서치 HTML
def research_list_html(items):
    h = ''
    for i, d in enumerate(items):
        h += (f'<div class="li" onclick="showResearch({i})" id="res-{i}">'
              f'<div class="li-tag"><span class="tag tr">리서치</span>'
              f'<span class="li-time">{HE(d["date"])} · <span class="research-firm">{HE(d["firm"])}</span></span></div>'
              f'<div class="li-title">{HE(d["title"])}</div></div>')
    if not items: h = '<div style="padding:20px;text-align:center;color:var(--txt3);font-size:12px">리서치 없음</div>'
    return h

html = patch(html, '<!-- ##RESEARCH_S## -->', '<!-- ##RESEARCH_E## -->', '\n' + research_list_html(research_items) + '\n')
print(f'  RESEARCH: {len(research_items)}건')

# ── JS 데이터 (뉴스 상세/종목분석용)
def news_to_js(items):
    lines = ['[']
    for n in items:
        lines.append(f"  {{title:'{JE(n['title'])}',titleKo:'{JE(n.get('titleKo',''))}',src:'{JE(n['src'])}',tag:'{JE(n['tag'])}',tc:'{n['tc']}',time:'{JE(n['time'])}',link:'{JE(n.get('link',''))}'}},")
    lines.append(']')
    return '\n'.join(lines)

html = patch(html, '// ##KR_NEWS_DATA_S##', '// ##KR_NEWS_DATA_E##', f'\nconst KR_NEWS=\n{news_to_js(kr_news)};\n')
html = patch(html, '// ##GL_NEWS_DATA_S##', '// ##GL_NEWS_DATA_E##', f'\nconst GL_NEWS=\n{news_to_js(gl_news)};\n')

# ── 뉴스/공시 요약 캐시 패치
def summaries_to_js(cache, var_name):
    # JSON 포맷으로 저장 — extract_cache의 json.loads()와 호환
    return f'\nconst {var_name}=' + json.dumps(cache, ensure_ascii=False) + ';\n'

# 캐시 파일 저장 (summaries_cache.json — repo 루트, git commit으로 영속 보장)
try:
    _cache_data = {
        '_meta': f'{NOW.strftime("%Y-%m-%d %H:%M")} KST — kr:{len(kr_news_summaries)} gl:{len(gl_news_summaries)} dart:{len(dart_summaries)}',
        'kr_news': kr_news_summaries,
        'gl_news': gl_news_summaries,
        'dart':    dart_summaries,
    }
    with open(SUMMARIES_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(_cache_data, f, ensure_ascii=False)
    print(f'  캐시 저장: {SUMMARIES_CACHE_FILE}')
except Exception as e:
    print(f'  캐시 저장 실패: {e}')

# SUMMARIES HTML 패치 — 항상 실행 (try 블록 외부)
html = patch(html, '// ##KR_NEWS_SUMMARIES_S##', '// ##KR_NEWS_SUMMARIES_E##', summaries_to_js(kr_news_summaries, 'KR_NEWS_SUMMARIES'))
html = patch(html, '// ##GL_NEWS_SUMMARIES_S##', '// ##GL_NEWS_SUMMARIES_E##', summaries_to_js(gl_news_summaries, 'GL_NEWS_SUMMARIES'))
html = patch(html, '// ##DART_SUMMARIES_S##',    '// ##DART_SUMMARIES_E##',    summaries_to_js(dart_summaries,    'DART_SUMMARIES'))
print(f'  SUMMARIES: 국내뉴스={len(kr_news_summaries)} 해외뉴스={len(gl_news_summaries)} 공시={len(dart_summaries)}')

def dart_to_js(items):
    lines = ['[']
    for d in items:
        lines.append(f"  {{title:'{JE(d['title'])}',corp:'{JE(d['corp'])}',date:'{JE(d['date'])}',type:'{JE(d['type'])}',link:'{JE(d.get('link',''))}'}},")
    lines.append(']')
    return '\n'.join(lines)

html = patch(html, '// ##DART_DATA_S##', '// ##DART_DATA_E##', f'\nconst DART_ITEMS=\n{dart_to_js(dart_items)};\n')

def research_to_js(items):
    lines = ['[']
    for d in items:
        lines.append(f"  {{title:'{JE(d['title'])}',firm:'{JE(d['firm'])}',stock:'{JE(d.get('stock',''))}',date:'{JE(d['date'])}',link:'{JE(d.get('link',''))}'}},")
    lines.append(']')
    return '\n'.join(lines)

html = patch(html, '// ##RESEARCH_DATA_S##', '// ##RESEARCH_DATA_E##', f'\nconst RESEARCH_ITEMS=\n{research_to_js(research_items)};\n')

# ── AI 분석 데이터 (섹션별 텍스트, full은 HTML 마커로만 패치)
ai_js = (
    f"const AI_SECTIONS={{\n"
    f"  full:'',\n"
    f"  bias:'{JE(ai_sections['bias'])}',\n"
    f"  forecast:'{JE(ai_sections['forecast'])}',\n"
    f"  picks:'{JE(ai_sections['picks'])}',\n"
    f"  risk:'{JE(ai_sections['risk'])}'\n"
    f"}};"
)
html = patch(html, '// ##AI_DATA_S##', '// ##AI_DATA_E##', '\n' + ai_js + '\n')

# AI분석 HTML 패치 (<!-- ##AI_S## --> 마커에 직접 삽입)
if ai_sections['full']:
    html = patch(html, '<!-- ##AI_S## -->', '<!-- ##AI_E## -->', '\n' + ai_sections['full'] + '\n')

# ── 원자재 차트 데이터
def cmdty_chart_js(data):
    lines = ['const CMDTY_CHART={']
    for key, pts in data.items():
        pts_js = json.dumps(pts, ensure_ascii=False)
        lines.append(f"  '{key}':{pts_js},")
    lines.append('};')
    return '\n'.join(lines)
html = patch(html, '// ##CMDTY_CHART_S##', '// ##CMDTY_CHART_E##', '\n' + cmdty_chart_js(cmdty_chart) + '\n')
print(f'  CMDTY_CHART: {len(cmdty_chart)}개')

# ── 이슈 JS 데이터
def issues_to_js(items):
    lines = ['[']
    for d in items:
        drivers = json.dumps([str(x) for x in d.get('drivers',[])], ensure_ascii=False)
        lines.append(
            f"  {{title:'{JE(d.get('title',''))}',impact:'{JE(d.get('impact',''))}',summary:'{JE(d.get('summary',''))}',scenarioA:'{JE(d.get('scenarioA',''))}',scenarioB:'{JE(d.get('scenarioB',''))}',drivers:{drivers},action:'{JE(d.get('action',''))}'}},")
    lines.append(']')
    return '\n'.join(lines)

issues_js = f'const GLOBAL_ISSUES=\n{issues_to_js(global_issues)};\nconst DOMESTIC_ISSUES=\n{issues_to_js(domestic_issues)};'
html = patch(html, '// ##ISSUES_DATA_S##', '// ##ISSUES_DATA_E##', '\n' + issues_js + '\n')

# ── 이슈 HTML 리스트
def issue_list_html(items, type_prefix):
    icons = ['🔴','🟠','🟡','🟢','🔵','🟣','⚪','⚫','🔶','🔷']
    h = ''
    for i, d in enumerate(items):
        h += (f'<div class="li" onclick="showIssue(\'{type_prefix}\',{i})" id="{type_prefix}-iss-{i}">'
              f'<div class="li-tag"><span>{icons[i%10]}</span>'
              f'<span class="li-time">임팩트: {HE(d.get("impact","-"))}</span></div>'
              f'<div class="li-title">{i+1}. {HE(d.get("title",""))}</div></div>')
    if not items: h = '<div style="padding:20px;text-align:center;color:var(--txt3);font-size:12px">분석 없음</div>'
    return h

html = patch(html, '<!-- ##ISSUES_GL_S## -->', '<!-- ##ISSUES_GL_E## -->', '\n' + issue_list_html(global_issues, 'gl') + '\n')
html = patch(html, '<!-- ##ISSUES_KR_S## -->', '<!-- ##ISSUES_KR_E## -->', '\n' + issue_list_html(domestic_issues, 'kr') + '\n')
print(f'  ISSUES: 글로벌 {len(global_issues)}개 · 국내 {len(domestic_issues)}개')

# ─────────────────────────────────────────
# 7. 종목 상세분석 (1시간마다 STOCK_MODE=1)
# ─────────────────────────────────────────
STOCK_LIST = [
    {'name':'삼성전자',  'th':'반도체', 'mkt':'KOSPI',  'act':'분할매수', 'desc':'이란전쟁 무관. 원화 하락 수출 수혜. 목표 227,000원.'},
    {'name':'SK하이닉스', 'th':'반도체', 'mkt':'KOSPI',  'act':'분할매수', 'desc':'AI 메모리 수요 견고.'},
    {'name':'삼성바이오', 'th':'제약',   'mkt':'KOSPI',  'act':'관심',    'desc':'지정학 무관 방어주.'},
    {'name':'한화에어로', 'th':'방산',   'mkt':'KOSPI',  'act':'보유',    'desc':'중동 수주 기대.'},
    {'name':'LIG넥스원',  'th':'방산',   'mkt':'KOSPI',  'act':'보유',    'desc':'천궁-II 수출.'},
    {'name':'한국전력',   'th':'역발상', 'mkt':'KOSPI',  'act':'관심',    'desc':'유가 하락시 이중 수혜.'},
    {'name':'S-Oil',      'th':'에너지', 'mkt':'KOSPI',  'act':'주의',    'desc':'⚠ WTI ▼-19% 재현 위험.'},
    {'name':'한국카본',   'th':'LNG소재','mkt':'KOSDAQ', 'act':'관심',    'desc':'LNG선 단열재 1위.'},
    {'name':'한국가스공사','th':'에너지', 'mkt':'KOSPI', 'act':'관심',    'desc':'EIA 연말 $70시 원가 하락.'},
]

stock_analysis = {}

if STOCK_MODE and GROQ_KEY:
    # 상세분석 대상: AI TOP10이 있으면 사용, 없으면 STOCK_LIST 폴백
    _analysis_list = [{'name':s['name'],'th':s.get('th',''),'mkt':s.get('mkt','KOSPI'),'act':s.get('act','관심'),'desc':s.get('desc',''),'code':s.get('code','')} for s in swing_top10] if swing_top10 else STOCK_LIST
    print(f'\n[종목분석] Sonnet 상세분석 시작 ({len(_analysis_list)}개)...')
    price_str = ' | '.join([f"{k}:{fmt_price(v['p'],k)}({'+' if v['c']>=0 else ''}{v['c']:.2f}%)" for k,v in PRICE_DATA.items()])
    for s in _analysis_list:
        try:
            # 6순위: 재무비율 KIS에서 실수치 조회
            fin_str = ''
            if kis_token:
                fin = fetch_finance_ratio(s.get('code', ''), kis_token)
                if fin:
                    fin_str = (f"\n[재무비율 (KIS 실데이터)]\n"
                               f"PER: {fin.get('per','-')}배  PBR: {fin.get('pbr','-')}배  "
                               f"ROE: {fin.get('roe','-')}%  EPS: {fin.get('eps','-')}원  "
                               f"BPS: {fin.get('bps','-')}원  DPS: {fin.get('dps','-')}원\n")
                time.sleep(0.1)
            # 투자자 동향 문자열
            inv_str = ''
            code = s.get('code','')
            if code and investor_data.get(code):
                inv = investor_data[code]
                parts = []
                for k,v in inv.items():
                    net = v.get('net',0)
                    arrow = '▲' if net > 0 else '▼'
                    parts.append(f"{k} {arrow}{abs(net):,}주")
                inv_str = f"\n[오늘 투자자 동향] {' | '.join(parts)}\n"
            t = call_claude(
                'claude-sonnet-4-20250514',
                '한국주식 전문 애널리스트. 한국어. 문어체로 작성. 존댓말 사용 금지. 구체적 수치와 가격 레벨 명시.',
                f"[현재시세]\n{price_str}\n"
                f"{fin_str}{inv_str}\n"
                f"종목: {s['name']}({s['th']}/{s['mkt']})\n"
                f"투자의견: {s['act']}\n현황: {s['desc']}\n\n"
                f"아래 7개 항목을 각각 구체적 수치와 함께 분석:\n\n"
                f"1. 펀더멘털\n"
                f"   - 최근 실적 (매출/영업이익 YoY)\n"
                f"   - PER/PBR/ROE 밸류에이션 (위 KIS 실데이터 기준으로 분석)\n"
                f"   - 배당수익률\n\n"
                f"2. 기술적분석\n"
                f"   - 현재 추세 (상승/하락/횡보)\n"
                f"   - 5일/20일/60일 이동평균 위치\n"
                f"   - RSI 과매수/과매도 여부\n"
                f"   - 핵심 지지선 2개 / 저항선 2개 (구체적 가격)\n"
                f"   - 볼린저밴드 위치\n"
                f"   - 거래량 특이사항\n\n"
                f"3. 진입 전략 (3단계)\n"
                f"   - 1차 매수가 / 비중\n"
                f"   - 2차 매수가 / 비중\n"
                f"   - 3차 매수가 / 비중\n\n"
                f"4. 목표가 (3단계)\n"
                f"   - 단기 목표가 (1~2개월)\n"
                f"   - 중기 목표가 (3~6개월)\n"
                f"   - 장기 목표가 (12개월)\n\n"
                f"5. 손절 기준\n"
                f"   - 손절가 (구체적 가격)\n"
                f"   - 손절 사유\n\n"
                f"6. 촉매 / 이벤트\n"
                f"   - 단기 주가 촉매 (실적발표, 수주 등)\n"
                f"   - 주의할 리스크 이벤트\n\n"
                f"7. 종합 의견 (2~3줄 요약)",
                2000
            )
            # 마크다운 → HTML 변환
            t = t.replace('**', '').replace('*', '')
            t = re.sub(r'^- ', '• ', t, flags=re.MULTILINE)
            t = re.sub(r'^(\d+\. .+)', r'<strong style="color:var(--blue)">\1</strong>', t, flags=re.MULTILINE)
            t = re.sub(r'^   - ', '  └ ', t, flags=re.MULTILINE)
            stock_analysis[s['name']] = {'html': HE(t), 'ts': TS_SHORT}
            print(f"  OK  {s['name']}")
            time.sleep(0.5)
        except Exception as e:
            print(f"  FAIL {s['name']}: {e}")
    print(f'  종목분석: {len(stock_analysis)}/{len(STOCK_LIST)} 완료')
elif STOCK_MODE:
    print('\n[종목분석] GROQ_API_KEY 없음 — 스킵')

# 기존 캐시 유지 (STOCK_MODE 아닐 때는 이전 분석 보존)
if not STOCK_MODE:
    # HTML에서 기존 STOCK_ANALYSIS 데이터 추출하여 유지
    m = re.search(r'// ##STOCK_ANALYSIS_S##\s*\nconst STOCK_ANALYSIS=(.*?);\s*\n// ##STOCK_ANALYSIS_E##', html, re.DOTALL)
    if m:
        try:
            existing = json.loads(m.group(1))
            stock_analysis = existing
            print(f'  종목분석: 기존 캐시 유지 ({len(stock_analysis)}개)')
        except:
            pass

# STOCK_ANALYSIS JS 패치
def stock_analysis_js(data):
    if not data:
        return '\nconst STOCK_ANALYSIS={};\n'
    lines = ['const STOCK_ANALYSIS={']
    for name, v in data.items():
        n_js = JE(name)
        h_js = JE(v['html'])
        t_js = JE(v['ts'])
        lines.append(f"  '{n_js}':{{html:'{h_js}',ts:'{t_js}'}},")
    lines.append('};')
    return '\n' + '\n'.join(lines) + '\n'

html = patch(html, '// ##STOCK_ANALYSIS_S##', '// ##STOCK_ANALYSIS_E##', stock_analysis_js(stock_analysis))

# STOCK_CHARTS JS 패치
def stock_charts_js(data):
    if not data:
        return '\nconst STOCK_CHARTS={};\n'
    lines = ['const STOCK_CHARTS={']
    for name, bars in data.items():
        n_js = JE(name)
        bars_json = json.dumps(bars, ensure_ascii=False)
        lines.append(f"  '{n_js}':{bars_json},")
    lines.append('};')
    return '\n' + '\n'.join(lines) + '\n'

html = patch(html, '// ##STOCK_CHARTS_S##', '// ##STOCK_CHARTS_E##', stock_charts_js(stock_charts))

# FRED / BOK / SECTOR JS 패치
def econ_js(fred, bok, sector):
    return (f'\nconst FRED_DATA={json.dumps(fred, ensure_ascii=False)};\n'
            f'const BOK_DATA={json.dumps(bok, ensure_ascii=False)};\n'
            f'const SECTOR_DATA={json.dumps(sector, ensure_ascii=False)};\n')

html = patch(html, '// ##ECON_DATA_S##', '// ##ECON_DATA_E##', econ_js(fred_data, bok_data, sector_data))

# CMDTY_AI / SWING_QUICK 패치
html = patch(html, '// ##CMDTY_AI_S##', '// ##CMDTY_AI_E##',
    f'\nconst CMDTY_AI={json.dumps(cmdty_ai if "cmdty_ai" in dir() else {}, ensure_ascii=False)};\n')
html = patch(html, '// ##SWING_QUICK_S##', '// ##SWING_QUICK_E##',
    f'\nconst SWING_QUICK={json.dumps(swing_quick if "swing_quick" in dir() else {}, ensure_ascii=False)};\n')

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# Telegram 알림
# ─────────────────────────────────────────
TG_BOT  = os.environ.get('TELEGRAM_BOT_TOKEN','')
TG_CHAT = os.environ.get('TELEGRAM_CHAT_ID','')
TG_CACHE_FILE = '/tmp/tg_alert_cache.json'   # 이전 알림 캐시

TG_QUEUE_FILE = '/tmp/tg_messages.json'

def tg_send(msg):
    """Telegram 메시지를 큐 파일에 저장 — yml에서 git push 후 실제 전송"""
    if not TG_BOT or not TG_CHAT:
        print('  [TG] 토큰/ChatID 미설정')
        return
    try:
        existing = []
        try:
            with open(TG_QUEUE_FILE, encoding='utf-8') as f:
                existing = json.load(f)
        except Exception:
            pass
        existing.append({'text': msg, 'parse_mode': 'HTML'})
        with open(TG_QUEUE_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing, f, ensure_ascii=False)
        print(f'  [TG] 큐 저장 (총 {len(existing)}건)')
    except Exception as e:
        print(f'  [TG] 큐 저장 실패: {e}')

def load_tg_cache():
    try:
        with open(TG_CACHE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {'sent_keys': [], 'ts': ''}

def save_tg_cache(cache):
    try:
        with open(TG_CACHE_FILE, 'w') as f:
            json.dump(cache, f)
    except:
        pass

if TG_BOT:
    # ── 이전 알림 캐시 로드
    tg_cache = load_tg_cache()
    prev_sent = set(tg_cache.get('sent_keys', []))

    tg_alerts = []      # 전송할 새 알림
    tg_all_keys = []    # 이번 실행에서 감지된 모든 키

    # ── 조건 알림 — 이전과 다른 것만
    for k, meta in TICK_META.items():
        d = PRICE_DATA.get(k)
        if not d: continue
        c = d['c']
        if abs(c) >= 3.0:
            sym = '🟢 급등' if c>=0 else '🔴 급락'
            # 키: 종목+방향+1% 단위 반올림 (같은 방향 1% 이내 변화는 중복)
            key = f"{k}_{sym}_{round(c)}"
            tg_all_keys.append(key)
            if key not in prev_sent:
                tg_alerts.append(f"{sym} <b>{meta['l']}</b> {meta['u']}{fmt_price(d['p'],k)} ({'+' if c>=0 else ''}{c:.2f}%)")

    for iss in global_issues + domestic_issues:
        if iss.get('impact') == '상':
            # 키: 이슈 제목 앞 20자
            key = f"iss_{iss['title'][:20]}"
            tg_all_keys.append(key)
            if key not in prev_sent:
                tg_alerts.append(f"⚠️ <b>[임팩트 상]</b> {iss['title'][:60]}")

    brent_p = PRICE_DATA.get('BRENT',{}).get('p',0)
    if brent_p > 110:
        key = f"brent_110_{int(brent_p//5)*5}"  # 5달러 단위
        tg_all_keys.append(key)
        if key not in prev_sent:
            tg_alerts.append(f"🛢 BRENT <b>${brent_p:.1f}</b> — $110 돌파")
    elif brent_p and brent_p < 70:
        key = f"brent_70_{int(brent_p//5)*5}"
        tg_all_keys.append(key)
        if key not in prev_sent:
            tg_alerts.append(f"🛢 BRENT <b>${brent_p:.1f}</b> — $70 하회")

    if tg_alerts:
        msg = f"📊 <b>HLOOMBERG 긴급알림</b> [{TS}]\n\n" + '\n'.join(tg_alerts[:5])
        tg_send(msg)
        print(f'\n[Telegram] 조건알림 {len(tg_alerts)}건 발송 (중복 {len(tg_all_keys)-len(tg_alerts)}건 억제)')
    else:
        dup = len([k for k in tg_all_keys if k in prev_sent])
        if dup > 0:
            print(f'\n[Telegram] 조건알림 없음 (중복 {dup}건 억제)')
        else:
            print('\n[Telegram] 알림 조건 없음')

    # 캐시 업데이트 — 현재 감지된 키로 교체 (조건 사라지면 다음에 다시 알림)
    save_tg_cache({'sent_keys': list(set(tg_all_keys)), 'ts': TS_SHORT})

    # ── 1시간마다 정기 요약 (매 정각 후 5분 이내 or 수동 실행 시)
    import os as _os
    is_manual   = _os.environ.get('GITHUB_EVENT_NAME','') == 'workflow_dispatch'
    is_hourly   = NOW.minute < 6   # :00~:05 → 1시간 주기
    if is_hourly or is_manual:
        lines = []

        # 1. 주요 시세
        def tk(k):
            d = PRICE_DATA.get(k, {})
            if not d: return ''
            p = fmt_price(d['p'], k)
            c = d['c']
            m = TICK_META.get(k, {})
            arr = '▲' if c >= 0 else '▼'
            return f"{m.get('l',k)} {m.get('u','')}{p} {arr}{abs(c):.2f}%"

        lines.append('📈 <b>시세</b>')
        for k in ['KOSPI','KOSDAQ','BRENT','USDKRW']:
            s = tk(k)
            if s: lines.append(f"  {s}")

        # 2. 이슈 상위 3개
        top_issues = sorted(
            global_issues + domestic_issues,
            key=lambda x: {'상':3,'중':2,'하':1}.get(x.get('impact','하'),0),
            reverse=True
        )[:3]
        if top_issues:
            lines.append('\n⚡ <b>핵심 이슈</b>')
            for iss in top_issues:
                imp = iss.get('impact','')
                imp_icon = '🔴' if imp=='상' else '🟡' if imp=='중' else '🟢'
                lines.append(f"  {imp_icon} {iss['title'][:45]}")

        # 3. 스윙종목 빠른 신호
        if swing_quick:
            lines.append('\n🎯 <b>스윙 신호</b>')
            for name, sq in list(swing_quick.items())[:5]:
                sig = sq.get('signal','')
                sig_icon = '🟢 매수' if sig=='매수' else '🔴 매도' if sig=='매도' else '⚪ 관망'
                lines.append(f"  {sig_icon} {name}")

        # 4. 뉴스 헤드라인 상위 3건
        if kr_news:
            lines.append('\n📰 <b>주요 뉴스</b>')
            for n in kr_news[:3]:
                lines.append(f"  • {n['title'][:40]}")

        lines.append(f'\n🔗 <a href="https://taehunyi-ai.github.io/hloomberg/hloomberg.html">HLOOMBERG 터미널</a>')

        summary_msg = f"📊 <b>HLOOMBERG 정기요약</b> [{TS}]\n\n" + '\n'.join(lines)
        tg_send(summary_msg)
        print(f'\n[Telegram] 정기요약 발송 ({NOW.hour:02d}:{NOW.minute:02d} KST)')
    else:
        if not tg_alerts:
            print('\n[Telegram] 알림 조건 없음 (정기요약 대기중)')
else:
    print('\n[Telegram] TELEGRAM_BOT_TOKEN 미설정 — 스킵')

# GROQ_KEY를 HTML에 주입 (브라우저 AI 호출용)
if GROQ_KEY:
    html = html.replace('Bearer ##GROQ_KEY##', f'Bearer {GROQ_KEY}')
else:
    html = html.replace('Bearer ##GROQ_KEY##', 'Bearer ')

# AI 프로바이더 항상 groq으로 고정
html = re.sub(
    r"localStorage\.getItem\('hloomberg_ai_provider'\) \|\| '[^']*'",
    "localStorage.getItem('hloomberg_ai_provider') || 'groq'",
    html
)

with open(HTML_FILE, 'w', encoding='utf-8') as f:
    f.write(html)

print(f'\n✅ Done — {TS}')
print(f'   시세:{len(PRICE_DATA)} 국내뉴스:{len(kr_news)} 해외뉴스:{len(gl_news)} 공시:{len(dart_items)} 리서치:{len(research_items)} AI:{"OK" if ai_sections["full"] else "SKIP"} 종목분석:{len(stock_analysis)}')
