#!/usr/bin/env python3
"""
HLOOMBERG TERMINAL — refresh.py
GitHub Actions에서 5분마다 실행
수집: 시세(Yahoo/Naver/Stooq) + 뉴스(RSS/네이버/다음) + 공시(DART) + 리서치(네이버금융) + AI분석(Claude)
패치: hloomberg.html 마커 치환
"""
import os, json, re, time, html as htmlmod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 설정 — API 키, 환경변수, 상수, 모델 매핑
# ─────────────────────────────────────────
# ─────────────────────────────────────────
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
GROQ_KEY      = os.environ.get('GROQ_API_KEY', '')
DART_KEY      = os.environ.get('DART_API_KEY', '')

# AI 프로바이더 선택: 환경변수 AI_PROVIDER=groq 이면 Groq, 기본값 claude
AI_PROVIDER = os.environ.get('AI_PROVIDER', 'claude').strip().lower()

# Groq 모델 매핑 (claude 모델명 → groq 모델명)
GROQ_MODEL_MAP = {
    'claude-sonnet-4-20250514':    'openai/gpt-oss-120b',
    'claude-haiku-4-5-20251001':   'openai/gpt-oss-120b',
}
GROQ_ACTIVE_MODEL = 'openai/gpt-oss-120b'  # 현재 사용 모델 (HTML 표시용)
HTML_FILE     = 'hloomberg.html'

# AI_MODE: full=전체AI (매시간 실행, 항상 full)
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

STOCK_MODE = (AI_MODE == 'full')             # 상세분석: full만
AI_FULL    = (AI_MODE == 'full')             # 이슈/원자재/TOP10: full만
AI_PARTIAL = (AI_MODE in ('full','partial')) # 종합분석/TOP10: full+partial

KST = timezone(timedelta(hours=9))
NOW = datetime.now(KST)
TS  = NOW.strftime('%Y-%m-%d %H:%M KST')
TS_SHORT = NOW.strftime('%m/%d %H:%M')
AI_HOURLY = True  # 매시간 실행 (스케줄 변경: 매 5분→매시간)

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
# ─────────────────────────────────────────
# 유틸리티 — HTML escape, HTTP 요청, 시간 포맷
# ─────────────────────────────────────────
# ─────────────────────────────────────────
def HE(s):
    """HTML 특수문자 escape (&, <, >)"""

    return htmlmod.escape(str(s or ''))

def JE(s):
    """JavaScript 문자열 escape (작은따옴표, 역슬래시, 줄바꿈)"""

    return str(s or '').replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n').replace('\r', '')

# 403 차단 도메인 자동 스킵 캐시 (런타임 중 누적)
_blocked_domains = set()

def safe_get(url, timeout=10, headers=None, referer=None):
    """재시도 없이 단순 HTTP GET. timeout 기본 10초."""

    import urllib.parse
    domain = urllib.parse.urlparse(url).netloc
    if domain in _blocked_domains:
        return None  # 이미 차단된 도메인 스킵 (무음)
    try:
        h = {'Referer': referer} if referer else {}
        if headers:
            h.update(headers)
        r = SESS.get(url, timeout=timeout, headers=h)
        if r.status_code == 403:
            _blocked_domains.add(domain)
            print(f'  403 차단 → 도메인 스킵 등록: {domain}')
            return None
        r.raise_for_status()
        return r
    except requests.exceptions.Timeout:
        # 타임아웃 1회 재시도 (짧은 대기)
        try:
            r = SESS.get(url, timeout=timeout + 5, headers=h)
            r.raise_for_status()
            return r
        except Exception:
            return None
    except Exception as e:
        err = str(e)
        if 'Max retries' in err or 'Connection' in err:
            pass  # 네트워크 오류 — 조용히 실패
        else:
            print(f'  FAIL {url[:60]}: {err[:60]}')
        return None

def fmt_time(dt):
    """datetime → 'MM/DD HH:MM' 형식 문자열"""
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
# ═════════════════════════════════════════
# 1. 시세 수집
#    KIS REST API: KOSPI/KOSDAQ/개별 종목
#    Yahoo Finance: 해외지수/원자재/환율/채권
#    병렬 수집 (ThreadPoolExecutor)
# ═════════════════════════════════════════
# ─────────────────────────────────────────

# KIS REST API 설정
KIS_APP_KEY    = os.environ.get('KIS_APP_KEY', '')
KIS_APP_SECRET = os.environ.get('KIS_APP_SECRET', '')
KIS_TOKEN_FILE = '/tmp/kis_token.json'   # GitHub Actions 임시 파일 (Actions 캐시로 24h 재사용)
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




def kis_get_token():
    """KIS access_token 관리 — 만료 30분 전까지 캐시 재사용, 이후만 재발급"""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return None
    # 캐시 파일 확인
    try:
        with open(KIS_TOKEN_FILE, 'r') as f:
            cached = json.load(f)
        expire_dt = datetime.fromisoformat(cached.get('access_token_token_expired', '2000-01-01'))
        expire_dt = expire_dt.replace(tzinfo=KST) if expire_dt.tzinfo is None else expire_dt
        remaining = (expire_dt - NOW).total_seconds() / 3600
        # 만료 30분 전까지 재사용 (SMS 알림 최소화)
        if remaining > 0.5:
            print(f'  KIS 토큰 재사용 (만료: {expire_dt.strftime("%m/%d %H:%M")} · 잔여 {remaining:.1f}h)')
            return cached['access_token']
        else:
            if remaining > 0:
                print(f'  KIS 토큰 만료 임박 ({remaining*60:.0f}분 남음) — 재발급')
            else:
                print(f'  KIS 토큰 이미 만료 ({abs(remaining)*60:.0f}분 경과) — 재발급')
    except Exception:
        pass
    # 신규 발급
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
        if not token:
            print(f'  KIS 토큰 없음: {data}')
            return None
        expire_str = data.get('access_token_token_expired', '?')
        # 캐시 저장 (토큰값은 파일에만, HTML에 절대 노출 안 함)
        with open(KIS_TOKEN_FILE, 'w') as f:
            json.dump(data, f)
        print(f'  KIS 토큰 신규 발급 OK (만료: {expire_str})')
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
        print(f'    KIS 종목 {code} 가격 0 (장 마감/데이터 없음)')
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
    except Exception:
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
    end   = NOW.strftime('%Y%m%d')
    start = (NOW - timedelta(days=days+30)).strftime('%Y%m%d')
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
    print('\n[KIS] 토큰 확인...')
    kis_token = kis_get_token()

if kis_token:
    print('[KIS] 국내 시세 수집...')
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
    def _kis_price(code):
        return code, fetch_kis_price(code, kis_token)
    with ThreadPoolExecutor(max_workers=5) as ex:
        for code, r in ex.map(lambda c: _kis_price(c), stock_code_map.keys()):
            if r:
                kis_stock_data[code] = r
                print(f"  OK  {code}  {r['p']:>10.0f}원  ({'+' if r['c']>=0 else ''}{r['c']:.2f}%)  [KIS]")
    print(f'  KIS 완료: 지수 + {len(kis_stock_data)}개 종목')

    # ── 수급/순위 3개 병렬 수집
    print('[KIS 수급/순위] 외국인·기관·등락률·거래량 병렬 수집...')
    with ThreadPoolExecutor(max_workers=4) as ex:
        f_fb  = ex.submit(fetch_foreign_institution, kis_token, '0001', '1')
        f_ib  = ex.submit(fetch_foreign_institution, kis_token, '0001', '2')
        f_flr = ex.submit(fetch_fluctuation_rank, kis_token)
        f_vol = ex.submit(fetch_volume_rank, kis_token)
        foreign_buy     = f_fb.result()
        institution_buy = f_ib.result()
        fluctuation_rank = f_flr.result()
        volume_rank_data = f_vol.result()
    print(f'  외국인:{len(foreign_buy)} 기관:{len(institution_buy)} 등락률:{len(fluctuation_rank)} 거래량:{len(volume_rank_data)}')

else:
    if KIS_APP_KEY:
        print('[KIS] 토큰 발급 실패 — Yahoo fallback')
    else:
        print('[KIS] KIS_APP_KEY 미설정 — Yahoo fallback')
    foreign_buy = []; institution_buy = []; fluctuation_rank = []; volume_rank_data = []


print(f'\n[시세] {len(TICKERS)}개 병렬 수집...')
need = {n: s for n, s in TICKERS.items() if n not in PRICE_DATA}
for n in TICKERS:
    if n not in need:
        print(f"  SKIP {n:<10} (KIS 수집 완료)")
def _get_price(args):
    name, sym = args
    return name, get_price(name, sym)
with ThreadPoolExecutor(max_workers=8) as ex:
    for name, res in ex.map(_get_price, need.items()):
        if res:
            PRICE_DATA[name] = res
            sg = '+' if res['c'] >= 0 else ''
            print(f"  OK  {name:<10} {res['p']:>12.2f}  ({sg}{res['c']:.2f}%)  [{res['src']}]")
        else:
            print(f"  FAIL {name}")
print(f'  → {len(PRICE_DATA)}/{len(TICKERS)} 수신')

def fmt_price(v, key):
    """숫자 → 단위 포함 문자열. 주가=원 단위, 지수=소수점"""
    dp = TICK_META.get(key, {}).get('dp', 2)
    if dp == 0: return f'{round(v):,}'
    return f'{v:,.{dp}f}'

# TICKS JS 배열 생성 (KIS 지수 우선 반영)
def make_ticks_js():
    """TICKS JS 배열 생성 — 시세 티커 데이터"""
    lines = ['const TICKS=[']
    for k, meta in TICK_META.items():
        d = PRICE_DATA.get(k)
        if not d: continue
        p = fmt_price(d['p'], k)
        c = d['c']
        sg = '+' if c >= 0 else ''
        cl = 'up' if c >= 0 else 'dn'
        cv = f'{sg}{abs(c):.2f}%'
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
    """STOCKS JS 배열 생성 — 스윙 TOP10 카드 데이터"""

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

print('\n[원자재 차트] 90일 데이터 병렬 수집...')
def _fetch_cmdty(args):
    key, sym = args
    return key, fetch_chart_data(sym, 90)
with ThreadPoolExecutor(max_workers=6) as ex:
    for key, pts in ex.map(_fetch_cmdty, CMDTY_CHART_SYMS.items()):
        if pts:
            cmdty_chart[key] = pts
            print(f'  OK  {key}: {len(pts)}일')
        else:
            print(f'  FAIL {key}')

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 종목 OHLCV + 기술지표
#    KIS OHLCV 90일치 → MA5/20/60, 볼린저밴드, RSI(14)
# ─────────────────────────────────────────
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
def _fetch_ohlcv(args):
    name, ticker = args
    code = ticker.split('.')[0]
    if kis_token:
        d = fetch_kis_ohlcv(code, kis_token, 90)
        if d: return name, d, 'KIS'
    d = fetch_ohlcv(ticker, 90)
    return name, d, 'Yahoo'
with ThreadPoolExecutor(max_workers=5) as ex:
    for name, data, src in ex.map(_fetch_ohlcv, STOCK_TICKERS.items()):
        if data:
            stock_charts[name] = data
            print(f'  OK  {name}: {len(data)}일  [{src}]')
        else:
            print(f'  FAIL {name}')

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# FRED API — 미국 경제지표
#    기준금리/CPI/실업률/GDP/PMI/10Y/DXY/PCE
# ─────────────────────────────────────────
# ─────────────────────────────────────────
FRED_KEY = os.environ.get('FRED_API_KEY', '')
fred_data = {}

FRED_SERIES = {
    'FED_RATE':  {'id':'FEDFUNDS',        'name':'미국 기준금리',   'unit':'%',  'yoy':False},
    'CPI_YOY':   {'id':'CPIAUCSL',        'name':'미국 CPI(YoY)',  'unit':'%',  'yoy':True},   # 지수 → YoY 계산
    'UNRATE':    {'id':'UNRATE',          'name':'미국 실업률',     'unit':'%',  'yoy':False},
    'GDP_QOQ':   {'id':'A191RL1Q225SBEA', 'name':'미국 GDP(QoQ)',  'unit':'%',  'yoy':False},  # 이미 성장률
    'US_PMI':    {'id':'NAPM',            'name':'미국 제조업PMI', 'unit':'',   'yoy':False},  # ISM Manufacturing
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

print('\n[FRED] 미국 경제지표 수집...')
def _fetch_fred(args):
    key, meta = args
    return key, meta, fetch_fred(meta['id'], yoy=meta.get('yoy', False))
with ThreadPoolExecutor(max_workers=6) as ex:
    for key, meta, vals in ex.map(_fetch_fred, FRED_SERIES.items()):
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
# ─────────────────────────────────────────
# 한국은행 OpenAPI — 한국 경제지표
#    기준금리/M2/BOP/CPI (100통계 API)
# ─────────────────────────────────────────
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
def _fetch_bok(args):
    key, meta = args
    return key, meta, fetch_bok(meta['stat'], meta['item'], meta.get('cycle','M'))
with ThreadPoolExecutor(max_workers=4) as ex:
    futs = {ex.submit(_fetch_bok, item): item for item in BOK_SERIES.items()}
    for fut in as_completed(futs):
        key, meta, vals = fut.result()
        if vals == 'BLOCKED':
            print(f'  SKIP {key} (ecos.bok.or.kr 네트워크 차단)')
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

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 섹터 히트맵 — KOSPI 10개 섹터 ETF 시세
#    반도체/2차전지/방산/바이오/자동차/금융/에너지/IT/건설/소비재
# ─────────────────────────────────────────
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

print('\n[섹터] 히트맵 데이터 수집...')
def _fetch_sector(args):
    sector, meta = args
    return sector, meta, fetch_sector_price(meta['sym'])
with ThreadPoolExecutor(max_workers=6) as ex:
    for sector, meta, d in ex.map(_fetch_sector, SECTOR_ETFS.items()):
        if d:
            sector_data[sector] = {**d, 'etf': meta['etf']}
            print(f'  OK  {sector}: {d["price"]} ({d["chg"]:+.2f}%)')
        else:
            print(f'  FAIL {sector}')

# ─────────────────────────────────────────
# ═════════════════════════════════════════
# 2. 뉴스 수집
#    KR_RSS 26개 + GL_RSS 62개 병렬 파싱
#    Google News API 병렬 쿼리
#    키워드 가중치 → 국내 20건 / 해외 20건 선별
# ═════════════════════════════════════════
# ─────────────────────────────────────────

# ── 국내 RSS: 경제·증시·기업실적·정부정책 전문 피드
KR_RSS = [
    # ── 한국경제 (4개)
    ('https://www.hankyung.com/feed/economy',       '한국경제',    'tk'),
    ('https://www.hankyung.com/feed/finance',       '한경증권',    'tk'),
    ('https://www.hankyung.com/feed/it',            '한경IT',      'te'),
    ('https://www.hankyung.com/feed/politics',      '한경정치',     'tg'),
    ('https://www.hankyung.com/feed/realestate',    '한경부동산',    'tg'),
    ('https://www.hankyung.com/feed/international', '한경국제',    'tk'),
    # ── 매일경제 (4개) — 올바른 섹션 URL로 교체
    ('https://www.mk.co.kr/rss/30100041/',          '매경경제',    'tk'),  # 경제
    ('https://www.mk.co.kr/rss/50200011/',          '매경증권',    'tk'),  # 증권 (수정)
    ('https://www.mk.co.kr/rss/30300018/',          '매경국제',    'tk'),  # 국제
    # ── 서울경제
    ('https://rss.mt.co.kr/mt_news.xml',            '머니투데이',  'tk'),  # 서울경제 403 대체
    # ── 연합뉴스 (정부발표·속보)
    ('https://www.yna.co.kr/rss/economy.xml',       '연합뉴스경제', 'te'),
    ('https://www.yna.co.kr/rss/politics.xml',      '연합뉴스정치', 'tg'),
    ('https://www.yna.co.kr/rss/industry.xml',      '연합뉴스산업', 'te'),
    ('https://www.yna.co.kr/rss/market.xml',        '연합뉴스증시', 'tm'),
    ('https://www.yna.co.kr/rss/international.xml', '연합뉴스국제', 'te'),
    # ── 전자신문 (IT·반도체)
    ('https://rss.etnews.com/Section902.xml',       '전자신문',    'te'),
    # ── 아시아경제
    ('https://www.asiae.co.kr/rss/economy.htm',     '아시아경제경제', 'te'),
    ('https://www.asiae.co.kr/rss/stock.htm',       '아시아경제증권', 'tm'),
    ('https://www.asiae.co.kr/rss/realestate.htm',  '아시아경제부동산','tg'),
    ('https://www.asiae.co.kr/rss/industry-IT.htm', '아시아경제IT',  'te'),
    ('https://www.asiae.co.kr/rss/politics.htm',    '아시아경제정치', 'tg'),
    ('https://www.asiae.co.kr/rss/world.htm',       '아시아경제국제', 'te'),
    # ── 이데일리
    ('https://rss.etoday.co.kr/eto/market_news.xml',      '이투데이마켓', 'tk'),
    ('https://rss.etoday.co.kr/eto/finance_news.xml',     '이투데이금융', 'tk'),
    ('https://rss.etoday.co.kr/eto/economy_news.xml',     '이투데이경제', 'tk'),
    # ── 뉴스핌 (증권·금융·글로벌 수급 특화)
    # ── 파이낸셜뉴스 (금융·산업)
    ('http://biz.heraldcorp.com/rss/010000.xml',                        '헤럴드경제',   'tk'),
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
    # ── 미국 (GitHub Actions 접근 가능한 공개 RSS)
    ('https://www.cnbc.com/id/20910258/device/rss/rss.html',  'CNBC Economy',  '미국',  'tm'),
    ('https://www.cnbc.com/id/15839069/device/rss/rss.html',  'CNBC Markets',  '미국',  'tm'),
    ('https://www.cnbc.com/id/15839135/device/rss/rss.html',  'CNBC Earnings', '미국',  'te'),
    ('https://www.cnbc.com/id/19832390/device/rss/rss.html',  'CNBC Asia',     '아시아', 'tg'),
    ('https://www.cnbc.com/id/19794221/device/rss/rss.html',  'CNBC Europe',   '유럽',  'tg'),
    ('https://www.cnbc.com/id/20409666/device/rss/rss.html',  'CNBC MktInsider','미국', 'tm'),
    ('https://feeds.marketwatch.com/marketwatch/topstories/',                                'MarketWatch', '미국',  'tm'),
    ('https://rss.nytimes.com/services/xml/rss/nyt/Business.xml',                           'NYT Business','미국',  'te'),
    ('https://rss.nytimes.com/services/xml/rss/nyt/World.xml',                              'NYT World',   '국제',  'te'),
    # ── 유럽
    ('https://feeds.bbci.co.uk/news/business/rss.xml',                                      'BBC Business','유럽',  'te'),
    ('https://feeds.bbci.co.uk/news/world/rss.xml',                                         'BBC World',   '국제',  'te'),
    ('https://feeds.bbci.co.uk/news/rss.xml',                                               'BBC Top',     '국제',  'te'),
    ('https://feeds.bbci.co.uk/news/technology/rss.xml',                                    'BBC Tech',    '국제',  'te'),
    ('https://feeds.bbci.co.uk/news/politics/rss.xml',                                      'BBC Politics','국제',  'tg'),
    ('https://www.theguardian.com/business/rss',                                            'Guardian Biz','유럽',  'te'),
    ('https://www.theguardian.com/world/rss',                                               'Guardian Wld','국제',  'te'),
    # ── Reuters (공개 RSS)
    # ── 중동
    ('https://www.aljazeera.com/xml/rss/all.xml',                                           'Al Jazeera',  '중동',  'tw'),
    ('https://www.middleeasteye.net/rss',                                                   'ME Eye',      '중동',  'tw'),
    ('https://www.timesofisrael.com/feed/',                                                 'ToI',         '중동',  'tw'),
    # ── 아시아
    ('https://www.scmp.com/rss/91/feed',                                                    'SCMP',        '아시아','tg'),
    ('https://economictimes.indiatimes.com/rssfeedstopstories.cms',                         'ET India',    '아시아','tg'),
    ('https://ir.thomsonreuters.com/rss/news-releases.xml',                               'TR IR',       'gl',   'tm'),
    ('https://www.ft.com/myft/following/9224e6f5-7d8e-4256-b8e0-576eb7400271.rss',       'FT',          '국제',  'tm'),
    ('https://feeds.content.dowjones.io/public/rss/RSSMarketsMain',                       'WSJ Markets', '미국',  'tm'),
    ('https://feeds.content.dowjones.io/public/rss/RSSUSnews',                            'WSJ US News', '미국',  'tm'),
    ('https://feeds.content.dowjones.io/public/rss/RSSWorldNews',                         'WSJ World',   '국제',  'tm'),
    ('https://feeds.content.dowjones.io/public/rss/RSSWSJD',                              'WSJ Tech',    '미국',  'tm'),
    ('https://feeds.content.dowjones.io/public/rss/socialeconomyfeed',                    'WSJ Economy', '미국',  'tm'),
    ('https://feeds.content.dowjones.io/public/rss/socialpoliticsfeed',                   'WSJ Politics','미국',  'tm'),
    ('https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness',                     'WSJ US Biz',  '미국',  'tm'),
    ('https://www.investing.com/rss/news_285.rss',                                        'INV CentralBnk',  '국제',  'tm'),
    ('https://www.investing.com/rss/news_20.rss',                                         'INV Commodit',    '국제',  'ten'),
    ('https://www.investing.com/rss/news.rss',                                             'INV News',        '국제',  'tm'),
    ('https://www.investing.com/rss/news_95.rss',                                          'INV Bonds',       '국제',  'tm'),
    ('https://www.investing.com/rss/stock_Options.rss',                                    'INV Options',     '미국',  'tm'),
    ('https://www.investing.com/rss/stock_ETFs.rss',                                       'INV ETFs',        '미국',  'tm'),
    ('https://www.investing.com/rss/stock_Futures.rss',                                    'INV Futures',     '미국',  'tm'),
    ('https://www.investing.com/rss/stock_Indices.rss',                                    'INV Indices',     '미국',  'tm'),
    ('https://www.investing.com/rss/stock_Stocks.rss',                                     'INV Stocks',      '미국',  'tm'),
    ('https://www.investing.com/rss/stock_stock_picks.rss',                                'INV Picks',       '미국',  'tm'),
    ('https://www.investing.com/rss/stock_Opinion.rss',                                    'INV StockOpin',   '미국',  'tm'),
    ('https://www.investing.com/rss/stock_Fundamental.rss',                                'INV StockFund',   '미국',  'tm'),
    ('https://www.investing.com/rss/stock_Technical.rss',                                  'INV StockTech',   '미국',  'tm'),
    ('https://www.investing.com/rss/286.rss',                                              'INV Popular',     '국제',  'tm'),
    ('https://www.investing.com/rss/forex_Signals.rss',                                    'INV FX Signal',   '국제',  'tm'),
    ('https://www.investing.com/rss/forex_Opinion.rss',                                    'INV FX Opinion',  '국제',  'tm'),
    ('https://www.investing.com/rss/market_overview_Fundamental.rss',                      'INV MktFund',     '국제',  'tm'),
    ('https://www.investing.com/rss/market_overview_Technical.rss',                        'INV MktTech',     '국제',  'tm'),
    ('https://www.investing.com/rss/121899.rss',                                           'INV Analysis',    '국제',  'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=commodities', 'NASDAQ Cmdty', '국제', 'ten'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=cryptocurrencies', 'NASDAQ Crypto', '국제', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=dividends', 'NASDAQ Divid', '미국', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=earnings', 'NASDAQ Earnings', '미국', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=etfs', 'NASDAQ ETFs', '미국', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=ipos', 'NASDAQ IPOs', '국제', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=markets', 'NASDAQ Markets', '국제', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=options', 'NASDAQ Options', '미국', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=stocks', 'NASDAQ Stocks', '국제', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=artificial-intelligence', 'NASDAQ AI', '국제', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=blockchain', 'NASDAQ Chain', '국제', 'tm'),
    ('https://www.nasdaq.com/feed/rssoutbound?category=corporate-governance', 'NASDAQ Gov', '국제', 'tm'),
    ('https://www.nasdaq.com/feed/nasdaq-original/rss.xml', 'NASDAQ Original', '국제', 'tm'),
    # ── 글로벌 기업 IR / 실적
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
    '수상','퇴임','인사발령',
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

from email.utils import parsedate_to_datetime as _parse_date

def parse_rss(url, src, tag, tc, max_items=4):
    """RSS/Atom 파싱 — 빈 title 자동 제거, 날짜 파싱 강화"""
    r = safe_get(url, timeout=12)
    if not r: return []
    try:
        root = ET.fromstring(r.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        items = root.findall('.//item') or root.findall('.//atom:entry', ns)
        out = []
        for it in items[:max_items]:
            title = (it.findtext('title') or it.findtext('atom:title', namespaces=ns) or '').strip()
            title = htmlmod.unescape(title)
            if not title or len(title) < 4: continue  # 빈 제목 스킵
            link  = (it.findtext('link')  or it.findtext('atom:link', namespaces=ns) or '').strip()
            pub   = it.findtext('pubDate') or it.findtext('dc:date') or                     it.findtext('atom:published', namespaces=ns) or ''
            try:   dt = _parse_date(pub)
            except:
                try:   dt = datetime.fromisoformat(pub[:19])
                except: dt = NOW
            out.append({'title': title, 'link': link, 'src': src, 'tag': tag, 'tc': tc,
                        'time': fmt_time(dt), 'stamp': dt.timestamp() if hasattr(dt,'timestamp') else 0})
        return out
    except ET.ParseError:
        return []  # XML 파싱 오류 — 조용히 실패 (HTML 응답 등)
    except Exception as e:
        print(f'  RSS parse fail [{src}]: {str(e)[:60]}')
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

print('\n[뉴스] 병렬 수집 시작...')

# ── 국내·해외 뉴스 동시 수집
def _fetch_kr_rss(args):
    url, src, tc = args
    return parse_rss(url, src, '한국', tc, 4)
def _fetch_kr_gnews(args):
    q, tc = args
    return fetch_gnews(q, '한국', tc, 'ko', 'KR', 5)
def _fetch_gl_rss(args):
    url, src, tag, tc = args
    items = parse_rss(url, src, tag, tc, 3)
    if not items:
        print(f'  GL RSS FAIL: {src} ({url[:50]})')
    return items
def _fetch_gl_gnews(args):
    q, tag, tc = args
    items = fetch_gnews(q, tag, tc, 'en', 'US', 5)
    return items

kr_raw, gl_raw = [], []
# KR/GL 분리 실행 — 각각 독립 pool, GL이 KR에 밀리지 않음
with ThreadPoolExecutor(max_workers=8) as ex_kr:
    f_kr_rss   = [ex_kr.submit(_fetch_kr_rss,   a) for a in KR_RSS]
    f_kr_gn    = [ex_kr.submit(_fetch_kr_gnews, a) for a in KR_GNEWS]
    f_kr_naver = ex_kr.submit(fetch_naver_news)
    f_kr_daum  = ex_kr.submit(fetch_daum_news)
    with ThreadPoolExecutor(max_workers=8) as ex_gl:
        f_gl_rss = [ex_gl.submit(_fetch_gl_rss,   a) for a in GL_RSS]
        f_gl_gn  = [ex_gl.submit(_fetch_gl_gnews, a) for a in GL_GNEWS]
        for f in f_gl_rss + f_gl_gn:
            try: gl_raw.extend(f.result(timeout=15))
            except Exception as e: print(f'  GL future 실패: {e}')
    for f in f_kr_rss + f_kr_gn:
        try: kr_raw.extend(f.result(timeout=15))
        except Exception as e: print(f'  KR future 실패: {e}')
    try: kr_raw.extend(f_kr_naver.result(timeout=10))
    except Exception: pass
    try: kr_raw.extend(f_kr_daum.result(timeout=5))
    except Exception: pass
print(f'  GL 원본 수집: {len(gl_raw)}건 (RSS+GNews)')

# 중복 제거
kr_news, seen_kr = [], set()
for it in kr_raw:
    if not is_relevant(it['title']): continue
    k = it['title'][:20]
    if k not in seen_kr: seen_kr.add(k); kr_news.append(it)

gl_news, seen_gl = [], set()
gl_raw_total = len(gl_raw)
for it in gl_raw:
    if not is_relevant(it['title']): continue
    k = it['title'][:20]
    if k not in seen_gl: seen_gl.add(k); gl_news.append(it)
if gl_raw_total == 0:
    print('  ⚠️  GL_RSS/GNEWS 전체 수집 실패 (네트워크/차단)')
elif len(gl_news) == 0:
    print(f'  ⚠️  GL 수집 {gl_raw_total}건 → 필터 후 0건 (BLACKLIST 과필터 가능성)')

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 키워드 가중치 선별 — 주요 종목/섹터/이슈 점수화
#    TIER1(+3): 삼성/SK하이닉스 등 대형 종목
#    TIER2(+2): 방산/에너지/중동 이슈
#    블랙리스트(제외): 스포츠/연예/생활정보
# ─────────────────────────────────────────
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
kr_news = kr_news[:20]
print(f'  국내뉴스: {len(kr_news)}건 (키워드 가중치 적용)')

# 해외뉴스 — 관련도 점수 정렬 (병렬 수집 완료 후)
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



gl_news.sort(key=score_gl, reverse=True)
gl_news = gl_news[:20]
print(f'  해외뉴스: {len(gl_news)}건 (관련도+최신 정렬)')

# 해외뉴스 제목 한글 번역 (Haiku)# ─────────────────────────────────────────
# 3. DART 공시 수집
# ─────────────────────────────────────────
print('\n[공시] DART 수집...')
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

FIRM_MAP = {
    '삼성': '삼성증권', '미래에셋': '미래에셋증권', '키움': '키움증권',
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
    """네이버 금융 리서치 - 기업분석 + 투자전략 리포트
    company_list: 종목명 | 제목 | 증권사 | 목표주가 | 날짜  (5열)
    invest_list:  분류   | 제목 | 증권사 | 날짜           (4열)
    """
    pages = [
        ('https://finance.naver.com/research/company_list.naver', 'company'),
        ('https://finance.naver.com/research/invest_list.naver',  'invest'),
    ]
    out = []
    for url, ptype in pages:
        r = safe_get(url, referer='https://finance.naver.com')
        if not r: continue
        try:
            soup = BeautifulSoup(r.content, 'html.parser')
            rows = soup.select('table.type_1 tr, table.tbl_type tr')
            for row in rows[:25]:
                tds = row.select('td')
                if len(tds) < 3: continue
                title_a = tds[1].select_one('a') if len(tds) > 1 else None
                if not title_a: continue
                title = title_a.get_text(strip=True)
                if not title or len(title) <= 3: continue
                href  = title_a.get('href', '')
                if href.startswith('/'): href = 'https://finance.naver.com' + href
                stock  = tds[0].get_text(strip=True)
                firm   = tds[2].get_text(strip=True) if len(tds) > 2 else ''
                # 목표주가: company_list는 td[3], invest_list는 없음
                target = ''
                if ptype == 'company' and len(tds) >= 5:
                    raw = tds[3].get_text(strip=True).replace(',', '').replace('원', '').strip()
                    if raw.isdigit(): target = raw + '원'
                date = tds[4].get_text(strip=True) if ptype == 'company' and len(tds) >= 5                        else tds[-1].get_text(strip=True)
                out.append({
                    'title':  title,
                    'stock':  stock,
                    'firm':   parse_firm(firm),
                    'target': target,   # 목표주가 (기업분석만, 투자전략은 '')
                    'date':   date,
                    'link':   href,
                })
        except Exception as e:
            print(f'  Research parse fail {url}: {e}')
        time.sleep(0.2)
    return out[:30]


# DART + 리서치 병렬 수집
with ThreadPoolExecutor(max_workers=2) as ex:
    f_dart     = ex.submit(lambda: fetch_dart_list() if DART_KEY else [])
    f_research = ex.submit(fetch_naver_research)
    dart_items     = f_dart.result()
    _research_raw  = f_research.result()
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
# ═════════════════════════════════════════
# 4. 리서치 리포트 수집
#    네이버 금융 증권사 리포트 (최신 15건)
#    삼성/미래에셋/키움/한투/신한/대신/NH/KB 등
# ═════════════════════════════════════════
# ─────────────────────────────────────────
print('\n[리서치] 네이버 금융 리서치 수집 (DART와 병렬)...')
research_items = []

research_items = _research_raw
print(f'  리서치: {len(research_items)}건')

# ─────────────────────────────────────────
# ═════════════════════════════════════════
# 5. AI 분석 (Groq 기본 / Claude 폴백)
#    종합분석: BIAS/FORECAST/PICKS/RISK 4섹션
#    이슈분석: 글로벌 10개 + 국내 10개
#    원자재 AI: 8개 방향성 + 한국 관련주
#    스윙 TOP10: 시세+뉴스+이슈 기반 AI 선정
#    번역: 해외뉴스 제목 한글화 (토큰 제한 없음)
# ═════════════════════════════════════════
# ─────────────────────────────────────────
print('\n[AI] Claude 분석...')
ai_sections = {'full': '', 'bias': '', 'forecast': '', 'picks': '', 'risk': ''}
ai_ts = ''
global_issues = []
domestic_issues = []

def parse_issues_json(text):
    """이슈 JSON 배열 파싱 — 4단계 복구 (응답 truncate 대응)"""
    if not text: return []
    # 마크다운/bold 제거
    clean = re.sub(r'^```(?:json)?\s*', '', text.strip()).rstrip('`').strip()
    clean = re.sub(r'\*{1,3}[^*]*\*{1,3}', '', clean)
    # [ ] 배열 범위 추출 — depth 기반으로 올바른 ] 탐색
    s1 = clean.find('[')
    if s1 >= 0:
        depth = 0
        s2 = -1
        for i in range(s1, len(clean)):
            if clean[i] == '[': depth += 1
            elif clean[i] == ']':
                depth -= 1
                if depth == 0:
                    s2 = i
                    break
        candidate = clean[s1:s2+1] if s2 > s1 else clean[s1:]
    else:
        candidate = clean
    # 1차: 직접 파싱
    try:
        result = json.loads(candidate)
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict) and x.get('title')]
    except:
        pass
    # 2차: 마지막 완전한 객체까지 (},)
    lc = candidate.rfind('},')
    if lc > 0:
        try:
            result = json.loads(candidate[:lc+1]+']')
            if isinstance(result, list):
                r = [x for x in result if isinstance(x, dict) and x.get('title')]
                if r: return r
        except:
            pass
    # 3차: 마지막 } 까지
    lc2 = candidate.rfind('}')
    if lc2 > 0:
        try:
            result = json.loads(candidate[:lc2+1]+']')
            if isinstance(result, list):
                r = [x for x in result if isinstance(x, dict) and x.get('title')]
                if r: return r
        except:
            pass
    # 4차: 마지막 { 이전까지 (잘린 마지막 객체 제거)
    lc3 = candidate.rfind('{')
    if lc3 > 0:
        try:
            result = json.loads(candidate[:lc3].rstrip(',').rstrip() + ']')
            if isinstance(result, list):
                r = [x for x in result if isinstance(x, dict) and x.get('title')]
                if r: return r
        except:
            pass
    print(f'  parse_issues_json 실패 (raw 앞 200자): {candidate[:200]}')
    return []


# Groq Free tier: 8,000 TPM 한도 → 호출 간 간격 제어
GROQ_CALL_INTERVAL = 8   # 초: 연속 호출 간 최소 대기
GROQ_RETRY_WAIT    = 15  # 초: 429 발생 시 재시도 대기
GROQ_MAX_RETRY     = 3   # 최대 재시도 횟수
_groq_last_call    = 0.0 # 마지막 Groq 호출 시각

def call_ai(model, system, user):
    """Claude 또는 Groq API 호출 (AI_PROVIDER 환경변수로 선택)"""
    global _groq_last_call

    if AI_PROVIDER == 'groq':
        if not GROQ_KEY:
            raise Exception('No GROQ_API_KEY')
        groq_model = GROQ_MODEL_MAP.get(model, 'openai/gpt-oss-120b')

        # TPM 한도 보호: 마지막 호출로부터 최소 간격 확보
        elapsed = time.time() - _groq_last_call
        if elapsed < GROQ_CALL_INTERVAL:
            time.sleep(GROQ_CALL_INTERVAL - elapsed)

        for attempt in range(GROQ_MAX_RETRY):
            _groq_last_call = time.time()
            resp = requests.post(
                'https://api.groq.com/openai/v1/chat/completions',
                headers={'Authorization': f'Bearer {GROQ_KEY}', 'Content-Type': 'application/json'},
                json={
                    'model': groq_model,
                    'messages': [
                        {'role': 'system', 'content': system},
                        {'role': 'user',   'content': user},
                    ],
                },
                timeout=120,
            )
            if resp.status_code == 401:
                raise Exception('Groq 401 Invalid API Key — GROQ_API_KEY Secret 재발급 필요')
            if resp.status_code == 429:
                wait = GROQ_RETRY_WAIT * (attempt + 1)
                print(f'    Groq 429 → {wait}초 대기 후 재시도 ({attempt+1}/{GROQ_MAX_RETRY})')
                time.sleep(wait)
                continue
            if not resp.ok:
                raise Exception(f'Groq HTTP {resp.status_code}: {resp.text[:200]}')
            raw = resp.json()['choices'][0]['message']['content'].strip()
            return re.sub(r'^```(?:json)?', '', raw).rstrip('`').strip()
        raise Exception(f'Groq 429 재시도 {GROQ_MAX_RETRY}회 초과')

    else:
        if not ANTHROPIC_KEY:
            raise Exception('No ANTHROPIC_API_KEY')
        resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json'},
            json={'model': model, 'system': system, 'messages': [{'role': 'user', 'content': user}]},
            timeout=120,
        )
        if not resp.ok:
            raise Exception(f'Claude HTTP {resp.status_code}: {resp.text[:200]}')
        raw = resp.json()['content'][0]['text'].strip()
    return re.sub(r'^```(?:json)?', '', raw).rstrip('`').strip()

# 하위 호환 alias
def call_claude(model, system, user):
    return call_ai(model, system, user)

def extract_json_array(text):
    """응답 텍스트에서 JSON 배열 안전 추출 — 4단계 복구
    1차: 마크다운 제거 2차: [] 직접 파싱 3차: 불완전 복구 4차: {} 블록 단위"""

    if not text:
        return None
    # 1차: 마크다운/코드블록 제거
    clean = re.sub(r'```(?:json)?\s*', '', text).strip().rstrip('`').strip()
    clean = re.sub(r'\*{1,3}[^*]*\*{1,3}', '', clean)  # **bold** 제거
    # 2차: 첫 [ ~ 마지막 ] 직접 파싱
    s1, s2 = clean.find('['), clean.rfind(']')
    if s1 >= 0 and s2 > s1:
        candidate = clean[s1:s2+1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
        # 3차: 마지막 완전한 객체까지 복구 (},  또는 } 패턴)
        for pat in ['},', '}']:
            last = candidate.rfind(pat)
            if last > 0:
                try:
                    result = json.loads(candidate[:last+1] + ']')
                    if result: return result
                except Exception:
                    pass
    # 4차: {} 블록 단위 개별 파싱 (배열 구조 손상 시 fallback)
    objs = []
    depth = 0; start_idx = -1
    for i, ch in enumerate(clean):
        if ch == '{':
            if depth == 0: start_idx = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start_idx >= 0:
                try:
                    obj = json.loads(clean[start_idx:i+1])
                    if isinstance(obj, dict) and obj: objs.append(obj)
                except Exception:
                    pass
    return objs if objs else None

def translate_titles(items, cache):
    """해외뉴스 제목 한글 번역 — URL 캐시 재사용, 신규만 Groq API 호출"""

    to_tr = [(i, n) for i, n in enumerate(items)
             if not re.search(r'[가-힣]', n['title']) and n['title'] not in cache]
    # 캐시에서 먼저 적용
    for n in items:
        if n['title'] in cache:
            n['titleKo'] = cache[n['title']]
    if not to_tr or not (ANTHROPIC_KEY or GROQ_KEY): return
    try:
        titles_str = '\n'.join([f"{i+1}. {n['title']}" for i, (_, n) in enumerate(to_tr)])
        result = call_ai('claude-haiku-4-5-20251001',
            '영문 뉴스 제목을 한국어로 번역. 아래 형식으로만 출력, 다른 텍스트 절대 금지:\n1. 번역된 제목\n2. 번역된 제목',
            titles_str)
        parsed = 0
        result_lines = [l.strip() for l in result.strip().split('\n') if l.strip()]
        for line in result_lines:
            # 번호. 또는 번호) 형식
            m = re.match(r'^(\d+)[.)]\s*(.+)', line)
            if m:
                idx = int(m.group(1)) - 1
                if 0 <= idx < len(to_tr):
                    orig_idx, orig_n = to_tr[idx]
                    ko = m.group(2).strip()
                    items[orig_idx]['titleKo'] = ko
                    cache[orig_n['title']] = ko
                    parsed += 1
        # fallback: 번호 없으면 순서대로 매핑
        if parsed == 0 and len(result_lines) >= len(to_tr):
            for i, line in enumerate(result_lines[:len(to_tr)]):
                orig_idx, orig_n = to_tr[i]
                ko = re.sub(r'^\d+[.)]\s*', '', line).strip()
                if ko:
                    items[orig_idx]['titleKo'] = ko
                    cache[orig_n['title']] = ko
                    parsed += 1
        print(f'  해외뉴스 제목 번역: {len(to_tr)}건 신규 / 캐시 {len(cache)}건 (파싱={parsed}) [{AI_PROVIDER}]')
    except Exception as e:
        print(f'  번역 FAIL: {e}')



if (ANTHROPIC_KEY or GROQ_KEY) and AI_PARTIAL:
    # 시세 요약
    price_str = ' | '.join([f"{k}:{fmt_price(v['p'],k)}({'+' if v['c']>=0 else ''}{v['c']:.2f}%)" for k,v in PRICE_DATA.items()])
    # 뉴스 요약 (상위 30개)
    kr_str = '\n'.join([f"{i+1}. [{n['src']}] {n['title']}" for i,n in enumerate(kr_news[:20])])
    gl_str = '\n'.join([f"{i+1}. [{n['tag']}/{n['src']}] {n['title']}" for i,n in enumerate(gl_news[:15])])
    # 공시 요약
    dart_str = '\n'.join([f"{d['corp']}: {d['title']}" for d in dart_items[:10]]) or '없음'
    # 리서치 요약
    research_str = '\n'.join([f"{r['firm']} - {r['title']}({r['stock']}){' TP:'+r['target'] if r.get('target') else ''}" for r in research_items[:10]]) or '없음'

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
            '한국 증시 전문 애널리스트 겸 글로벌 매크로 전략가. 한국어. 음슴체로 작성 (~임, ~함, ~됨, ~없음). 존댓말 금지. 각 섹션 최소 3~5문장 이상 구체적으로 작성. 면책조항/투자주의문구/데이터기준안내 절대 출력 금지.',
            combined_prompt + '\n\n아래 4개 섹션을 각각 구체적 수치와 근거를 포함해 상세히 분석:\n\n'
            '### BIAS\n'
            '뉴스 편향/오류 분석: 어떤 뉴스가 과장되거나 축소됐는지, 놓친 관점은 무엇인지, 시장이 어떤 부분을 오독하고 있는지 분석\n\n'
            '### FORECAST\n'
            '시장 전망: KOSPI/KOSDAQ 단기(1주) 방향성, 주요 지지/저항선, 관심 섹터, 환율 및 유가 전망, 다음 주 주요 이벤트\n\n'
            '### PICKS\n'
            '추천종목 3~5개: 각 종목별 추천 이유, 진입가격대, 목표가, 손절가 명시\n\n'
            '### RISK\n'
            '핵심 리스크: 상위 3개 리스크 요인, 각 리스크의 발생 가능성(%)과 시장 충격 크기, 대응 전략'
        )
        parts = re.split(r'###\s*', t)
        full_html = ''
        SECTION_ICONS = {'BIAS':'','FORECAST':'','PICKS':'','RISK':''}
        SECTION_LABELS = {'BIAS':'BIAS – 뉴스 편향/오류 분석','FORECAST':'FORECAST – 시장 전망','PICKS':'PICKS – 추천 종목','RISK':'RISK – 핵심 리스크 경고'}
        for p in parts:
            if not p.strip(): continue
            lines = p.strip().split('\n')
            header = lines[0].strip()
            body   = '\n'.join(lines[1:]).strip()
            # ** 마크다운 제거 후 헤더 키 추출
            header_clean = re.sub(r'\*+', '', header).strip()
            hkey = header_clean.upper().split()[0] if header_clean else ''
            hkey = re.sub(r'[^A-Z]', '', hkey)  # 영문자만 추출
            icon  = SECTION_ICONS.get(hkey, '📋')
            label = SECTION_LABELS.get(hkey, header_clean)
            # 본문 포맷팅 — 섹션별 특화 렌더링
            body = body.replace('**','').replace('*','')

            def md_table_to_html(text):
                """마크다운 테이블 → HTML table 변환. HE escape 없음 (raw HTML)"""

                lines = [l.strip() for l in text.strip().split('\n') if l.strip()]
                rows = [l for l in lines if l.startswith('|') and not re.match(r'^\|[-| ]+\|$', l)]
                if len(rows) < 2: return None
                th_style = 'background:var(--bg2);color:var(--blue);font-weight:600;padding:6px 10px;text-align:left;border:1px solid var(--bd);font-size:13px'
                td_style = 'padding:6px 10px;border:1px solid var(--bd);color:var(--txt2);font-size:13px;vertical-align:top'
                html_t = '<table style="width:100%;border-collapse:collapse;margin:8px 0">'
                for i, row in enumerate(rows):
                    cells = [c.strip() for c in row.strip('|').split('|')]
                    tag = 'th' if i == 0 else 'td'
                    style = th_style if i == 0 else td_style
                    html_t += '<tr>' + ''.join(f'<{tag} style="{style}">{c}</{tag}>' for c in cells) + '</tr>'
                return html_t + '</table>'

            def numbered_to_html(text):
                """번호 리스트 → ai-item div. raw HTML 직접 조립 (escape 금지)"""

                parts = re.split(r'(?=\n?\d+\.\s)', '\n' + text.strip())
                items = [p.strip() for p in parts if p.strip()]
                if len(items) < 2: return None
                out = ''
                for item in items:
                    m2 = re.match(r'^(\d+)\.\s*(.*)', item, re.DOTALL)
                    if m2:
                        num  = m2.group(1)
                        body = m2.group(2)
                        # 하위 불릿 - → 들여쓰기
                        body = re.sub(r'\n-\s+', '<br><span style="padding-left:12px;color:var(--txt3)">– ', body)
                        body = re.sub(r'(<span[^>]+>– [^<]+)', r'\1</span>', body)
                        # 줄바꿈 → <br>
                        body = body.replace('\n', '<br>')
                        out += (f'<div class="ai-item">'
                                f'<strong style="color:var(--blue)">{num}.</strong> {body}'
                                f'</div>')
                    else:
                        out += f'<div class="ai-item">{item.replace(chr(10), "<br>")}</div>'
                return out

            def bullet_to_html(text):
                """불릿(•/·) 리스트 → ai-bullet div"""

                parts = re.split(r'(?=\n?[•·]\s)', '\n' + text.strip())
                items = [p.strip() for p in parts if p.strip()]
                if len(items) < 2: return None
                out = ''
                for item in items:
                    item = re.sub(r'^[•·]\s*', '', item)
                    # 첫 콜론까지를 제목으로 강조
                    item = re.sub(r'^([^:：]{1,20}[:：])', r'<strong style="color:var(--txt)">\1</strong>', item)
                    out += f'<div class="ai-bullet">{HE_BR(item)}</div>'
                return out

            # HE_BR: HTML escape + \n → <br>
            def HE_BR(s):
                return HE(s).replace('\n', '<br>')

            # 섹션별 렌더링
            if hkey == 'PICKS' and '|' in body:
                # 테이블 앞 설명 텍스트 분리
                pre_table = re.split(r'(?=\|)', body, maxsplit=1)
                intro = pre_table[0].strip() if len(pre_table) > 1 else ''
                table_part = pre_table[1] if len(pre_table) > 1 else body
                table_html = md_table_to_html(table_part)
                if table_html:
                    intro_html = f'<p style="margin-bottom:10px">{HE(intro)}</p>' if intro else ''
                    body_html = intro_html + table_html
                else:
                    paras = [pp.strip() for pp in body.split('\n\n') if pp.strip()]
                    body_html = ''.join(f'<p>{HE(pp)}</p>' for pp in paras) if paras else HE(body)
            elif hkey == 'PICKS' and re.search(r'^\d+\.', body, re.MULTILINE):
                # PICKS 번호 형식 (테이블 없을 때)
                result = numbered_to_html(body)
                body_html = result if result else HE_BR(body)
            elif hkey == 'FORECAST' and (re.search(r'^[•·\-]\s', body, re.MULTILINE)):
                # FORECAST: • 또는 - 불릿 모두 처리
                body_norm = re.sub(r'^-\s', '• ', body, flags=re.MULTILINE)
                result = bullet_to_html(body_norm)
                body_html = result if result else HE_BR(body)
            elif hkey == 'RISK' and re.search(r'^\d+\.', body, re.MULTILINE):
                result = numbered_to_html(body)
                body_html = result if result else HE_BR(body)
            elif hkey == 'BIAS':
                # BIAS: 번호 항목이면 줄바꿈, 아니면 불릿/단락 처리
                if re.search(r'^\d+\.', body, re.MULTILINE):
                    result = numbered_to_html(body)
                    body_html = result if result else HE_BR(body)
                elif '•' in body:
                    result = bullet_to_html(body)
                    body_html = result if result else HE_BR(body)
                else:
                    body = re.sub(r'^- ', '• ', body, flags=re.MULTILINE)
                    paras = [pp.strip() for pp in body.split('\n\n') if pp.strip()]
                    body_html = ''.join(f'<p>{HE(pp)}</p>' for pp in paras) if paras else HE(body)
            else:
                body = re.sub(r'^- ', '• ', body, flags=re.MULTILINE)
                paras = [pp.strip() for pp in body.split('\n\n') if pp.strip()]
                body_html = ''.join(f'<p>{HE(pp)}</p>' for pp in paras) if paras else HE(body)
            section_html = (
                f'<div class="ai-section">'
                f'<h3>{(icon + " ") if icon else ""}{HE(label)}</h3>'
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
        # 면책/주의 문구 후처리 제거
        full_html = re.sub(
            r'---\s*모든 수치는[^<]*</p>|<p>[^<]*모든 수치는[^<]*</p>|<div[^>]*>[^<]*모든 수치는[^<]*</div>',
            '', full_html)
        full_html = re.sub(r'<p>\s*[-–—]+\s*</p>', '', full_html)
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
            gt = call_claude('claude-sonnet-4-20250514', '글로벌 매크로 전략가. 한국어. 음슴체로 작성 (예: ~임, ~함, ~됨, ~없음). 존댓말 사용 금지.',
                f'[시세]\n{price_str}\n[해외뉴스]\n{gl_str}\n\n글로벌 금융시장 핵심 이슈 10개 선정.\n{issue_prompt}')
            global_issues = parse_issues_json(gt)
            print(f'  글로벌 이슈: {len(global_issues)}개')
        except Exception as e:
            print(f'  글로벌 이슈 FAIL: {e}')

        try:
            print('  [3/4] 국내 이슈분석...')
            dt = call_claude('claude-sonnet-4-20250514', '한국 증시 전문 애널리스트. 한국어. 음슴체로 작성 (예: ~임, ~함, ~됨, ~없음). 존댓말 사용 금지.',
                f'[시세]\n{price_str}\n[국내뉴스]\n{kr_str}\n[공시]\n{dart_str}\n\n한국 증시/경제 핵심 이슈 10개 선정.\n{issue_prompt}')
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
        ('EIA',     'EIA 유가전망',       '현재 이란전쟁 상황 반영한 EIA 단기/중기 유가전망: 분기별 Brent 예상가, 장기전/단기종결 시나리오별 가격대, 한국 정유·에너지주 투자시사점'),
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
                '글로벌 원자재 전문 애널리스트. 한국어. 음슴체(~임,~함,~됨). 존댓말 금지. 각 원자재별 단기 방향성·매매전략·한국 관련주 영향을 2~3문장으로.',
                f'[현재시세]\n{price_str}\n[해외뉴스요약]\n{gl_str[:800]}\n\n'
                f'아래 원자재 각각에 대해 JSON 배열로 분석:\n{cmdty_prompt}\n\n'
                f'출력형식: [{{"sym":"BRENT","comment":"분석내용","direction":"상승/하락/횡보","kr_stocks":"관련주"}},...]\n'
                f'JSON만 출력, 다른 텍스트 없이.'
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

    # 5-4. 스윙종목 AI TOP10 선정 + 빠른 신호 (매 실행, Sonnet)
    swing_quick = {}
    swing_top10 = []   # AI가 선정한 TOP10 — HTML STOCKS 패치용
    if AI_FULL:
        try:
            print('  [5/4+] 스윙종목 AI TOP10 선정...')
    
            # 종목코드 → KIS 실시간 가격 맵 (참고용)
            kis_price_str = ' | '.join([
                f"{code}:{int(d['p']):,}원({'+' if d['c']>=0 else ''}{d['c']:.2f}%)"
                for code, d in kis_stock_data.items()
            ]) if kis_stock_data else '장 마감'
    
            # 테마/색상 코드 매핑 (Claude 프롬프트용)
            theme_guide = (
                'tc(테마색): tsm=반도체/IT/AI, tdf=방산/우주, ten=에너지/정유, '
                'trv=역발상/가치, tlg=LNG/소재, tph=제약/바이오\n'
                'ac(매매색): aby=매수/분할매수, aho=보유, awa=관심/대기, awn=주의/회피'
            )
    
            issue_summary = '\n'.join([
                f"- {i.get('title','')} [{i.get('impact','')}]"
                for i in (global_issues + domestic_issues)[:10]
            ])
            top10_resp = call_claude(
                'claude-sonnet-4-20250514',
                '한국주식 전문 스윙트레이딩 애널리스트. 한국어. 음슴체. 존댓말 금지. JSON만 출력.',
                f'[현재시세]\n{price_str}\n\n'
                f'[KIS 실시간 종목시세]\n{kis_price_str}\n\n'
                f'[국내뉴스]\n{kr_str[:800]}\n\n'
                f'[글로벌 이슈]\n{issue_summary}\n\n'
                f'현재 시장상황 기반 KOSPI/KOSDAQ 스윙트레이딩 추천종목 10개 선정.\n'
                f'반드시 종목코드(6자리) 포함. 종목당 1~2문장 근거.\n\n'
                f'{theme_guide}\n\n'
                f'출력형식(JSON 배열만):\n'
                f'[{{"name":"삼성전자","code":"005930","mkt":"KOSPI","th":"반도체","tc":"tsm",'
                f'"act":"분할매수","ac":"aby","risk":2,"desc":"목표가 XX만원. 근거 1문장."}},...]\n'
                f'risk: 1=매우낮음 2=낮음 3=중간 4=높음 5=매우높음'
            )
            top10_list = extract_json_array(top10_resp)
            if not top10_list:
                raise ValueError('TOP10 JSON 파싱 실패')
            swing_top10 = [x for x in top10_list if isinstance(x, dict) and x.get('name') and x.get('code')][:10]
            print(f'  스윙 TOP10 선정: {len(swing_top10)}개')
    
            # 빠른 신호도 TOP10 기준으로 생성
            swing_prompt = '\n'.join([
                f"{s['name']}({s.get('th','')}/{s.get('mkt','')}): {s.get('act','')} — {s.get('desc','')}"
                for s in swing_top10
            ])
            swing_resp = call_claude(
                'claude-haiku-4-5-20251001',
                '한국주식 전문 애널리스트. 한국어. 음슴체. 존댓말 금지.',
                f'[시세] {price_str}\n[뉴스] {kr_str[:400]}\n\n'
                f'아래 종목 각각의 오늘 투자의견:\n{swing_prompt}\n\n'
                f'출력형식: [{{"name":"삼성전자","signal":"매수/관망/매도","reason":"1줄이유","target":0,"stop":0}},...]\n'
                f'JSON만 출력.')
            swing_list2 = extract_json_array(swing_resp) or []
            for item in swing_list2:
                nm = item.get('name','')
                if nm:
                    swing_quick[nm] = {
                        'signal': item.get('signal',''),
                        'reason': item.get('reason',''),
                        'target': item.get('target',0),
                        'stop':   item.get('stop',0),
                        'ts':     TS_SHORT
                    }
            print(f'  스윙 빠른 신호: {len(swing_quick)}개')
    
        except Exception as e:
            print(f'  스윙 TOP10 FAIL: {e}')
            swing_top10 = []

else:
    print('  ANTHROPIC_API_KEY 없음 — AI 분석 스킵')
    cmdty_ai = {}
    swing_quick = {}
    swing_top10 = []

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 5-2. 뉴스/공시 요약
#    URL 기반 캐시 키 → 신규 항목만 API 호출
#    국내/해외 뉴스 20건 + 공시 10건
#    Haiku(속도) / Sonnet(공시 상세)
# ─────────────────────────────────────────
# ─────────────────────────────────────────
kr_news_summaries = {}
gl_news_summaries = {}
dart_summaries    = {}
title_trans       = {}

if ANTHROPIC_KEY or GROQ_KEY:
    # 기존 캐시 로드
    try:
        with open(HTML_FILE, encoding='utf-8') as f:
            existing_html = f.read()

        def extract_cache(marker_s, marker_e, html_content):
            m = re.search(re.escape(marker_s) + r'\s*\nconst \w+=(\{.*?\});\s*\n' + re.escape(marker_e), html_content, re.DOTALL)
            if m:
                try: return json.loads(m.group(1))
                except: pass
            return {}

        kr_news_summaries = extract_cache('// ##KR_NEWS_SUMMARIES_S##', '// ##KR_NEWS_SUMMARIES_E##', existing_html)
        gl_news_summaries = extract_cache('// ##GL_NEWS_SUMMARIES_S##', '// ##GL_NEWS_SUMMARIES_E##', existing_html)
        dart_summaries    = extract_cache('// ##DART_SUMMARIES_S##',    '// ##DART_SUMMARIES_E##',    existing_html)
        title_trans       = extract_cache('// ##TITLE_TRANS_S##',       '// ##TITLE_TRANS_E##',       existing_html)
        print(f'  캐시 로드: 국내뉴스={len(kr_news_summaries)} 해외뉴스={len(gl_news_summaries)} 공시={len(dart_summaries)} 번역={len(title_trans)}')
    except Exception as e:
        print(f'  캐시 로드 실패: {e}')

    def summarize_news(items, existing_cache, label, system_prompt, max_new=10):
        cache = dict(existing_cache)
        new_count = 0
        sys_prompt = system_prompt + ' 마크다운 헤더(###,##,#) 절대 사용 금지. 단락 구분은 빈 줄로만. 음슴체로 작성 (~임, ~함, ~됨).'
        for n in items[:max_new]:
            key = n.get('link','') or n['title'][:30]  # URL 우선, 없으면 제목 30자
            if key in cache:
                continue
            try:
                # Haiku — 속도 우선 (10건 전체 처리)
                t = call_claude('claude-haiku-4-5-20251001', sys_prompt,
                    f"제목: {n['title']}\n출처: {n.get('src','')}\n태그: {n.get('tag','')}")
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
                time.sleep(0.15)  # Haiku는 더 빠르므로 대기 단축
            except Exception as e:
                print(f'  요약 FAIL [{label}] {n["title"][:20]}: {e}')
        print(f'  {label}: 신규 {new_count}건 요약 (캐시 총 {len(cache)}건)')
        return cache

    def summarize_dart(items, existing_cache, max_new=20):
        cache = dict(existing_cache)
        new_count = 0
        for d in items[:max_new]:
            key = d.get('rcp_no','') or d['title'][:50]  # 공시 접수번호 우선
            if key in cache:
                continue
            try:
                t = call_claude('claude-sonnet-4-20250514', '금융공시 전문가. 한국어. 음슴체로 작성 (예: ~임, ~함, ~됨, ~없음). 존댓말 사용 금지. 투자자 관점 핵심 요약. 마크다운 헤더(###,##) 사용 금지. 단락으로만 작성.',
                    f"공시: {d['title']}\n기업: {d.get('corp','')}\n유형: {d.get('type','')}")
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

    if (ANTHROPIC_KEY or GROQ_KEY) and AI_HOURLY:
        print('\n[뉴스/공시 요약] 20건 전체 처리 (Haiku 고속)...')
    # 기존 캐시에서 ### 마크다운이 남아있는 항목 제거 (재생성 대상)
    def clean_cache(cache):
        return {k: v for k, v in cache.items() if '###' not in v.get('html','') and '##' not in v.get('html','')}
    kr_news_summaries = clean_cache(kr_news_summaries)
    gl_news_summaries = clean_cache(gl_news_summaries)
    dart_summaries    = clean_cache(dart_summaries)


    # 해외뉴스 제목 한글 번역 (매 실행, 캐시 재사용)
    translate_titles(gl_news, title_trans)
    kr_news_summaries = summarize_news(kr_news, kr_news_summaries, '국내뉴스',
        '한국 금융/경제 뉴스 전문 기자. 한국어. 음슴체(~임,~함,~됨). 존댓말 금지. 마크다운 헤더 절대 금지. 배경/핵심/시장영향/투자시사점 4단락 완성.', max_new=20)
    gl_news_summaries = summarize_news(gl_news, gl_news_summaries, '해외뉴스',
        '글로벌 금융/경제 뉴스 전문 기자. 한국어. 음슴체(~임,~함,~됨). 존댓말 금지. 마크다운 헤더 절대 금지. 배경/핵심/시장영향/투자시사점 4단락 완성.', max_new=20)
    dart_summaries    = summarize_dart(dart_items, dart_summaries, max_new=10)

# ─────────────────────────────────────────
# ═════════════════════════════════════════
# 6. HTML 패치
#    ## 마커 기반 동적 콘텐츠 삽입
#    시세/뉴스/공시/AI분석/이슈/원자재/경제지표
#    결과를 hloomberg.html에 직접 embed → GitHub Pages 서빙
# ═════════════════════════════════════════
# ─────────────────────────────────────────
print(f'\n[패치] {HTML_FILE} 패치 중...')

def patch(html, s_marker, e_marker, content):
    """## 마커 사이 콘텐츠 교체. 마커 없으면 WARNING 출력"""
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
html = patch(html, '<!-- ##TS_S## -->', '<!-- ##TS_E## -->', '')  # 갱신바 제거
html = re.sub(r'id="refresh-ts-bar"', f'id="refresh-ts-bar" data-ts="{ts_unix}"', html)
html = re.sub(r'<!-- ##TS_SHORT_S## -->.*?<!-- ##TS_SHORT_E## -->', f'<!-- ##TS_SHORT_S## -->{TS_SHORT}<!-- ##TS_SHORT_E## -->', html)

# ── 서버사이드 AI 뱃지 (실제 refresh.py가 사용한 AI)
_ai_label = 'GROQ' if AI_PROVIDER == 'groq' else 'CLAUDE'
html = re.sub(r'<!-- ##SERVER_AI_S## -->.*?<!-- ##SERVER_AI_E## -->', f'<!-- ##SERVER_AI_S## -->{_ai_label}<!-- ##SERVER_AI_E## -->', html)
# 서버사이드 AI 모델명 패치
_model_label = GROQ_ACTIVE_MODEL if AI_PROVIDER == 'groq' else 'claude-sonnet-4'
# 화면 표시용 짧은 모델명 (gpt-oss-120b, sonnet-4 등)
_model_short = _model_label.split('/')[-1] if '/' in _model_label else _model_label
html = re.sub(r'<!-- ##SERVER_MODEL_S## -->.*?<!-- ##SERVER_MODEL_E## -->', f'<!-- ##SERVER_MODEL_S## -->{_model_short}<!-- ##SERVER_MODEL_E## -->', html)
# title(tooltip) 업데이트
html = re.sub(r'(<span id="server-model-badge"[^>]*title=")[^"]*(")', rf'\g<1>{_model_label}\g<2>', html)

# ── 다음 업데이트 시간 (cron: 매 5분 주기 기준)
_next_min = (NOW.minute // 5 + 1) * 5
if _next_min >= 60:
    _next_dt = NOW.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
else:
    _next_dt = NOW.replace(minute=_next_min, second=0, microsecond=0)
_next_str = _next_dt.strftime('%H:%M')
html = re.sub(r'<!-- ##NEXT_UPDATE_S## -->.*?<!-- ##NEXT_UPDATE_E## -->', f'<!-- ##NEXT_UPDATE_S## -->{_next_str}<!-- ##NEXT_UPDATE_E## -->', html)

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
    print('  STOCKS: AI 선정 실패 → 기존 유지')
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
        target_str = f' · <span style="color:var(--yel)">{HE(d["target"])}</span>' if d.get('target') else ''
        h += (f'<div class="li" onclick="showResearch({i})" id="res-{i}">'
              f'<div class="li-tag"><span class="tag tr">리서치</span>'
              f'<span class="li-time">{HE(d["date"])} · <span class="research-firm">{HE(d["firm"])}</span>{target_str}</span></div>'
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
    """요약 캐시 → JS const 선언문. json.dumps로 직렬화"""
    # json.dumps로 직렬화 → extract_cache의 json.loads가 정상 파싱되도록
    return '\n' + f'const {var_name}=' + json.dumps(cache, ensure_ascii=False) + ';\n'

html = patch(html, '// ##KR_NEWS_SUMMARIES_S##', '// ##KR_NEWS_SUMMARIES_E##', summaries_to_js(kr_news_summaries, 'KR_NEWS_SUMMARIES'))
html = patch(html, '// ##GL_NEWS_SUMMARIES_S##', '// ##GL_NEWS_SUMMARIES_E##', summaries_to_js(gl_news_summaries, 'GL_NEWS_SUMMARIES'))
html = patch(html, '// ##DART_SUMMARIES_S##',    '// ##DART_SUMMARIES_E##',    summaries_to_js(dart_summaries,    'DART_SUMMARIES'))
# TITLE_TRANS LRU — 최근 200건만 유지
if len(title_trans) > 200:
    keep = list(title_trans.items())[-200:]
    title_trans = dict(keep)
html = patch(html, '// ##TITLE_TRANS_S##',       '// ##TITLE_TRANS_E##',       summaries_to_js(title_trans,       'TITLE_TRANS'))
print(f'  SUMMARIES: 국내뉴스={len(kr_news_summaries)} 해외뉴스={len(gl_news_summaries)} 공시={len(dart_summaries)} 번역캐시={len(title_trans)}')

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
        lines.append(f"  {{title:'{JE(d['title'])}',firm:'{JE(d['firm'])}',stock:'{JE(d.get('stock',''))}',target:'{JE(d.get('target',''))}',date:'{JE(d['date'])}',link:'{JE(d.get('link',''))}'}},")
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
# ═════════════════════════════════════════
# 7. 종목 상세분석
#    AI TOP10 또는 STOCK_LIST 9개 폴백
#    KIS 재무비율(PER/PBR/ROE/EPS) + 투자자동향 주입
#    7항목: 펀더멘털/기술분석/진입전략/목표가/손절/촉매/종합
#    raw HTML 변환 (_tbl 테이블 + 번호헤더 + 불릿)
# ═════════════════════════════════════════
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

if STOCK_MODE and (ANTHROPIC_KEY or GROQ_KEY):
    # 상세분석 대상: AI TOP10이 있으면 사용, 없으면 STOCK_LIST 폴백
    _analysis_list = [{'name':s['name'],'th':s.get('th',''),'mkt':s.get('mkt','KOSPI'),'act':s.get('act','관심'),'desc':s.get('desc',''),'code':s.get('code','')} for s in swing_top10] if swing_top10 else STOCK_LIST
    print(f'\n[종목분석] AI 상세분석 시작 ({len(_analysis_list)}개) [{AI_PROVIDER.upper()}]...')
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
                '한국주식 전문 애널리스트. 한국어. 음슴체로 작성 (예: ~임, ~함, ~됨, ~없음). 존댓말 사용 금지. 구체적 수치와 가격 레벨 명시.',
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
                f"7. 종합 의견 (2~3줄 요약)"
            )
            # 마크다운 → 구조화 HTML 변환
            t = t.replace('**', '').replace('*', '')

            # 마크다운 테이블 → HTML table
            def _tbl(m):
                lines = [l.strip() for l in m.group(0).strip().split('\n') if l.strip()]
                rows = [l for l in lines if not re.match(r'^\|[-| ]+\|$', l)]
                if len(rows) < 2: return m.group(0)
                ht = '<table style="width:100%;border-collapse:collapse;margin:8px 0;font-size:12px">'
                for i, row in enumerate(rows):
                    cells = [c.strip() for c in row.strip('|').split('|')]
                    tag = 'th' if i == 0 else 'td'
                    style = 'background:var(--bg2);color:var(--blue);font-weight:600;padding:5px 8px;border:1px solid var(--bd)' if i == 0 else 'padding:5px 8px;border:1px solid var(--bd);color:var(--txt2)'
                    ht += '<tr>' + ''.join(f'<{tag} style="{style}">{cell}</{tag}>' for cell in cells) + '</tr>'
                return ht + '</table>'
            t = re.sub(r'(\|[^\n]+\n)+', _tbl, t)

            # 번호 헤더 (1. 2. 3. ...) → 섹션 구분
            t = re.sub(
                r'^(\d+)\.\s+(.+)$',
                r'<div style="margin:14px 0 6px;padding:6px 0;border-bottom:1px solid var(--bd);color:var(--blue);font-weight:700;font-size:13px">\1. \2</div>',
                t, flags=re.MULTILINE
            )
            # 불릿 처리 (• / - )
            t = re.sub(r'^[•·]\s*(.+)$', r'<div style="padding:3px 0 3px 12px;color:var(--txt2)">• \1</div>', t, flags=re.MULTILINE)
            t = re.sub(r'^-\s+(.+)$',    r'<div style="padding:2px 0 2px 20px;color:var(--txt3);font-size:12px">– \1</div>', t, flags=re.MULTILINE)
            # 나머지 줄바꿈 → <br> (빈 줄은 간격)
            t = re.sub(r'\n\n+', '<div style="height:6px"></div>', t)
            t = t.replace('\n', '')

            stock_analysis[s['name']] = {'html': t, 'ts': TS_SHORT}
            print(f"  OK  {s['name']}")
            time.sleep(0.5)
        except Exception as e:
            print(f"  FAIL {s['name']}: {e}")
    print(f'  종목분석: {len(stock_analysis)}/{len(_analysis_list)} 완료')
elif STOCK_MODE:
    print('\n[종목분석] ANTHROPIC_API_KEY 없음 — 스킵')

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
    f'\nconst CMDTY_AI={json.dumps(cmdty_ai, ensure_ascii=False)};\n')
html = patch(html, '// ##SWING_QUICK_S##', '// ##SWING_QUICK_E##',
    f'\nconst SWING_QUICK={json.dumps(swing_quick, ensure_ascii=False)};\n')

# GROQ_KEY를 HTML에 주입 (브라우저 AI 호출용)
if GROQ_KEY:
    html = html.replace('Bearer ##GROQ_KEY##', f'Bearer {GROQ_KEY}')
else:
    html = html.replace('Bearer ##GROQ_KEY##', 'Bearer ')

# AI_PROVIDER 클라이언트 토글 제거됨 — groq 고정

with open(HTML_FILE, 'w', encoding='utf-8') as f:
    f.write(html)

print(f'\n✅ Done — {TS}')
print(f'   시세:{len(PRICE_DATA)} 국내뉴스:{len(kr_news)} 해외뉴스:{len(gl_news)} 공시:{len(dart_items)} 리서치:{len(research_items)} AI:{"OK" if ai_sections["full"] else "SKIP"} 종목분석:{len(stock_analysis)}')

# ─────────────────────────────────────────
# ─────────────────────────────────────────
# ═════════════════════════════════════════
# 8. Telegram 알림
#    정기요약: 매시간 1회 (시세4 + 이슈3 + 스윙5 + 뉴스3)
#    Bot API sendMessage (HTML parse_mode)
# ═════════════════════════════════════════
# ─────────────────────────────────────────
TG_BOT  = os.environ.get('TELEGRAM_BOT_TOKEN','')
TG_CHAT = os.environ.get('TELEGRAM_CHAT_ID','')
TG_CACHE_FILE = '/tmp/tg_alert_cache.json'   # 이전 알림 캐시

def tg_send(msg):
    """Telegram Bot API sendMessage (HTML 파싱)"""
    if not TG_BOT or not TG_CHAT:
        print('  [TG] 토큰/ChatID 미설정')
        return
    try:
        chat_id = int(TG_CHAT)
        r = requests.post(
            f'https://api.telegram.org/bot{TG_BOT}/sendMessage',
            json={'chat_id': chat_id, 'text': msg, 'parse_mode': 'HTML'},
            timeout=10
        )
        j = r.json()
        if not j.get('ok'):
            print(f'  [TG] 전송 실패: {j.get("error_code")} {j.get("description")}')
        else:
            print(f'  [TG] 전송 OK (message_id={j["result"]["message_id"]})')
    except Exception as e:
        print(f'  [TG] 예외: {e}')

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
    # ── 정기요약 (매시간 실행)
    if True:
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

        lines.append('\n🔗 <a href="https://taehunyi-ai.github.io/hloomberg/hloomberg.html">HLOOMBERG 터미널</a>')

        summary_msg = f"📊 <b>HLOOMBERG 정기요약</b> [{TS}]\n\n" + '\n'.join(lines)
        tg_send(summary_msg)
        print(f'\n[Telegram] 정기요약 발송 ({NOW.hour:02d}:{NOW.minute:02d} KST)')
else:
    print('\n[Telegram] TELEGRAM_BOT_TOKEN 미설정 — 스킵')
