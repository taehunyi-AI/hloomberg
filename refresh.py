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

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
DART_KEY      = os.environ.get('DART_API_KEY', '')
HTML_FILE     = 'hloomberg.html'
STOCK_MODE    = os.environ.get('STOCK_MODE', '0') == '1'  # 종목 상세분석 모드

KST = timezone(timedelta(hours=9))
NOW = datetime.now(KST)
TS  = NOW.strftime('%Y-%m-%d %H:%M KST')
TS_SHORT = NOW.strftime('%m/%d %H:%M')

SESS = requests.Session()
SESS.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0',
    'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
})

mode_label = '[STOCK MODE]' if STOCK_MODE else ''
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
TICKERS = {
    'KOSPI':   '^KS11', 'KOSDAQ':  '^KQ11',
    'BRENT':   'BZ=F',  'WTI':     'CL=F',
    'GOLD':    'GC=F',  'NATGAS':  'NG=F',
    'SP500':   '^GSPC', 'NIKKEI':  '^N225',
    'VIX':     '^VIX',  'UST10':   '^TNX',
    'USDKRW':  'USDKRW=X',
}
TICK_META = {
    'KOSPI':  {'l':'KOSPI',    'u':'',  'dp':0},
    'KOSDAQ': {'l':'KOSDAQ',   'u':'',  'dp':2},
    'BRENT':  {'l':'BRENT',    'u':'$', 'dp':2},
    'WTI':    {'l':'WTI',      'u':'$', 'dp':2},
    'GOLD':   {'l':'GOLD',     'u':'$', 'dp':0},
    'NATGAS': {'l':'NAT GAS',  'u':'$', 'dp':3},
    'SP500':  {'l':'S&P500',   'u':'',  'dp':2},
    'NIKKEI': {'l':'NIKKEI',   'u':'',  'dp':0},
    'VIX':    {'l':'VIX',      'u':'',  'dp':2},
    'UST10':  {'l':'10Y UST',  'u':'',  'dp':3},
    'USDKRW': {'l':'USD/KRW',  'u':'',  'dp':2},
}

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

print(f'\n[시세] {len(TICKERS)}개 수집...')
PRICE_DATA = {}
for name, sym in TICKERS.items():
    res = get_price(name, sym)
    if res:
        PRICE_DATA[name] = res
        sg = '+' if res['c'] >= 0 else ''
        print(f"  OK  {name:<10} {res['p']:>12.2f}  ({sg}{res['c']:.2f}%)  [{res['src']}]")
    else:
        print(f"  FAIL {name}")
    time.sleep(0.1)
print(f'  → {len(PRICE_DATA)}/{len(TICKERS)} 수신')

def fmt_price(v, key):
    dp = TICK_META.get(key, {}).get('dp', 2)
    if dp == 0: return f'{round(v):,}'
    return f'{v:,.{dp}f}'

# TICKS JS 배열 생성
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
        lines.append(f"  {{k:'{k}',l:'{meta['l']}',v:'{meta['u']}{p}',c:'{cv}',cl:'{cl}'}},")
    lines.append('];')
    return '\n'.join(lines)

# ─────────────────────────────────────────
# 2. 뉴스 수집
# ─────────────────────────────────────────
KR_RSS = [
    ('https://www.hankyung.com/feed/economy',   '한국경제',   'tk'),
    ('https://www.hankyung.com/feed/finance',   '한경증권',   'tk'),
    ('https://www.mk.co.kr/rss/30100041/',      '매일경제',   'tk'),
    ('https://www.mk.co.kr/rss/30200030/',      '매경증권',   'tk'),
    ('https://rss.donga.com/economy.xml',       '동아경제',   'tk'),
    ('https://www.asiae.co.kr/rss/stock.htm',   '아시아경제', 'tk'),
    ('https://rss.etnews.com/Section902.xml',   '전자신문',   'te'),
    ('https://www.sedaily.com/RSS/Economy',     '서울경제',   'tk'),
    ('https://www.yna.co.kr/rss/economy.xml',   '연합뉴스',   'tk'),
]
KR_GNEWS = [
    ('한국 증시 KOSPI 코스피',         'tk'),
    ('삼성전자 SK하이닉스 반도체',      'te'),
    ('한화에어로스페이스 방산 LIG넥스원','tk'),
    ('한국 금리 환율 경제',            'tk'),
    ('코스닥 중소형주 테마주',          'tk'),
    ('이투데이 증시 주식',             'tk'),
    ('머니투데이 증시 주식',           'tk'),
    ('파이낸셜뉴스 증시',              'tk'),
    ('헤럴드경제 증시 주식',           'tk'),
    ('뉴스핌 증시 주식',               'tk'),
]
GL_RSS = [
    ('https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114', 'CNBC',       '글로벌', 'tm'),
    ('https://feeds.marketwatch.com/marketwatch/topstories/',                                'MarketWatch','글로벌', 'tm'),
    ('https://feeds.bbci.co.uk/news/business/rss.xml',                                      'BBC',        '유럽',   'te'),
    ('https://feeds.a.dj.com/rss/RSSMarketsMain.xml',                                       'WSJ',        '유럽',   'te'),
    ('https://feeds.bloomberg.com/markets/news.rss',                                        'Bloomberg',  '유럽',   'te'),
    ('https://www.aljazeera.com/xml/rss/all.xml',                                           'Al Jazeera', '중동',   'tw'),
    ('https://www.middleeasteye.net/rss',                                                   'ME Eye',     '중동',   'tw'),
    ('https://www.timesofisrael.com/feed/',                                                 'ToI',        '중동',   'tw'),
    ('https://www.jpost.com/Rss/RssFeedsHeadlines.aspx',                                    'J.Post',     '중동',   'tw'),
    ('https://www.scmp.com/rss/91/feed',                                                    'SCMP',       '아시아', 'tg'),
    ('https://economictimes.indiatimes.com/rssfeedstopstories.cms',                         'ET India',   '아시아', 'tg'),
    ('https://seekingalpha.com/market_currents.xml',                                        'SA',         '미국',   'tm'),
    ('https://www.benzinga.com/feed',                                                       'Benzinga',   '미국',   'tm'),
]
GL_GNEWS = [
    ('Iran war oil Middle East Hormuz',  '중동',  'tw'),
    ('crude oil price WTI Brent OPEC',   '에너지','tn'),
    ('Fed interest rate inflation US',    '경제',  'te'),
    ('China economy trade war tariff',    '중국',  'tg'),
    ('Reuters business financial news',   '글로벌','tm'),
    ('Financial Times markets economy',   '유럽',  'te'),
    ('Nikkei Asia markets Japan',         '아시아','tg'),
]

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
for url, src, tc in KR_RSS:
    items = parse_rss(url, src, '한국', tc, 3)
    for it in items:
        k = it['title'][:20]
        if k not in seen_kr: seen_kr.add(k); kr_news.append(it)
    time.sleep(0.05)
# Google News
for q, tc in KR_GNEWS:
    items = fetch_gnews(q, '한국', tc, 'ko', 'KR', 4)
    for it in items:
        k = it['title'][:20]
        if k not in seen_kr: seen_kr.add(k); kr_news.append(it)
    time.sleep(0.05)
# 네이버
for it in fetch_naver_news():
    k = it['title'][:20]
    if k not in seen_kr: seen_kr.add(k); kr_news.append(it)
# 다음
for it in fetch_daum_news():
    k = it['title'][:20]
    if k not in seen_kr: seen_kr.add(k); kr_news.append(it)

kr_news.sort(key=lambda x: x.get('stamp',0), reverse=True)
kr_news = kr_news[:40]
print(f'  국내뉴스: {len(kr_news)}건')

# 해외뉴스
gl_news = []
seen_gl = set()
for url, src, tag, tc in GL_RSS:
    items = parse_rss(url, src, tag, tc, 3)
    for it in items:
        k = it['title'][:20]
        if k not in seen_gl: seen_gl.add(k); gl_news.append(it)
    time.sleep(0.05)
for q, tag, tc in GL_GNEWS:
    items = fetch_gnews(q, tag, tc, 'en', 'US', 4)
    for it in items:
        k = it['title'][:20]
        if k not in seen_gl: seen_gl.add(k); gl_news.append(it)
    time.sleep(0.05)

gl_news.sort(key=lambda x: x.get('stamp',0), reverse=True)
gl_news = gl_news[:40]
print(f'  해외뉴스: {len(gl_news)}건')

# ─────────────────────────────────────────
# 3. DART 공시 수집
# ─────────────────────────────────────────
print(f'\n[공시] DART 수집...')
dart_items = []

def fetch_dart_list():
    """OpenDART API - 전체 공시 (당일)"""
    today = NOW.strftime('%Y%m%d')
    url = f'https://opendart.fss.or.kr/api/list.json?crtfc_key={DART_KEY}&bgn_de={today}&end_de={today}&page_count=40'
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
print(f'  공시: {len(dart_items)}건')

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
    clean = re.sub(r'^```(?:json)?', '', text).rstrip('`').strip()
    s1, s2 = clean.find('['), clean.rfind(']')
    if s1 >= 0 and s2 > s1: clean = clean[s1:s2+1]
    try: return json.loads(clean)
    except:
        # 마지막 완전한 객체까지 복구
        lc = clean.rfind('},')
        if lc > 0:
            try: return json.loads(clean[:lc+1]+']')
            except: pass
        return []


def call_claude(model, system, user, max_tokens=3000):
    if not ANTHROPIC_KEY:
        raise Exception('No ANTHROPIC_API_KEY')
    resp = requests.post(
        'https://api.anthropic.com/v1/messages',
        headers={'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json'},
        json={'model': model, 'max_tokens': max_tokens, 'system': system, 'messages': [{'role': 'user', 'content': user}]},
        timeout=120,
    )
    if not resp.ok:
        raise Exception(f'Claude HTTP {resp.status_code}: {resp.text[:200]}')
    raw = resp.json()['content'][0]['text'].strip()
    return re.sub(r'^```(?:json)?', '', raw).rstrip('`').strip()

if ANTHROPIC_KEY:
    # 시세 요약
    price_str = ' | '.join([f"{k}:{fmt_price(v['p'],k)}({'+' if v['c']>=0 else ''}{v['c']:.2f}%)" for k,v in PRICE_DATA.items()])
    # 뉴스 요약 (상위 30개)
    kr_str = '\n'.join([f"{i+1}. [{n['src']}] {n['title']}" for i,n in enumerate(kr_news[:20])])
    gl_str = '\n'.join([f"{i+1}. [{n['tag']}/{n['src']}] {n['title']}" for i,n in enumerate(gl_news[:15])])
    # 공시 요약
    dart_str = '\n'.join([f"{d['corp']}: {d['title']}" for d in dart_items[:10]]) or '없음'
    # 리서치 요약
    research_str = '\n'.join([f"{r['firm']} - {r['title']}({r['stock']})" for r in research_items[:10]]) or '없음'

    combined_prompt = (
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
            '한국 증시 전문 애널리스트. 한국어. HTML 마크업 사용 가능.',
            combined_prompt + '\n\n4섹션 분석:\n### BIAS\n뉴스 편향/오류 분석\n### FORECAST\n시장 전망\n### PICKS\n추천종목 3~5개\n### RISK\n핵심 리스크 경고\n(각 섹션 끝에 한줄요약 추가)',
            3000
        )
        parts = re.split(r'###\s*', t)
        full_html = ''
        for p in parts:
            if not p.strip(): continue
            f = p.replace('**','').replace('*','')
            f = re.sub(r'^- ', '• ', f, flags=re.MULTILINE)
            f = HE(f)
            full_html += f'<div class="ai-section"><div class="ai-body">{f}</div></div>'
            lo = p.lower()
            if lo.startswith('bias'):     ai_sections['bias']     = HE(f)
            elif lo.startswith('forecast'): ai_sections['forecast'] = HE(f)
            elif lo.startswith('picks'):    ai_sections['picks']    = HE(f)
            elif lo.startswith('risk'):     ai_sections['risk']     = HE(f)
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
    try:
        print('  [2/3] 글로벌 이슈분석...')
        gt = call_claude('claude-sonnet-4-20250514', '글로벌 매크로 전략가. 한국어.',
            f'[시세]\n{price_str}\n[해외뉴스]\n{gl_str}\n\n글로벌 금융시장 핵심 이슈 5개 선정.\n{issue_prompt}', 3000)
        global_issues = parse_issues_json(gt)
        print(f'  글로벌 이슈: {len(global_issues)}개')
    except Exception as e:
        print(f'  글로벌 이슈 FAIL: {e}')

    try:
        print('  [3/3] 국내 이슈분석...')
        dt = call_claude('claude-sonnet-4-20250514', '한국 증시 전문 애널리스트. 한국어.',
            f'[시세]\n{price_str}\n[국내뉴스]\n{kr_str}\n[공시]\n{dart_str}\n\n한국 증시/경제 핵심 이슈 5개 선정.\n{issue_prompt}', 3000)
        domestic_issues = parse_issues_json(dt)
        print(f'  국내 이슈: {len(domestic_issues)}개')
    except Exception as e:
        print(f'  국내 이슈 FAIL: {e}')

else:
    print('  ANTHROPIC_API_KEY 없음 — AI 분석 스킵')

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

# ── Anthropic API 키 주입 (GitHub Secret → HTML, 키가 소스코드에 노출되지 않음)
if ANTHROPIC_KEY:
    html = html.replace("'##ANTHROPIC_KEY##'", f"'{ANTHROPIC_KEY}'")
    print(f'  API key injected (length: {len(ANTHROPIC_KEY)})')
else:
    print('  WARNING: ANTHROPIC_API_KEY not set — Haiku on-demand calls will fail')

# ── 타임스탬프
html = patch(html, '<!-- ##TS_S## -->', '<!-- ##TS_E## -->', f'🔄 {TS} · {len(kr_news)}국내 · {len(gl_news)}해외 · {len(dart_items)}공시')
html = re.sub(r'<!-- ##TS_SHORT_S## -->.*?<!-- ##TS_SHORT_E## -->', f'<!-- ##TS_SHORT_S## -->{TS_SHORT}<!-- ##TS_SHORT_E## -->', html)

# ── 시세 티커
html = patch(html, '// ##TICKS_S##', '// ##TICKS_E##', '\n' + make_ticks_js() + '\n')
print(f'  TICKS: {len(PRICE_DATA)}개')

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

# ── AI 분석 데이터
ai_js = (
    f"const AI_SECTIONS={{\n"
    f"  full:'{JE(ai_sections['full'])}',\n"
    f"  bias:'{JE(ai_sections['bias'])}',\n"
    f"  forecast:'{JE(ai_sections['forecast'])}',\n"
    f"  picks:'{JE(ai_sections['picks'])}',\n"
    f"  risk:'{JE(ai_sections['risk'])}'\n"
    f"}};"
)
html = patch(html, '// ##AI_DATA_S##', '// ##AI_DATA_E##', '\n' + ai_js + '\n')

# AI분석 HTML 패치 (overview 직접 표시)
if ai_sections['full']:
    html = patch(html, '<!-- ##AI_S## -->', '<!-- ##AI_E## -->', '\n' + ai_sections['full'] + '\n')
html = patch(html, '<!-- ##AI_TS_S## -->', '<!-- ##AI_TS_E## -->', ai_ts or '대기')

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

if STOCK_MODE and ANTHROPIC_KEY:
    print(f'\n[종목분석] Sonnet 상세분석 시작 ({len(STOCK_LIST)}개)...')
    price_str = ' | '.join([f"{k}:{fmt_price(v['p'],k)}({'+' if v['c']>=0 else ''}{v['c']:.2f}%)" for k,v in PRICE_DATA.items()])
    for s in STOCK_LIST:
        try:
            t = call_claude(
                'claude-sonnet-4-20250514',
                '한국주식 애널리스트. 한국어. 구체적 수치 포함.',
                f"[현재시세]\n{price_str}\n\n"
                f"종목: {s['name']}({s['th']}/{s['mkt']})\n"
                f"의견: {s['act']}\n기본: {s['desc']}\n\n"
                f"상세분석:\n"
                f"1. 펀더멘털 (실적/밸류에이션)\n"
                f"2. 기술적분석 (지지/저항/추세)\n"
                f"3. 진입가 3단계\n"
                f"4. 목표가 3단계\n"
                f"5. 손절가\n"
                f"6. 핵심 리스크",
                1500
            )
            # 마크다운 → HTML 변환
            t = t.replace('**', '').replace('*', '')
            t = re.sub(r'^- ', '• ', t, flags=re.MULTILINE)
            t = re.sub(r'^(\d+\. )', r'<strong style="color:var(--blue)">\1</strong>', t, flags=re.MULTILINE)
            stock_analysis[s['name']] = {'html': HE(t), 'ts': TS_SHORT}
            print(f"  OK  {s['name']}")
            time.sleep(0.5)
        except Exception as e:
            print(f"  FAIL {s['name']}: {e}")
    print(f'  종목분석: {len(stock_analysis)}/{len(STOCK_LIST)} 완료')
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

# ── 저장
with open(HTML_FILE, 'w', encoding='utf-8') as f:
    f.write(html)

print(f'\n✅ Done — {TS}')
print(f'   시세:{len(PRICE_DATA)} 국내뉴스:{len(kr_news)} 해외뉴스:{len(gl_news)} 공시:{len(dart_items)} 리서치:{len(research_items)} AI:{"OK" if ai_sections["full"] else "SKIP"} 종목분석:{len(stock_analysis)}')
