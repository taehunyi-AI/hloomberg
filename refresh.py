#!/usr/bin/env python3
"""
HLOOMBERG TERMINAL — GitHub Actions refresh script
Yahoo Finance (→ Naver → Stooq fallback) + Claude API → patches index.html
"""
import os, json, re, time, requests
from datetime import datetime, timezone, timedelta

API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
HTML_FILE = 'index.html'

KST = timezone(timedelta(hours=9))
NOW = datetime.now(KST)
TS  = NOW.strftime('%Y-%m-%d %H:%M KST')

# ── Ticker map
TICKERS = {
    'KOSPI':      '^KS11',
    'KOSDAQ':     '^KQ11',
    'Samsung':    '005930.KS',
    'SKHynix':    '000660.KS',
    'SamsungBio': '207940.KS',
    'HanwhaAero': '012450.KS',
    'LIG':        '079550.KS',
    'KorCarbon':  '017960.KS',
    'KOGAS':      '036460.KS',
    'KEPCO':      '015760.KS',
    'KMW':        '032500.KS',
    'SOil':       '010950.KS',
    'SP500':      '^GSPC',
    'GOLD':       'GC=F',
    'VIX':        '^VIX',
    'UST10':      '^TNX',
    'USDKRW':     'USDKRW=X',
    'WTI':        'CL=F',
    'BRENT':      'BZ=F',
    'NATGAS':     'NG=F',
    'NIKKEI':     '^N225',
}

TMAP = [
    ('KOSPI',   'KOSPI',   0),
    ('KOSDAQ',  'KOSDAQ',  2),
    ('BRENT',   'BRENT',   2),
    ('WTI',     'WTI',     2),
    ('GOLD',    'GOLD',    0),
    ('VIX',     'VIX',     2),
    ('UST10',   '10Y UST', 3),
    ('USDKRW',  'USD/KRW', 2),
    ('SP500',   'S&P500',  2),
    ('NATGAS',  'NAT GAS', 3),
    ('NIKKEI',  'NIKKEI',  0),
    ('KMW',     'KMW',     0),
]

SESS = requests.Session()
SESS.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'})

# ─────────────────────────────────────────
# 1. 시세 수집
# ─────────────────────────────────────────
def fetch_yahoo(ticker):
    for q in ('query1', 'query2'):
        try:
            url = f'https://{q}.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(ticker)}?interval=1d&range=5d'
            r = SESS.get(url, timeout=8, headers={'Referer': 'https://finance.yahoo.com', 'Accept': 'application/json'})
            if not r.ok: continue
            closes = [v for v in r.json()['chart']['result'][0]['indicators']['quote'][0]['close'] if v is not None]
            if not closes: continue
            p, p0 = closes[-1], (closes[-2] if len(closes) >= 2 else closes[-1])
            return {'p': p, 'c': (p - p0) / p0 * 100 if p0 else 0, 'src': 'Yahoo'}
        except:
            time.sleep(0.3)
    return None

def fetch_naver(ticker):
    try:
        if ticker == '^KS11':      url = 'https://m.stock.naver.com/api/index/KOSPI/basic'
        elif ticker == '^KQ11':    url = 'https://m.stock.naver.com/api/index/KOSDAQ/basic'
        elif ticker == 'USDKRW=X': url = 'https://m.stock.naver.com/api/stock/USDKRW/basic'
        elif ticker.endswith('.KS'):
            code = ticker.replace('.KS', '')
            url = f'https://m.stock.naver.com/api/stock/{code}/basic'
        else: return None
        r = SESS.get(url, timeout=6, headers={'Referer': 'https://finance.naver.com'})
        if not r.ok: return None
        j = r.json()
        p  = float(str(j['closePrice']).replace(',', ''))
        cv = float(re.sub(r'[^0-9.\-]', '', str(j['fluctuationsRatio'])) or '0')
        return {'p': p, 'c': cv, 'src': 'Naver'} if p > 0 else None
    except: return None

def fetch_stooq(ticker):
    try:
        sym = '^nkx' if ticker == '^N225' else None
        if not sym: return None
        r = SESS.get(f'https://stooq.com/q/d/l/?s={sym}&i=d', timeout=6)
        if not r.ok: return None
        rows = [l for l in r.text.strip().split('\n') if l and l[0].isdigit()]
        if len(rows) < 2: return None
        last, prev = rows[-1].split(','), rows[-2].split(',')
        p, p0 = float(last[4]), float(prev[4])
        return {'p': p, 'c': (p - p0) / p0 * 100 if p0 else 0, 'src': 'Stooq'} if p > 0 else None
    except: return None

def get_price(ticker):
    res = fetch_yahoo(ticker)
    if res: return res
    res = fetch_naver(ticker)
    if res: return res
    return fetch_stooq(ticker)

print(f'[{TS}] Fetching {len(TICKERS)} tickers...')
DATA = {}
ok = naver_fb = stooq_fb = 0
for name, sym in TICKERS.items():
    res = get_price(sym)
    if res:
        DATA[name] = res; ok += 1
        if res['src'] == 'Naver': naver_fb += 1
        if res['src'] == 'Stooq': stooq_fb += 1
        sg = '+' if res['c'] >= 0 else ''
        print(f"  OK  {name:<14} {res['p']:>14.2f}  ({sg}{res['c']:.2f}%)  [{res['src']}]")
    else:
        print(f"  FAIL {name}")
    time.sleep(0.12)
print(f'Fetched: {ok}/{len(TICKERS)}  Yahoo:{ok-naver_fb-stooq_fb} Naver:{naver_fb} Stooq:{stooq_fb}')

# ─────────────────────────────────────────
# 2. Claude API
# ─────────────────────────────────────────
AI = None
if API_KEY:
    print('\nCalling Claude API...')
    lines = [f'time: {TS}']
    for k in sorted(DATA):
        v = DATA[k]
        lines.append(f"{k}: {v['p']:.4f} ({v['c']:.2f}%)")
    summary = '\n'.join(lines)

    sys_prompt = 'You are a global macro investment expert. Answer ONLY in Korean. Output ONLY pure JSON (no markdown, no code block).'
    user_msg = (
        f'[Market Data KST]\n{summary}\n\n'
        '[JSON Format]{"alert":"1-line summary in Korean (max120chars with key figures)",'
        '"news":[{"tag":"tw|te|tm|tg|tn|tk","time":"3/12 AM","title":"Korean title","desc":"Korean desc"}],'
        '"sc_long":{"sum":"Korean summary","drivers":[{"t":"title","d":"desc"}],"oil":"price path","kospi":"KOSPI forecast"},'
        '"sc_short":{"sum":"Korean summary","drivers":[{"t":"title","d":"desc"}],"oil":"price path","kospi":"KOSPI forecast"},'
        '"stocks":[{"name":"Korean name","theme":"Korean theme","tc":"tsm|tdf|ten|trv|tlg","act":"Korean action","ac":"aby|aho|awa|awn","risk":3,"desc":"Korean desc"}]}'
        '\n[Code Rules] tag:tw=war te=econ tm=market tg=geo tn=energy tk=korea | tc:tsm=chip tdf=defense ten=energy trv=contrarian tlg=LNG'
        '\n[Rules] ac: aby=BUY aho=HOLD awa=WATCH awn=CAUTION | news:12 KMW(032500)+29.8% today 3/19ER include | stocks:9 (Samsung SK Hynix SamsungBio Hanwha LIG KorCarbon KOGAS KEPCO SOil) | sc_long drivers:5-6 sc_short:4-5 | Iran war(2/28) Hormuz blockade context | ALL text values in Korean'
    )
    try:
        resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': API_KEY,
                'anthropic-version': '2023-06-01',
                'Content-Type': 'application/json',
            },
            json={
                'model': 'claude-sonnet-4-20250514',
                'max_tokens': 4096,
                'system': sys_prompt,
                'messages': [{'role': 'user', 'content': user_msg}],
            },
            timeout=120,
        )
        if resp.ok:
            raw = resp.json()['content'][0]['text'].strip()
            raw = re.sub(r'^```(?:json)?', '', raw).rstrip('`').strip()
            AI = json.loads(raw)
            print(f'Claude OK: {str(AI.get("alert",""))[:60]}')
        else:
            print(f'Claude HTTP {resp.status_code}: {resp.text[:200]}')
    except Exception as e:
        print(f'Claude error: {e}')
else:
    print('No ANTHROPIC_API_KEY — skipping AI')

# ─────────────────────────────────────────
# 3. HTML 패치
# ─────────────────────────────────────────
def HE(s):
    return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')
def JE(s):
    return str(s).replace('\\','\\\\').replace("'","\\'")
def patch(html, mks, mke, content):
    si, ei = html.find(mks), html.find(mke)
    if si >= 0 and ei > si:
        return html[:si] + mks + content + mke + html[ei+len(mke):]
    return html

print('\nPatching index.html...')
html = open(HTML_FILE, encoding='utf-8').read()

# title timestamp
html = re.sub(r'<title>.*?</title>', f'<title>HLOOMBERG TERMINAL · {NOW.strftime("%m/%d %H:%M")}</title>', html, flags=re.S)

# P1: alert
alert_txt = HE(AI['alert']) if AI else f'{TS} (AI unavailable)'
html = patch(html, '<!-- ##ALERT_S## -->', '<!-- ##ALERT_E## -->', f'<div class="alert">>> {alert_txt}</div>')

# P2: ticks
tick_objs = []
for k, l, dp in TMAP:
    d = DATA.get(k)
    if not d: continue
    sg = '+' if d['c'] >= 0 else ''
    cl = 'up' if d['c'] >= 0 else 'dn'
    pv = f"{d['p']:,.{dp}f}"
    tick_objs.append(f"  {{l:'{l}',v:'{pv}',c:'{sg}{d['c']:.2f}%',cl:'{cl}'}}")
ticks_js = '\nconst TICKS=[\n' + ',\n'.join(tick_objs) + '\n];\n'
html = patch(html, '// ##TICKS_S##', '// ##TICKS_E##', ticks_js)
print(f'  ticks: {len(tick_objs)}')

# P3: news
if AI and AI.get('news'):
    news_html = '\n' + ''.join(
        f'<div class="nc"><div class="nm"><span class="tag {HE(n.get("tag","tk"))}">{HE(n.get("tag","tk"))}</span>'
        f'<span class="nt">{HE(n.get("time",""))}</span></div>'
        f'<div class="ni">{HE(n.get("title",""))}</div>'
        f'<div class="nd">{HE(n.get("desc",""))}</div></div>\n'
        for n in AI['news']
    )
    html = patch(html, '<!-- ##NEWS_S## -->', '<!-- ##NEWS_E## -->', news_html)
    print(f'  news: {len(AI["news"])}')

# P4: stocks
if AI and AI.get('stocks'):
    lines = ['const STOCKS=[']
    for s in AI['stocks']:
        rk = max(1, min(5, int(s.get('risk', 3))))
        lines.append(f"  {{name:'{JE(s.get('name',''))}',th:'{JE(s.get('theme',''))}',tc:'{s.get('tc','')}',act:'{JE(s.get('act',''))}',ac:'{s.get('ac','')}',risk:{rk},desc:'{JE(s.get('desc',''))}'}},")
    lines.append('];')
    html = patch(html, '// ##STOCKS_S##', '// ##STOCKS_E##', '\n' + '\n'.join(lines) + '\n')
    print(f'  stocks: {len(AI["stocks"])}')

# P5: SCD
if AI and AI.get('sc_long') and AI.get('sc_short'):
    def jd(drivers):
        items = [f"{{t:'{JE(d.get('t',''))}',d:'{JE(d.get('d',''))}'}}" for d in (drivers or [])]
        return '[' + ','.join(items) + ']'
    sl, ss = AI['sc_long'], AI['sc_short']
    sj = (
        "const SCD={\n  long:{\n"
        f"    col:'var(--red)',\n    sum:'{JE(sl.get('sum',''))}',\n    drivers:{jd(sl.get('drivers',[]))},\n"
        f"    oil:'{JE(sl.get('oil',''))}',\n    kospi:'{JE(sl.get('kospi',''))}',\n  }},\n  short:{{\n"
        f"    col:'var(--green)',\n    sum:'{JE(ss.get('sum',''))}',\n    drivers:{jd(ss.get('drivers',[]))},\n"
        f"    oil:'{JE(ss.get('oil',''))}',\n    kospi:'{JE(ss.get('kospi',''))}',\n  }}\n}};"
    )
    html = patch(html, '// ##SCD_S##', '// ##SCD_E##', '\n' + sj + '\n')
    print('  SCD: OK')

open(HTML_FILE, 'w', encoding='utf-8').write(html)
print(f'\nDone — {TS}')
