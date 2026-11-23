import os
import re
import sys
import json
import requests
import datetime
from zoneinfo import ZoneInfo
from openpyxl import load_workbook
from io import BytesIO

# ==============================
# DROPBOX AUTH
# ==============================

REFRESH_TOKEN = os.environ['DP_TOKEN']
APP_KEY = os.environ['DP_APP_TOKEN']
APP_SECRET = os.environ['DP_SECRET']

def get_access_token():
    r = requests.post(
        'https://api.dropbox.com/oauth2/token',
        data={
            'grant_type': 'refresh_token',
            'refresh_token': REFRESH_TOKEN,
            'client_id': APP_KEY,
            'client_secret': APP_SECRET,
        }
    )
    print('Token svar:', r.status_code)
    r.raise_for_status()
    return r.json()['access_token']

ACCESS_TOKEN = get_access_token()
HEADERS = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

DROPBOX_FOLDER = ''
OUTPUT_DIR = 'data'

FILE_MAP = {
    'provisionstrappa': 'trappa',   # NYTT: Provisionstrappa_ÅÅÅÅ-MM.xlsx (måste stå före 'provision')
    'provision': 'provision',       # NYTT: Provision_ÅÅÅÅ-MM.xlsx
    'mål': 'mal',
    'mal': 'mal',
    'produktionsr': 'utfall',
    'uträkning': 'utfall',
    'utfall': 'utfall',
}

# Rådata-blad som ska exporteras separat
RAWDATA_SHEETS = {
    '0.1 data försäljning': 'rawdata_orders',
    '0.0 data timmar': 'rawdata_timmar',
    '0.2 data säljrapport': 'saljrapport',   # NYTT: teamledarnas rapporter per säljare och dag
}

# NYTT: bara dessa kolumner får lämna Excel. Kunduppgifter (kundnummer, personnummer,
# namn, telefon, e-post, adress, prospekt-ID) exporteras aldrig till repot.
RAWDATA_FIELDS = {
    'rawdata_orders': ['Kund', 'Projekt', 'Datum', 'År-månad', 'Produkt', 'Produkttyp',
                       'Produktkod', 'Säljare', 'LoxysoftID', 'kolumn?'],
    'rawdata_timmar': ['Kund', 'Projekt', 'Säljare', 'Loxysoft ID', 'Datum', 'År-månad',
                       'Inloggad tid timmar', 'Mansdagar',
                       # NYTT: för säljarsidans effektivitet
                       'Väntetid timmar', 'Samtalstid timmar', 'Registrerade samtal', 'Samtal - tel.svar',
                       'Antal nej tack', 'Antal bortfal', 'Hitrate', 'Genomsnittlig samtalstid timmar'],
    'saljrapport': ['Datum', 'Säljare', 'LoxyID', 'Projekt',
                    'Bredband', 'TV', 'MBB', 'Mobil', 'Streaming', 'TV BOX', 'Trygghetspaket', 'Total',
                    'Order Bredband', 'Order TV', 'Order MBB', 'Order Mobil', 'Order Streaming',
                    'Order TV BOX', 'Order Trygghetspaket', 'Order total', 'Differens', 'Status'],
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==============================
# DROPBOX FILE FUNCTIONS
# ==============================

def list_files():
    all_entries = []
    r = requests.post(
        'https://api.dropboxapi.com/2/files/list_folder',
        headers=HEADERS,
        json={'path': DROPBOX_FOLDER, 'recursive': True}
    )
    print('list_folder status:', r.status_code)
    r.raise_for_status()
    data = r.json()
    all_entries.extend(data.get('entries', []))

    while data.get('has_more'):
        r = requests.post(
            'https://api.dropboxapi.com/2/files/list_folder/continue',
            headers=HEADERS,
            json={'cursor': data['cursor']}
        )
        r.raise_for_status()
        data = r.json()
        all_entries.extend(data.get('entries', []))

    return all_entries

def download_file(path):
    r = requests.post(
        'https://content.dropboxapi.com/2/files/download',
        headers={
            'Authorization': f'Bearer {ACCESS_TOKEN}',
            'Dropbox-API-Arg': json.dumps({'path': path})
        }
    )
    r.raise_for_status()
    return r.content

# ==============================
# ROBUST EXCEL PARSER
# ==============================

def excel_to_json(content):
    wb = load_workbook(BytesIO(content), data_only=True)
    result = {}
    rawdata = {}

    def norm(x):
        return str(x).strip().lower().replace(':','') if x is not None else ''

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        # NYTT: dolda flikar hoppas över – en synlig flik i uträknaren = ett projekt i prodplanen
        if ws.sheet_state != 'visible':
            print(f'Hoppar över dold flik: {sheet_name}')
            continue

        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        # Check if this is a rawdata sheet
        sheet_lower = sheet_name.strip().lower()
        raw_key = None
        for pattern, key in RAWDATA_SHEETS.items():
            if sheet_lower.startswith(pattern) or pattern in sheet_lower:
                raw_key = key
                break

        if raw_key:
            # Parse rawdata sheet as flat array of rows
            raw_rows = parse_rawdata_sheet(sheet_name, rows, raw_key)
            if raw_rows:
                rawdata[raw_key] = rawdata.get(raw_key, []) + raw_rows
                print(f'✅ RÅDATA {sheet_name}: {len(raw_rows)} rader')
            else:
                print(f'⚠️ RÅDATA {sheet_name}: inga rader efter parsing')
            continue

        # Skip other "0." sheets that aren't mapped
        if sheet_name.strip().startswith('0'):
            print(f'Hoppar över okänt rådata-blad: {sheet_name}')
            continue

        # === Normal sheet parsing (unchanged) ===
        header_row_idx = None
        for i in range(min(60, len(rows))):
            if any(norm(c) in ('datum', 'date') for c in rows[i]):
                header_row_idx = i
                break

        if header_row_idx is None:
            print(f'⚠️ Ingen header med DATUM hittades i: {sheet_name}')
            continue

        headers = [str(h).strip() if h is not None else '' for h in rows[header_row_idx]]

        date_col = None
        for h in headers:
            if norm(h) in ('datum', 'date'):
                date_col = h
                break

        if not date_col:
            print(f'⚠️ Hittade header men ingen datum-kolumn i: {sheet_name}')
            continue

        sheet_data = {}

        for row in rows[header_row_idx + 1:]:
            row_dict = dict(zip(headers, row))
            date_val = row_dict.get(date_col)

            if not date_val:
                continue

            date_key = parse_date(date_val)
            if not date_key:
                continue

            clean = clean_row(row_dict)
            if clean:
                sheet_data[date_key] = clean

        if sheet_data:
            result[sheet_name] = sheet_data
            print(f'✅ {sheet_name}: {len(sheet_data)} rader')
        else:
            print(f'⚠️ {sheet_name}: inga rader efter parsing')

    return result, rawdata


def parse_rawdata_sheet(sheet_name, rows, raw_key):
    """Parse a rawdata sheet into a flat list of row dicts (only allowed columns)."""
    def norm(x):
        return str(x).strip().lower().replace(':','') if x is not None else ''

    allowed = {f.strip().lower() for f in RAWDATA_FIELDS.get(raw_key, [])}

    # Find header row
    header_row_idx = None
    for i in range(min(20, len(rows))):
        cells = [norm(c) for c in rows[i]]
        if 'datum' in cells or 'date' in cells:
            header_row_idx = i
            break
        # Also check for 'säljare' or 'projekt' as header indicators
        if 'säljare' in cells and 'projekt' in cells:
            header_row_idx = i
            break

    if header_row_idx is None:
        print(f'⚠️ Ingen header hittades i rådata: {sheet_name}')
        return []

    headers = [str(h).strip() if h is not None else '' for h in rows[header_row_idx]]

    parsed = []
    for row in rows[header_row_idx + 1:]:
        row_dict = dict(zip(headers, row))

        # Skip completely empty rows
        if all(v is None or str(v).strip() == '' for v in row):
            continue

        clean = clean_row(row_dict)

        # NYTT: behåll bara tillåtna kolumner – inga kunduppgifter lämnar Excel
        clean = {k: v for k, v in clean.items() if str(k).strip().lower() in allowed}
        if not clean:
            continue

        # Convert any date fields to string
        for k, v in clean.items():
            if isinstance(v, (datetime.datetime, datetime.date)):
                clean[k] = v.strftime('%Y-%m-%d')

        # Ensure Datum is a proper date string
        date_val = clean.get('Datum') or clean.get('datum')
        if date_val:
            parsed_date = parse_date(date_val)
            if parsed_date:
                clean['Datum'] = parsed_date

        parsed.append(clean)

    return parsed


def parse_date(date_val):
    """Convert various date formats to YYYY-MM-DD string."""
    if isinstance(date_val, datetime.datetime):
        return date_val.strftime('%Y-%m-%d')
    elif isinstance(date_val, datetime.date):
        return date_val.strftime('%Y-%m-%d')
    elif isinstance(date_val, (int, float)):
        base = datetime.datetime(1899, 12, 30)
        try:
            return (base + datetime.timedelta(days=float(date_val))).strftime('%Y-%m-%d')
        except Exception:
            return None
    elif isinstance(date_val, str):
        s = date_val.strip()
        try:
            if '-' in s:
                return s[:10]
            elif '/' in s:
                parts = s.split('/')
                if len(parts[0]) == 4:
                    return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                else:
                    return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        except Exception:
            return None
    return None


def clean_row(row_dict):
    """Clean a row dict, removing empty keys and converting types."""
    clean = {}
    for k, v in row_dict.items():
        if not k or str(k).strip() == '':
            continue
        if isinstance(v, datetime.datetime):
            clean[k] = v.strftime('%Y-%m-%d')
        elif isinstance(v, datetime.date):
            clean[k] = v.strftime('%Y-%m-%d')
        elif isinstance(v, (int, float)):
            clean[k] = v
        elif v is not None and str(v).strip() != '':
            clean[k] = str(v)
    return clean


# ==============================
# NYTT: SÄLJARSIDAN (individ.json) OCH LÖN
# ==============================
# Lönen skrivs bara ut när PUBLICERA_LON är satt till "ja" (GitHub-variabel), alltså först när
# prodplanen ligger bakom inloggning. Annars innehåller individ.json allt utom lön.
PUBLICERA_LON = os.environ.get('PUBLICERA_LON', '').strip().lower() in ('ja', 'true', '1')
SEMESTERFAKTOR = 1.12

# Trappräknare: steget bestäms av säljarens totala antal trappsatta produkter i projektet under månaden,
# och hela månaden prissätts efter det steget. Varje projekt har sin egen räknare, även för säljare som
# säljer i flera projekt (till exempel både Telia FMC och Telia Mobil).
# Familjeabonnemang räknas in i LAN och Koax men behåller fast pris.
# TRAPPGRUPPER kan slå ihop projekt till en gemensam räknare om det skulle behövas igen, t.ex.
# {'telia fmc': 'telia mobil och fmc', 'telia mobil': 'telia mobil och fmc'}. Tom = separata trappor.
TRAPPGRUPPER = {}
HUSKONTON = {6666, 66666}   # Viasales huskonto visas aldrig på säljarsidan


def n_txt(s):
    return ' '.join(str(s).split()).lower() if s is not None else ''


def n_num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# Produkter med fast pris som ändå räknas in i trappsteget: (projektet börjar med, produkt).
# Familjeabonnemang (250 kr) och Streaming Nyteckning (300 kr) i alla Tele2-projekt och bara där: båda räknas mot
# steget men behåller sitt fasta pris. Beslut 18–19 nov 2026, avstämt mot PBI.
RAKNAS_I_TRAPPAN = [('tele2', 'familjeabonnemang'), ('tele2', 'streaming nyteckning')]


def raknas_i_trappan(proj, prod):
    return any(proj.startswith(p) and prod == x for p, x in RAKNAS_I_TRAPPAN)


def parse_provision_file(content):
    """Läser Provision- eller Provisionstrappa-filen till en lista med rader (första synliga bladet)."""
    wb = load_workbook(BytesIO(content), data_only=True)
    ws = next(w for w in wb.worksheets if w.sheet_state == 'visible')
    rows = list(ws.iter_rows(values_only=True))
    hi = next((i for i, r in enumerate(rows[:20]) if 'Projekt' in [str(c).strip() for c in r if c is not None]
               and 'Produkt' in [str(c).strip() for c in r if c is not None]), None)
    if hi is None:
        return []
    heads = [str(h).strip() if h is not None else '' for h in rows[hi]]
    out, last = [], {}
    for r in rows[hi + 1:]:
        d = dict(zip(heads, r))
        if not d.get('Projekt') or not d.get('Produkt'):
            continue
        for k in ('Säljare', 'LoxysoftID'):          # formler som =E2 utan sparat värde ärver blockets första rad
            if k in d and (d[k] is None or str(d[k]).startswith('=')):
                d[k] = last.get(k)
            elif k in d:
                last[k] = d[k]
        out.append(d)
    return out


def startdatum(r):
    """Från vilket datum en rad i provisionsfilerna gäller: kolumnen 'Gäller från' om den är ifylld,
    annars första dagen i radens År-månad. Utan båda gäller raden från början."""
    g = r.get('Gäller från')
    if isinstance(g, (datetime.datetime, datetime.date)):
        return g.strftime('%Y-%m-%d')
    s = str(g or '').strip()
    t = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})', s)
    if t:
        return f'{int(t.group(1)):04d}-{int(t.group(2)):02d}-{int(t.group(3)):02d}'
    t = re.match(r'^(\d{1,2})[./](\d{1,2})[./](\d{4})', s)          # 1.9.2026 eller 1/9/2026
    if t:
        return f'{int(t.group(3)):04d}-{int(t.group(2)):02d}-{int(t.group(1)):02d}'
    am = str(r.get('År-månad') or '').strip()[:7]
    return am + '-01' if re.match(r'^\d{4}-\d{2}$', am) else ''


def senaste(lista, datum):
    """Den version (start, ...) i en lista sorterad på start som gäller på datumet, eller None."""
    val = None
    for x in lista:
        if x[0] <= datum:
            val = x
        else:
            break
    return val


def bygg_lonetabeller(prov_rows, trappa_rows):
    """Prislistor med startdatum. Varje rad gäller från sitt startdatum tills en nyare rad för samma projekt
    och produkt tar över, så en ändring från ett visst datum påverkar inte tidigare dagar och månader.
      fast:    (projekt, produkt) -> [(start, belopp), ...]
      trappor: (projekt, produkt) -> [(start, standardtrappa, {säljar-ID: trappa}), ...]"""
    fast, trappv, visning = {}, {}, {}
    for r in prov_rows:
        k = (n_txt(r['Projekt']), n_txt(r['Produkt']))
        visning.setdefault(k, (' '.join(str(r['Projekt']).split()), ' '.join(str(r['Produkt']).split())))
        fast.setdefault(k, {})[startdatum(r)] = n_num(r.get('Ersättning Ut'))   # samma startdatum två gånger: sista raden gäller
    fast = {k: sorted(v.items()) for k, v in fast.items()}
    for r in trappa_rows:
        k = (n_txt(r['Projekt']), n_txt(r['Produkt']))
        visning.setdefault(k, (' '.join(str(r['Projekt']).split()), ' '.join(str(r['Produkt']).split())))
        sid = int(n_num(r.get('LoxysoftID')))
        steg = (n_num(r.get('Intervall start')), n_num(r.get('Intervall slut')), n_num(r.get('Provisionsbelopp')))
        trappv.setdefault(k, {}).setdefault(startdatum(r), {}).setdefault(sid, set()).add(steg)
    trappor = {}
    for k, vers in trappv.items():
        lista, std, undantag = [], None, {}
        for start in sorted(vers):
            per = vers[start]
            egna = {s: sorted(v) for s, v in per.items() if s != 0}
            if 0 in per:                                    # hel trappa (rader utan säljare): ersätter tidigare undantag
                std, undantag = sorted(per[0]), egna
            elif std is not None:                           # bara undantag från datumet: övriga säljare behåller trappan
                undantag = dict(undantag); undantag.update(egna)
            else:                                           # äldre format med en rad per säljare: vanligaste trappan gäller
                var = {}
                for st in egna.values():
                    var[tuple(st)] = var.get(tuple(st), 0) + 1
                std, undantag = list(max(var, key=var.get)), egna
            lista.append((start, std, undantag))
        trappor[k] = lista
    projstart = {}                                          # första datum som varje projekt har provision
    for (proj, _), v in list(fast.items()) + list(trappor.items()):
        projstart[proj] = min(projstart.get(proj, v[0][0]), v[0][0])
    starter = sorted({s for v in fast.values() for s, _ in v} | {x[0] for v in trappor.values() for x in v})
    return fast, trappor, projstart, starter, visning


def steg_pris(steglista, antal):
    traff = [s for s in steglista if s[0] <= antal <= s[1]]
    if not traff:   # över sista steget, eller i ett glapp mellan två steg: steget som senast började gäller
        traff = [s for s in steglista if s[0] <= antal] or [min(steglista, key=lambda s: s[0])]
    return max(traff, key=lambda s: s[0])[2]             # vid överlapp (t.ex. 31 i två steg) gäller högre steg


def steg_nr(granser, antal):
    """Vilket steg (1, 2, …) ett antal hamnar på, med samma regler som steg_pris."""
    idx = [i for i, g in enumerate(granser) if g[0] <= antal <= g[1]] or [i for i, g in enumerate(granser) if g[0] <= antal] or [0]
    return max(idx, key=lambda i: granser[i][0]) + 1


def bygg_individ(orders, timmar, prov_rows, trappa_rows, med_lon):
    fast, trappor, projstart, starter, visning = bygg_lonetabeller(prov_rows, trappa_rows)
    utan_lon = {}   # projekt utan provision under månaden: ingen lön och ingen varning om saknade priser

    def prisinfo(proj, prod, datum):
        """('trappa', version) eller ('fast', belopp) enligt raden som gäller på datumet, annars None.
        Har både trappan och det fasta priset startdatum gäller det som började senast; samma datum: trappan."""
        lv, fv = senaste(trappor.get((proj, prod), []), datum), senaste(fast.get((proj, prod), []), datum)
        if lv and (not fv or lv[0] >= fv[0]):
            return ('trappa', lv)
        return ('fast', fv[1]) if fv else None

    def har_provision(proj, datum):
        s = projstart.get(proj)
        return s is not None and s <= datum

    def prov_manad(m):
        """Månaden för den senaste ändringen i provisionsfilerna som gäller i månaden (visas på säljarsidan)."""
        tidigare = [s for s in starter if s <= m + '-31']
        return (tidigare[-1][:7] or m) if tidigare else None      # rader utan datum gäller från början

    giltiga = [o for o in orders if str(o.get('Produkttyp', '')).strip().upper() != 'TA BORT' and o.get('Datum')
               and int(n_num(o.get('LoxysoftID'))) not in HUSKONTON]
    # 1. Trappräknare per säljare, månad och trappgrupp
    raknare = {}
    for o in giltiga:
        m, d = o['Datum'][:7], o['Datum'][:10]
        proj, prod, sid = n_txt(o.get('Projekt')), n_txt(o.get('Produkt')), int(n_num(o.get('LoxysoftID')))
        pi = prisinfo(proj, prod, d)
        if (pi and pi[0] == 'trappa') or (har_provision(proj, d) and raknas_i_trappan(proj, prod)):
            k = (m, sid, TRAPPGRUPPER.get(proj, proj))
            raknare[k] = raknare.get(k, 0) + 1

    # 2. Rader per projekt, säljare och dag
    agg, namn, saknar, tillagg = {}, {}, {}, {}   # tillagg: vilka produkter som är tillägg, per projekt
    def rad(m, proj, sid, datum):
        return agg.setdefault((m, proj, sid, datum), {'prod': 0, 'till': 0, 'md': 0.0, 'inl': 0.0, 'vant': 0.0,
                                                      'samt': 0.0, 'samtal': 0.0, 'telsvar': 0.0, 'nej': 0.0,
                                                      'bortfall': 0.0, 'hit_w': 0.0, 'snitt_w': 0.0, 'lon': 0.0, 'pr': {}, 'kat': {}})
    for o in giltiga:
        m, proj_namn = o['Datum'][:7], ' '.join(str(o.get('Projekt', '')).split())
        proj, prod, sid = n_txt(proj_namn), n_txt(o.get('Produkt')), int(n_num(o.get('LoxysoftID')))
        r = rad(m, proj_namn, sid, o['Datum'][:10])
        typ = str(o.get('Produkttyp', '')).strip().lower()
        if typ == 'produkt':
            r['prod'] += 1
        elif typ.startswith('till'):
            r['till'] += 1
        namn.setdefault(sid, {}).setdefault(' '.join(str(o.get('Säljare', '')).split()), 0)
        namn[sid][' '.join(str(o.get('Säljare', '')).split())] += 1
        r['pr'][str(o.get('Produkt'))] = r['pr'].get(str(o.get('Produkt')), 0) + 1   # produkter per säljare, dag och projekt
        if typ == 'produkt':                                                         # kategori enligt uträknaren, t.ex. Bredband, TV, Mobil
            kt = ' '.join(str(o.get('kolumn?') or 'Övrigt').split()) or 'Övrigt'
            r['kat'][kt] = r['kat'].get(kt, 0) + 1
        if typ.startswith('till'):
            tillagg.setdefault(m, {}).setdefault(proj_namn, set()).add(str(o.get('Produkt')))
        d = o['Datum'][:10]
        if med_lon and not har_provision(proj, d):
            if projstart.get(proj) is None or projstart[proj] > m + '-31':
                utan_lon.setdefault(m, set()).add(proj_namn)   # före projektets startdatum: ingen lön och ingen varning
        elif med_lon:
            pris, pi = None, prisinfo(proj, prod, d)
            if pi and pi[0] == 'trappa':
                ver = pi[1]
                pris = steg_pris(list(ver[2].get(sid) or ver[1]), raknare.get((m, sid, TRAPPGRUPPER.get(proj, proj)), 1))
            elif pi:
                pris = pi[1]
            if pris is None:
                sk = (m, proj_namn, str(o.get('Produkt')))
                saknar[sk] = saknar.get(sk, 0) + 1
            else:
                r['lon'] += pris * SEMESTERFAKTOR
    for t in timmar:
        if not t.get('Datum') or int(n_num(t.get('Loxysoft ID'))) in HUSKONTON:
            continue
        m, proj_namn, sid = t['Datum'][:7], ' '.join(str(t.get('Projekt', '')).split()), int(n_num(t.get('Loxysoft ID')))
        r = rad(m, proj_namn, sid, t['Datum'][:10])
        samtal = n_num(t.get('Registrerade samtal'))
        r['md'] += n_num(t.get('Mansdagar')); r['inl'] += n_num(t.get('Inloggad tid timmar'))
        r['vant'] += n_num(t.get('Väntetid timmar')); r['samt'] += n_num(t.get('Samtalstid timmar'))
        r['samtal'] += samtal; r['telsvar'] += n_num(t.get('Samtal - tel.svar'))
        r['nej'] += n_num(t.get('Antal nej tack')); r['bortfall'] += n_num(t.get('Antal bortfal'))
        r['hit_w'] += n_num(t.get('Hitrate')) * samtal
        r['snitt_w'] += n_num(t.get('Genomsnittlig samtalstid timmar')) * samtal
        nm = ' '.join(str(t.get('Säljare', '')).split())
        namn.setdefault(sid, {}).setdefault(nm, 0)
        namn[sid][nm] += 1

    falt = ['prod', 'till', 'md', 'inl', 'vant', 'samt', 'samtal', 'telsvar', 'nej', 'bortfall', 'hit_w', 'snitt_w']
    if med_lon:
        falt.append('lon')
    ut = {}
    for (m, proj, sid, datum), v in sorted(agg.items()):
        bast = max(namn.get(sid, {'': 1}).items(), key=lambda x: x[1])[0]
        man = ut.setdefault(m, {'kolumner': ['projekt', 'id', 'namn', 'datum'] + falt + ['pr', 'kat'], 'rader': [],
                                'lon': bool(med_lon and prov_manad(m)), 'provisionsmanad': prov_manad(m) if med_lon else None,
                                'andringar': [{'fran': s, 'projekt': visning[k][0], 'produkt': visning[k][1]}
                                              for k, s in sorted({(k, x[0]) for k, v in list(fast.items()) + list(trappor.items())
                                                                  for x in v if x[0][:7] == m and x[0][8:] != '01'}, key=lambda t: (t[1], t[0]))]
                                             if med_lon else [],
                                'saknar_pris': [],
                                'tillagg': {k: sorted(v) for k, v in tillagg.get(m, {}).items()},
                                'utan_lon': sorted(utan_lon.get(m, set()) | ({x for x in {a[1] for a in agg if a[0] == m}
                                                    if projstart.get(n_txt(x)) is None or projstart[n_txt(x)] > m + '-31'}
                                                   if med_lon and prov_manad(m) else set()))})
        man['rader'].append([proj, sid, bast, datum] + [round(v[f], 4) for f in falt] + [v['pr'], v['kat']])
    for (m, proj, prod), antal in sorted(saknar.items()):
        if m in ut:
            ut[m]['saknar_pris'].append({'projekt': proj, 'produkt': prod, 'antal': antal})
    # Steg per säljare och projekt: antal trappsatta produkter i månaden, steg, antal steg och antal kvar till nästa steg
    for m, man in ut.items():
        pm, steg = prov_manad(m), {}
        if pm:
            sista = max(a[3] for a in agg if a[0] == m)
            steggranser = {}                                 # projektets steggränser: de flesta produkternas, på sista dagen
            for (proj_k, prod_k), lista in trappor.items():
                ver = senaste(lista, sista)
                if ver:
                    g = tuple((s[0], s[1]) for s in sorted(ver[1]))
                    steggranser.setdefault(proj_k, {}).setdefault(g, 0)
                    steggranser[proj_k][g] += 1
            steggranser = {k: list(max(v, key=v.get)) for k, v in steggranser.items()}
            for (mm, proj, sid, datum) in agg:
                g = steggranser.get(n_txt(proj)) if mm == m else None
                if not g or str(sid) in steg.get(proj, {}):
                    continue
                n = raknare.get((m, sid, TRAPPGRUPPER.get(n_txt(proj), n_txt(proj))), 0)
                nr = steg_nr(g, n)
                steg.setdefault(proj, {})[str(sid)] = [n, nr, len(g), int(max(0, g[nr][0] - n)) if nr < len(g) else 0]
        man['steg'] = steg
    return ut


# ==============================
# FILE TYPE DETECTION
# ==============================

def detect_type(filename):
    fn = filename.lower()
    for keyword, typ in FILE_MAP.items():
        if keyword in fn:
            return typ
    return None

# ==============================
# SUPABASE: skyddad lagring av prodplanens data (synken skriver, sidorna läser med kod)
SUPABASE_URL = re.sub(r'/rest/v1/?$', '', os.environ.get('SUPABASE_URL', '').strip().rstrip('/'))
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '').strip()


def supabase_upsert(tabell, rader):
    """Skriver rader med den hemliga nyckeln. Finns raden redan (samma primärnyckel) uppdateras den."""
    h = {'apikey': SUPABASE_KEY, 'Content-Type': 'application/json', 'Prefer': 'resolution=merge-duplicates,return=minimal'}
    if not SUPABASE_KEY.startswith('sb_'):          # äldre service_role-nyckel (JWT) skickas även som Bearer
        h['Authorization'] = f'Bearer {SUPABASE_KEY}'
    r = requests.post(f'{SUPABASE_URL}/rest/v1/{tabell}', headers=h,
                      data=json.dumps(rader, ensure_ascii=False).encode('utf-8'), timeout=120)
    if r.status_code >= 300:
        raise RuntimeError(f'{tabell}: HTTP {r.status_code} {r.text[:300]}')


def saljarlista(individ):
    """Alla säljare med tid eller order: LoxysoftID, vanligaste namnet, projekt och senaste datum."""
    per = {}
    for man in individ.values():
        k = man['kolumner']
        for rad in man['rader']:
            r = dict(zip(k, rad))
            s = per.setdefault(int(r['id']), {'namn': {}, 'projekt': set(), 'senast': ''})
            s['namn'][r['namn']] = s['namn'].get(r['namn'], 0) + 1
            s['projekt'].add(r['projekt'])
            s['senast'] = max(s['senast'], r['datum'])
    return [{'id': i, 'namn': max(s['namn'], key=s['namn'].get), 'projekt': sorted(s['projekt']),
             'senast_sedd': s['senast'] or None} for i, s in sorted(per.items())]


def skriv_till_supabase(all_data, individ, ts, status=None):
    """Skriver det sidorna läser (utfall, mål, säljardata per månad, senaste synk) och säljarlistan.
    Supabase är enda platsen för datan, så ett fel stoppar körningen och syns i GitHub."""
    if not (SUPABASE_URL and SUPABASE_KEY):
        print('Supabase: hoppar över (SUPABASE_URL eller SUPABASE_SERVICE_KEY saknas)')
        return
    nu = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rader = [{'typ': typ, 'manad': 'alla', 'data': data, 'uppdaterad': nu} for typ, data in all_data.items() if data]
    rader += [{'typ': 'individ', 'manad': m, 'data': d, 'uppdaterad': nu} for m, d in sorted(individ.items())]
    rader.append({'typ': 'last_synced', 'manad': 'alla', 'data': {'last_synced': ts}, 'uppdaterad': nu})
    if status:
        rader.append({'typ': 'status', 'manad': 'alla', 'data': status, 'uppdaterad': nu})
    try:
        for rad in rader:                                # en datamängd per anrop håller anropen små
            supabase_upsert('prodplan_data', [rad])
        lista = [dict(s, uppdaterad=nu) for s in saljarlista(individ)]
        for i in range(0, len(lista), 500):
            supabase_upsert('saljare', lista[i:i + 500])
        print(f'Supabase: {len(rader)} datamängder och {len(lista)} säljare sparade')
    except Exception as e:
        print(f'::error::Supabase: kunde inte spara ({e}). Kontrollera hemligheterna SUPABASE_URL och SUPABASE_SERVICE_KEY.')
        sys.exit(1)


# MAIN SYNC LOOP
# ==============================

files = list_files()
print(f'Hittade {len(files)} filer/mappar i Dropbox (rekursivt)')

all_data = {'utfall': {}, 'mal': {}}
all_rawdata = {'rawdata_orders': [], 'rawdata_timmar': [], 'saljrapport': []}   # NYTT: saljrapport
all_prov = {'provision': [], 'trappa': []}   # NYTT: provisionsfilerna
prov_filer = {'provision': [], 'trappa': []}  # filnamnen, för felsökning av lönen

for f in files:
    if f['.tag'] != 'file':
        continue

    name = f['name']
    if not name.endswith(('.xlsx', '.xls')):
        continue

    typ = detect_type(name)
    if not typ:
        print(f'Okänd filtyp: {name}, hoppar över')
        continue

    print(f'Laddar ner: {name} ({typ}) från {f["path_lower"]}')
    content = download_file(f['path_lower'])

    # NYTT: provisionsfilerna läses som tabeller och används bara till lönen
    if typ in ('provision', 'trappa'):
        rows = parse_provision_file(content)
        all_prov[typ].extend(rows)
        prov_filer[typ].append(name)
        print(f'  → {len(rows)} rader från {name}')
        continue

    data, rawdata = excel_to_json(content)

    # Merge normal sheet data - only overwrite if new row has real data
    for sheet_name, sheet_data in data.items():
        if sheet_name not in all_data[typ]:
            all_data[typ][sheet_name] = {}
        for date_key, row in sheet_data.items():
            # Check if new row has any non-zero numeric values (besides DATUM)
            has_real_data = any(
                isinstance(v, (int, float)) and v != 0
                for k, v in row.items()
                if k.upper() != 'DATUM'
            )
            if date_key not in all_data[typ][sheet_name] or has_real_data:
                all_data[typ][sheet_name][date_key] = row

    # Merge rawdata
    for raw_key, raw_rows in rawdata.items():
        all_rawdata[raw_key].extend(raw_rows)

    print(f'  → {len(data)} flikar, {sum(len(v) for v in rawdata.values())} rådata-rader från {name}')

# ==============================
# SPARA: Supabase (skyddat, med lön) eller publika filer (utan lön)
# ==============================
SUPABASE_PA = bool(SUPABASE_URL and SUPABASE_KEY)
MED_LON = PUBLICERA_LON and SUPABASE_PA     # lön bara när datan ligger skyddad bakom kod
if PUBLICERA_LON and not SUPABASE_PA:
    print('::warning::PUBLICERA_LON är på men Supabase saknas. Lönen räknas inte, eftersom datan då skulle bli publik.')

individ = bygg_individ(all_rawdata['rawdata_orders'], all_rawdata['rawdata_timmar'],
                       all_prov['provision'], all_prov['trappa'], MED_LON)
print(f'Säljardata: {len(individ)} månader, lön {"med" if MED_LON else "avstängd"}')
ts = datetime.datetime.now(ZoneInfo('Europe/Stockholm')).strftime('%Y-%m-%d %H:%M')
LON_VARIABEL = os.environ.get('PUBLICERA_LON', '')
print(f'Lön: PUBLICERA_LON={LON_VARIABEL!r}, Supabase {"på" if SUPABASE_PA else "av"} → lön {"på" if MED_LON else "av"}')
print(f'Provisionsfiler: {", ".join(prov_filer["provision"]) or "inga"} | Trappfiler: {", ".join(prov_filer["trappa"]) or "inga"}')
# Synkens status följer med till sidan, så att säljarsidan kan visa varför lönen saknas
STATUS = {'tid': ts, 'lon_variabel': LON_VARIABEL, 'lon_pa': MED_LON,
          'provisionsfiler': prov_filer['provision'], 'trappfiler': prov_filer['trappa']}

if SUPABASE_PA:
    # Prodplanen läser från Supabase med kod. Gamla publika filer tas bort, så att de försvinner från sidan.
    for f in sorted(os.listdir(OUTPUT_DIR)):
        if f.endswith('.json'):
            os.remove(os.path.join(OUTPUT_DIR, f))
            print(f'Tog bort den publika filen data/{f}')
    skriv_till_supabase(all_data, individ, ts, STATUS)
else:
    for typ, data in all_data.items():
        if data:
            with open(os.path.join(OUTPUT_DIR, f'{typ}.json'), 'w', encoding='utf-8') as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)
            print(f'Sparade data/{typ}.json med {len(data)} flikar')
    with open(os.path.join(OUTPUT_DIR, 'individ.json'), 'w', encoding='utf-8') as fh:
        json.dump(individ, fh, ensure_ascii=False)
    with open(os.path.join(OUTPUT_DIR, 'last_synced.json'), 'w') as fh:
        json.dump({'last_synced': ts}, fh)
    print('Sparade data/individ.json och data/last_synced.json (publika, utan lön)')

print(f'Klar! Synkad {ts}')
