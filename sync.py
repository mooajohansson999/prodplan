import os
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

DROPBOX_FOLDER = ''   # Scoped app ser bara sin egen mapp
OUTPUT_DIR = 'data'

FILE_MAP = {
    'mål': 'mal',
    'mal': 'mal',
    'produktionsr': 'utfall',
    'uträkning': 'utfall',
    'utfall': 'utfall',
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

    def norm(x):
        return str(x).strip().lower().replace(':','') if x is not None else ''

    for sheet_name in wb.sheetnames:

        # Hoppa över rådata-flikar
        if sheet_name.strip().startswith('0'):
            print(f'Hoppar över rådata-blad: {sheet_name}')
            continue

        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        # 1️⃣ Hitta header-rad (letar efter DATUM)
        header_row_idx = None
        for i in range(min(60, len(rows))):
            if any(norm(c) in ('datum', 'date') for c in rows[i]):
                header_row_idx = i
                break

        if header_row_idx is None:
            print(f'⚠️ Ingen header med DATUM hittades i: {sheet_name}')
            continue

        headers = [str(h).strip() if h is not None else '' for h in rows[header_row_idx]]

        # Hitta datumkolumn
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

            date_key = None

            # 2️⃣ Konvertera datum
            if isinstance(date_val, datetime.datetime):
                date_key = date_val.strftime('%Y-%m-%d')

            elif isinstance(date_val, datetime.date):
                date_key = date_val.strftime('%Y-%m-%d')

            elif isinstance(date_val, (int, float)):
                base = datetime.datetime(1899, 12, 30)
                try:
                    date_key = (base + datetime.timedelta(days=float(date_val))).strftime('%Y-%m-%d')
                except Exception:
                    continue

            elif isinstance(date_val, str):
                s = date_val.strip()
                try:
                    if '-' in s:
                        date_key = s[:10]
                    elif '/' in s:
                        parts = s.split('/')
                        if len(parts[0]) == 4:  # YYYY/MM/DD
                            date_key = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                        else:  # DD/MM/YYYY
                            date_key = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                except Exception:
                    continue

            if not date_key:
                continue

            # 3️⃣ Rensa raden
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

            if clean:
                sheet_data[date_key] = clean

        if sheet_data:
            result[sheet_name] = sheet_data
            print(f'✅ {sheet_name}: {len(sheet_data)} rader')
        else:
            print(f'⚠️ {sheet_name}: inga rader efter parsing')

    return result

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
# MAIN SYNC LOOP
# ==============================

files = list_files()
print(f'Hittade {len(files)} filer/mappar i Dropbox (rekursivt)')

# Samla data per typ från ALLA filer
all_data = {'utfall': {}, 'mal': {}}

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
    data = excel_to_json(content)

    # Merga in i samlad data – nya flikar läggs till, befintliga uppdateras
    for sheet_name, sheet_data in data.items():
        if sheet_name not in all_data[typ]:
            all_data[typ][sheet_name] = {}
        all_data[typ][sheet_name].update(sheet_data)

    print(f'  → {len(data)} flikar från {name}')

# Spara samlade JSON-filer
for typ, data in all_data.items():
    if data:
        out_path = os.path.join(OUTPUT_DIR, f'{typ}.json')
        with open(out_path, 'w', encoding='utf-8') as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        print(f'Sparade {out_path} med {len(data)} flikar')

# ==============================
# SAVE LAST SYNC TIME (svensk tid)
# ==============================

ts = datetime.datetime.now(ZoneInfo('Europe/Stockholm')).strftime('%Y-%m-%d %H:%M')
with open(os.path.join(OUTPUT_DIR, 'last_synced.json'), 'w') as fh:
    json.dump({'last_synced': ts}, fh)

print(f'Klar! Synkad {ts}')
