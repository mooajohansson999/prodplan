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

DROPBOX_FOLDER = ''
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


def excel_to_json(content):
    wb = load_workbook(BytesIO(content), data_only=True)
    result = {}

    def norm(x):
        return str(x).strip().lower().replace(':','') if x is not None else ''

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        # Hoppa över rådata-blad (0.x)
        if sheet_name.strip().startswith('0'):
            print(f'Hoppar över rådata-blad: {sheet_name}')
            continue

        # === Normal sheet parsing ===
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

    # Merge normal sheet data - only overwrite if new row has real data
    for sheet_name, sheet_data in data.items():
        if sheet_name not in all_data[typ]:
            all_data[typ][sheet_name] = {}
        for date_key, row in sheet_data.items():
            has_real_data = any(
                isinstance(v, (int, float)) and v != 0
                for k, v in row.items()
                if k.upper() != 'DATUM'
            )
            if date_key not in all_data[typ][sheet_name] or has_real_data:
                all_data[typ][sheet_name][date_key] = row

    print(f'  → {len(data)} flikar från {name}')

# Save JSON files
for typ, data in all_data.items():
    if data:
        out_path = os.path.join(OUTPUT_DIR, f'{typ}.json')
        with open(out_path, 'w', encoding='utf-8') as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        print(f'Sparade {out_path} med {len(data)} flikar')

# ==============================
# SAVE LAST SYNC TIME
# ==============================

ts = datetime.datetime.now(ZoneInfo('Europe/Stockholm')).strftime('%Y-%m-%d %H:%M')
with open(os.path.join(OUTPUT_DIR, 'last_synced.json'), 'w') as fh:
    json.dump({'last_synced': ts}, fh)

print(f'Klar! Synkad {ts}')
