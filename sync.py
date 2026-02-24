import os, json, requests, datetime
from openpyxl import load_workbook
from io import BytesIO

REFRESH_TOKEN = os.environ['DP_TOKEN']
APP_KEY = 't15p5v3rcqofusj'
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
    r.raise_for_status()
    return r.json()['access_token']

ACCESS_TOKEN = get_access_token()
HEADERS = {'Authorization': f'Bearer {ACCESS_TOKEN}', 'Content-Type': 'application/json'}

DROPBOX_FOLDER = '/moa johansson/appar/produktionsplan'
OUTPUT_DIR = 'data'

# Mappning: filnamns-nyckelord -> typ
FILE_MAP = {
    'mål': 'mal',
    'mal': 'mal',
    'produktionsr': 'utfall',
    'utfall': 'utfall',
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

def list_files():
    # Testa rotnivån först för att se mappstruktur
    r = requests.post(
        'https://api.dropboxapi.com/2/files/list_folder',
        headers=HEADERS,
        json={'path': ''}
    )
    print('ROOT innehåll:', [e['name'] for e in r.json().get('entries', [])])

    r2 = requests.post(
        'https://api.dropboxapi.com/2/files/list_folder',
        headers=HEADERS,
        json={'path': DROPBOX_FOLDER}
    )
    print('Status:', r2.status_code)
    print('Svar:', r2.text[:500])
    r2.raise_for_status()
    return r2.json().get('entries', [])

def download_file(path):
    r = requests.post(
        'https://content.dropboxapi.com/2/files/download',
        headers={
            'Authorization': f'Bearer {TOKEN}',
            'Dropbox-API-Arg': json.dumps({'path': path})
        }
    )
    r.raise_for_status()
    return r.content

def excel_to_json(content):
    wb = load_workbook(BytesIO(content), data_only=True)
    result = {}
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        headers = [str(h).strip() if h is not None else '' for h in rows[0]]
        sheet_data = {}
        for row in rows[1:]:
            row_dict = dict(zip(headers, row))
            # Hitta datumkolumnen
            date_val = row_dict.get('DATUM') or row_dict.get('Datum') or row_dict.get('datum')
            if not date_val:
                continue
            if isinstance(date_val, datetime.datetime):
                date_key = date_val.strftime('%Y-%m-%d')
            elif isinstance(date_val, str) and '-' in date_val:
                date_key = date_val[:10]
            else:
                continue
            # Konvertera värden
            clean = {}
            for k, v in row_dict.items():
                if k == '':
                    continue
                if isinstance(v, datetime.datetime):
                    clean[k] = v.strftime('%Y-%m-%d')
                elif isinstance(v, (int, float)):
                    clean[k] = v
                elif v is not None:
                    clean[k] = str(v)
            sheet_data[date_key] = clean
        if sheet_data:
            result[sheet_name] = sheet_data
    return result

def detect_type(filename):
    fn = filename.lower()
    for keyword, typ in FILE_MAP.items():
        if keyword in fn:
            return typ
    return None

files = list_files()
print(f'Hittade {len(files)} filer i Dropbox-mappen')

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

    print(f'Laddar ner: {name} ({typ})')
    content = download_file(f['path_lower'])
    data = excel_to_json(content)

    out_path = os.path.join(OUTPUT_DIR, f'{typ}.json')
    with open(out_path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    print(f'Sparade {out_path} med {len(data)} flikar')

# Spara tidsstämpel för senast synkad
ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
with open(os.path.join(OUTPUT_DIR, 'last_synced.json'), 'w') as fh:
    json.dump({'last_synced': ts}, fh)

print(f'Klar! Synkad {ts}')
