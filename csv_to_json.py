# convert_csv_to_json.py
import csv, json, collections, pathlib
#csv_path  = pathlib.Path('haestarettardomar.csv')
csv_path  = pathlib.Path('data/allir_domar_og_akvardanir.csv')
json_path = pathlib.Path('data/mapping_d_og_a.json')

grouped = collections.defaultdict(list)
with csv_path.open(encoding='utf-8-sig', newline='') as f:
    for row in csv.DictReader(f):
        grouped[row['appeals_case_number'].strip()].append({
            'appeals_case_number' : row['appeals_case_number'].strip(),
            'supreme_case_number': row['supreme_case_number'].strip(),
            'supreme_case_link'    : row['supreme_case_link'].strip(),
            'appeals_case_link'    : row['appeals_case_link'].strip(),
            'source_type'    : row['source_type'].strip()
        })

# if a key has exactly one verdict, store just the object (smaller JSON)
mapping = {k: v[0] if len(v) == 1 else v for k, v in grouped.items()}

json_path.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding='utf-8')
print(f'Wrote {json_path} with {sum(len(v) if isinstance(v,list) else 1 for v in mapping.values()):,} verdict links')
