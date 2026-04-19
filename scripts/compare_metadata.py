import json
import os
import re

def extract_rust_columns(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    tables = {}
    
    # Pattern 1: virtual_table_def("name", vec![ ... ])
    pattern1 = r'virtual_table_def\(\s*"([^"]+)",\s*vec!\[(.*?)\s*\]\s*\)'
    for m in re.finditer(pattern1, content, re.DOTALL):
        table_name = m.group(1)
        cols_content = m.group(2)
        col_pattern = r'\(\s*"([^"]+)",\s*DataType::(.*?),\s*(true|false)\s*\)'
        cols = [cm.group(1) for cm in re.finditer(col_pattern, cols_content)]
        if cols:
            tables[table_name] = cols

    # Pattern 2: vec![ ("name", ... ) ]
    # Often used in helper functions
    pattern2 = r'vec!\[\s*(\(\s*"[^"]+",\s*DataType::.*?\).*?)\s*\]'
    for m in re.finditer(pattern2, content, re.DOTALL):
        cols_content = m.group(1)
        col_pattern = r'\(\s*"([^"]+)",\s*DataType::(.*?),\s*(true|false)\s*\)'
        cols = [cm.group(1) for cm in re.finditer(col_pattern, cols_content)]
        # We need to guess the table name if it's not in the same call
        # For now, let's just see if we found columns
        if cols and "object_id" in cols:
            # Try to find if this is inside a function like `column_table_def(name: &str)`
            pass

    return tables

def main():
    with open('sys_metadata_master.json', 'r') as f:
        master = json.load(f)
    
    iridium_metadata = {}
    base_path = 'crates/iridium_core/src/executor/metadata/sys'
    
    paths = [base_path, os.path.join(base_path, 'tables')]
    for p in paths:
        if not os.path.exists(p): continue
        for f in os.listdir(p):
            if f.endswith('.rs'):
                iridium_metadata.update(extract_rust_columns(os.path.join(p, f)))
    
    print(f"Iridium views found: {list(iridium_metadata.keys())}")

    diff = {}
    for view, master_cols in master.items():
        master_col_names = [c['name'] for c in master_cols]
        if view not in iridium_metadata:
            diff[view] = { "status": "MISSING_VIEW", "missing": master_col_names }
        else:
            iridium_cols = iridium_metadata[view]
            missing = [name for name in master_col_names if name not in iridium_cols]
            if missing:
                diff[view] = { "status": "MISSING_COLUMNS", "missing": missing }

    with open('metadata_diff.json', 'w') as f:
        json.dump(diff, f, indent=2)
    
    print(f"\nSummary of differences saved to metadata_diff.json")
    for view, d in diff.items():
        print(f"- {view}: {d['status']} ({len(d['missing'])} items)")

if __name__ == '__main__':
    main()
