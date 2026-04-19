import subprocess
import json
import sys

VIEWS = [
    "tables", "columns", "indexes", "index_columns", "stats", "stats_columns", 
    "check_constraints", "foreign_keys", "foreign_key_columns", "default_constraints", 
    "sql_modules", "system_sql_modules", "objects", "all_objects", "partitions", 
    "allocation_units", "data_spaces", "schemas", "types", "xml_indexes", "internal_tables",
    "periods", "xml_schema_collections", "dm_db_index_usage_stats", "dm_db_partition_stats"
]

def get_columns(view):
    cmd = [
        "podman", "exec", "iridium_test_sqlserver",
        "/opt/mssql-tools/bin/sqlcmd", "-S", "localhost", "-U", "sa", "-P", "Iridium12345!",
        "-d", "master", "-Q", 
        f"SET NOCOUNT ON; SELECT c.name, t.name as type, c.max_length, c.is_nullable FROM sys.columns c JOIN sys.views v ON c.object_id = v.object_id JOIN sys.schemas s ON v.schema_id = s.schema_id JOIN sys.types t ON c.user_type_id = t.user_type_id WHERE s.name = 'sys' AND v.name = '{view}'",
        "-W", "-s", "|"
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        lines = res.stdout.strip().split('\n')
        if len(lines) < 2: return []
        cols = []
        for line in lines[2:]: # Skip header and separator
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                cols.append({
                    "name": parts[0],
                    "type": parts[1],
                    "max_length": int(parts[2]),
                    "is_nullable": parts[3] == "1"
                })
        return cols
    except Exception as e:
        print(f"Error fetching {view}: {e}")
        return None

metadata = {}
for v in VIEWS:
    print(f"Fetching {v}...")
    cols = get_columns(v)
    if cols is not None:
        metadata[v] = cols

with open("sys_metadata_master.json", "w") as f:
    json.dump(metadata, f, indent=2)

print("Done! Saved to sys_metadata_master.json")
