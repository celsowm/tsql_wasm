SET NOCOUNT ON;
SELECT v.name as view_name, c.name as column_name, c.is_nullable
FROM sys.columns c
JOIN sys.system_objects v ON c.object_id = v.object_id
WHERE v.type = 'V' 
  AND SCHEMA_NAME(v.schema_id) = 'sys'
  AND v.name IN ('tables', 'columns', 'indexes', 'index_columns', 'objects', 'all_objects', 'stats', 'stats_columns', 'check_constraints', 'foreign_keys', 'foreign_key_columns', 'default_constraints', 'partitions', 'allocation_units', 'data_spaces', 'schemas', 'types')
ORDER BY v.name, c.column_id;
