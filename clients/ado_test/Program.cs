using Microsoft.Data.SqlClient;

var port = args.Length > 0 ? args[0] : "1433";
var connStr = $"Server=127.0.0.1,{port};Database=master;User Id=sa;Password=;TrustServerCertificate=True;Encrypt=False;Connection Timeout=10;";

Console.WriteLine($"Connecting to 127.0.0.1:{port} via ADO.NET (Microsoft.Data.SqlClient)...");

int passed = 0, failed = 0;

async Task Test(string name, Func<SqlConnection, Task> fn)
{
    using var conn = new SqlConnection(connStr);
    try
    {
        await conn.OpenAsync();
        await fn(conn);
        Console.WriteLine($"  [PASS] {name}");
        passed++;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        failed++;
    }
}

// 1. Basic connect + scalar
await Test("SELECT 1", async conn =>
{
    var cmd = new SqlCommand("SELECT 1 AS n", conn);
    var result = await cmd.ExecuteScalarAsync();
    if (Convert.ToInt32(result) != 1) throw new Exception($"Expected 1, got {result}");
});

// 2. String result
await Test("SELECT string literal", async conn =>
{
    var cmd = new SqlCommand("SELECT 'hello' AS greeting", conn);
    var result = await cmd.ExecuteScalarAsync();
    if (result?.ToString() != "hello") throw new Exception($"Expected 'hello', got {result}");
});

// 3. Parameterized query (uses sp_executesql under the hood — the SSMS path)
await Test("Parameterized query (@param)", async conn =>
{
    var cmd = new SqlCommand("SELECT @val AS result", conn);
    cmd.Parameters.AddWithValue("@val", 42);
    var result = await cmd.ExecuteScalarAsync();
    if (Convert.ToInt32(result) != 42) throw new Exception($"Expected 42, got {result}");
});

// 4. String parameter
await Test("String parameter", async conn =>
{
    var cmd = new SqlCommand("SELECT @name AS name", conn);
    cmd.Parameters.Add("@name", System.Data.SqlDbType.NVarChar, 50).Value = "world";
    var result = await cmd.ExecuteScalarAsync();
    if (result?.ToString() != "world") throw new Exception($"Expected 'world', got {result}");
});

// 5. DDL + DML
await Test("CREATE TABLE + INSERT + SELECT", async conn =>
{
    await new SqlCommand("CREATE TABLE ado_test_t (id INT, name NVARCHAR(50))", conn).ExecuteNonQueryAsync();
    await new SqlCommand("INSERT INTO ado_test_t VALUES (1, N'Alice')", conn).ExecuteNonQueryAsync();
    await new SqlCommand("INSERT INTO ado_test_t VALUES (2, N'Bob')", conn).ExecuteNonQueryAsync();

    var cmd = new SqlCommand("SELECT COUNT(*) FROM ado_test_t", conn);
    var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
    if (count != 2) throw new Exception($"Expected 2 rows, got {count}");
});

// 6. SqlDataReader — multiple rows
await Test("SqlDataReader multi-row", async conn =>
{
    await new SqlCommand("CREATE TABLE ado_reader_t (id INT, val NVARCHAR(20))", conn).ExecuteNonQueryAsync();
    await new SqlCommand("INSERT INTO ado_reader_t VALUES (1,'a'),(2,'b'),(3,'c')", conn).ExecuteNonQueryAsync();

    var cmd = new SqlCommand("SELECT id, val FROM ado_reader_t ORDER BY id", conn);
    using var reader = await cmd.ExecuteReaderAsync();
    int rows = 0;
    while (await reader.ReadAsync())
    {
        rows++;
        var id = reader.GetInt32(0);
        var val = reader.GetString(1);
        if (id != rows) throw new Exception($"Row {rows}: expected id={rows}, got {id}");
    }
    if (rows != 3) throw new Exception($"Expected 3 rows, got {rows}");
});

// 7. @@VERSION (what SSMS queries on connect)
await Test("@@VERSION", async conn =>
{
    var cmd = new SqlCommand("SELECT @@VERSION AS version", conn);
    var result = await cmd.ExecuteScalarAsync();
    Console.Write($"    version={result?.ToString()?[..Math.Min(40, result?.ToString()?.Length ?? 0)]}... ");
});

// 8. GETDATE()
await Test("GETDATE()", async conn =>
{
    var cmd = new SqlCommand("SELECT GETDATE() AS now", conn);
    var result = await cmd.ExecuteScalarAsync();
    if (result == null || result == DBNull.Value) throw new Exception("Expected a date, got NULL");
});

// 9. Multiple statements in one batch
await Test("Multi-statement batch", async conn =>
{
    var cmd = new SqlCommand("SELECT 1 AS a; SELECT 2 AS b", conn);
    using var reader = await cmd.ExecuteReaderAsync();
    var first = await reader.ReadAsync() ? reader.GetInt32(0) : -1;
    if (first != 1) throw new Exception($"First result set: expected 1, got {first}");
    // advance to second result set
    await reader.NextResultAsync();
    var second = await reader.ReadAsync() ? reader.GetInt32(0) : -1;
    if (second != 2) throw new Exception($"Second result set: expected 2, got {second}");
});

// 10. Error handling
await Test("Error on bad SQL returns SqlException", async conn =>
{
    try
    {
        var cmd = new SqlCommand("SELECT * FROM nonexistent_table_xyz", conn);
        await cmd.ExecuteNonQueryAsync();
        throw new Exception("Expected SqlException but got none");
    }
    catch (SqlException)
    {
        // expected
    }
});

Console.WriteLine();
Console.WriteLine($"Results: {passed} passed, {failed} failed");
Environment.Exit(failed > 0 ? 1 : 0);
