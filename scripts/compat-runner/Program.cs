using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.SqlClient;

// Resolve the compat-query binary path
var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
var credentialsPath = Path.Combine(repoRoot, "scripts", "credentials.json");
var credentialsJson = File.ReadAllText(credentialsPath);
var credentials = JsonSerializer.Deserialize<Dictionary<string, string>>(credentialsJson)!;
var azureUser = credentials["sql_server_user"];
var azurePassword = credentials["sql_server_password"];
var azureConnStr = $"Server=tcp:[::1],11433;Database=master;User Id={azureUser};Password={azurePassword};TrustServerCertificate=True;Encrypt=false;Connection Timeout=10;";
var compatBin = Path.Combine(repoRoot, "target", "debug", "compat-query.exe");

if (!File.Exists(compatBin))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"compat-query.exe not found at {compatBin}");
    Console.ResetColor();
    return 1;
}

// ── Seed statements for Azure SQL Edge ──────────────────────────────
var seedStatements = new string[]
{
    "IF OBJECT_ID('dbo.vMonthlySales','V')      IS NOT NULL DROP VIEW dbo.vMonthlySales;",
    "IF OBJECT_ID('dbo.vEmployeeHierarchy','V')  IS NOT NULL DROP VIEW dbo.vEmployeeHierarchy;",
    "IF OBJECT_ID('dbo.vProductSales','V')       IS NOT NULL DROP VIEW dbo.vProductSales;",
    "IF OBJECT_ID('dbo.vOrderDetails','V')       IS NOT NULL DROP VIEW dbo.vOrderDetails;",
    "IF OBJECT_ID('dbo.vCustomerOrders','V')     IS NOT NULL DROP VIEW dbo.vCustomerOrders;",

    "IF OBJECT_ID('dbo.OrderItems','U') IS NOT NULL DROP TABLE dbo.OrderItems;",
    "IF OBJECT_ID('dbo.Orders','U')     IS NOT NULL DROP TABLE dbo.Orders;",
    "IF OBJECT_ID('dbo.Employees','U')  IS NOT NULL DROP TABLE dbo.Employees;",
    "IF OBJECT_ID('dbo.Products','U')   IS NOT NULL DROP TABLE dbo.Products;",
    "IF OBJECT_ID('dbo.Customers','U')  IS NOT NULL DROP TABLE dbo.Customers;",
    "IF OBJECT_ID('dbo.Categories','U') IS NOT NULL DROP TABLE dbo.Categories;",

    @"CREATE TABLE dbo.Customers (
    CustomerId INT IDENTITY(1,1) PRIMARY KEY,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email VARCHAR(100) UNIQUE,
    Phone VARCHAR(20),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    IsActive BIT NOT NULL DEFAULT 1
);",
    @"CREATE TABLE dbo.Products (
    ProductId INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Description NVARCHAR(500),
    Price DECIMAL(10,2) NOT NULL,
    Stock INT NOT NULL DEFAULT 0,
    Category NVARCHAR(50),
    IsAvailable BIT NOT NULL DEFAULT 1
);",
    @"CREATE TABLE dbo.Orders (
    OrderId INT IDENTITY(1,1) PRIMARY KEY,
    CustomerId INT NOT NULL,
    OrderDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    TotalAmount DECIMAL(12,2) NOT NULL DEFAULT 0,
    Status NVARCHAR(20) NOT NULL DEFAULT 'Pending',
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES dbo.Customers(CustomerId)
);",
    @"CREATE TABLE dbo.OrderItems (
    OrderItemId INT IDENTITY(1,1) PRIMARY KEY,
    OrderId INT NOT NULL,
    ProductId INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    Subtotal DECIMAL(12,2) NOT NULL,
    CONSTRAINT FK_OrderItems_Orders FOREIGN KEY (OrderId) REFERENCES dbo.Orders(OrderId),
    CONSTRAINT FK_OrderItems_Products FOREIGN KEY (ProductId) REFERENCES dbo.Products(ProductId)
);",
    @"CREATE TABLE dbo.Employees (
    EmployeeId INT IDENTITY(1,1) PRIMARY KEY,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email VARCHAR(100) UNIQUE,
    Department NVARCHAR(50),
    Salary DECIMAL(12,2),
    HireDate DATE NOT NULL,
    ManagerId INT NULL,
    CONSTRAINT FK_Employees_Managers FOREIGN KEY (ManagerId) REFERENCES dbo.Employees(EmployeeId)
);",
    @"CREATE TABLE dbo.Categories (
    CategoryId INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(50) NOT NULL,
    ParentCategoryId INT NULL,
    Description NVARCHAR(200),
    CONSTRAINT FK_Categories_Parent FOREIGN KEY (ParentCategoryId) REFERENCES dbo.Categories(CategoryId)
);",

    @"SET IDENTITY_INSERT dbo.Customers ON;
INSERT INTO dbo.Customers (CustomerId, FirstName, LastName, Email, Phone, IsActive) VALUES
(1, N'John', N'Doe', 'john.doe@email.com', '555-0101', 1),
(2, N'Jane', N'Smith', 'jane.smith@email.com', '555-0102', 1),
(3, N'Bob', N'Johnson', 'bob.johnson@email.com', '555-0103', 1),
(4, N'Alice', N'Williams', 'alice.w@email.com', '555-0104', 1),
(5, N'Charlie', N'Brown', 'charlie.b@email.com', '555-0105', 0);
SET IDENTITY_INSERT dbo.Customers OFF;",

    @"SET IDENTITY_INSERT dbo.Products ON;
INSERT INTO dbo.Products (ProductId, Name, Description, Price, Stock, Category, IsAvailable) VALUES
(1, N'Laptop Pro 15', N'High-performance laptop with 16GB RAM', 1299.99, 50, 'Electronics', 1),
(2, N'Wireless Mouse', N'Ergonomic wireless mouse', 29.99, 200, 'Electronics', 1),
(3, N'USB-C Hub', N'7-in-1 USB-C hub adapter', 49.99, 150, 'Electronics', 1),
(4, N'Mechanical Keyboard', N'RGB mechanical gaming keyboard', 89.99, 75, 'Electronics', 1),
(5, N'Monitor 27""', N'4K UHD monitor 27 inches', 399.99, 30, 'Electronics', 1),
(6, N'Desk Chair', N'Ergonomic office chair', 249.99, 40, 'Furniture', 1),
(7, N'Standing Desk', N'Electric height-adjustable desk', 599.99, 20, 'Furniture', 1),
(8, N'Notebook Set', N'Pack of 5 premium notebooks', 24.99, 100, 'Office Supplies', 1),
(9, N'Pen Collection', N'Set of 10 gel pens', 12.99, 300, 'Office Supplies', 1),
(10, N'Webcam HD', N'1080p webcam with microphone', 79.99, 80, 'Electronics', 0);
SET IDENTITY_INSERT dbo.Products OFF;",

    @"SET IDENTITY_INSERT dbo.Orders ON;
INSERT INTO dbo.Orders (OrderId, CustomerId, OrderDate, TotalAmount, Status) VALUES
(1, 1, '2025-01-15 10:30:00', 1329.98, 'Completed'),
(2, 2, '2025-01-16 14:45:00', 119.98, 'Completed'),
(3, 1, '2025-01-20 09:15:00', 399.99, 'Completed'),
(4, 3, '2025-02-01 16:20:00', 849.98, 'Completed'),
(5, 4, '2025-02-05 11:00:00', 29.99, 'Completed'),
(6, 2, '2025-02-10 13:30:00', 599.99, 'Pending'),
(7, 5, '2025-02-15 15:45:00', 1299.99, 'Cancelled'),
(8, 3, '2025-03-01 10:00:00', 74.98, 'Pending'),
(9, 1, '2025-03-05 12:00:00', 249.99, 'Pending'),
(10, 4, '2025-03-10 17:30:00', 89.99, 'Completed');
SET IDENTITY_INSERT dbo.Orders OFF;",

    @"SET IDENTITY_INSERT dbo.OrderItems ON;
INSERT INTO dbo.OrderItems (OrderItemId, OrderId, ProductId, Quantity, UnitPrice, Subtotal) VALUES
(1, 1, 1, 1, 1299.99, 1299.99),
(2, 1, 2, 1, 29.99, 29.99),
(3, 2, 4, 1, 89.99, 89.99),
(4, 2, 3, 1, 49.99, 49.99),
(5, 3, 5, 1, 399.99, 399.99),
(6, 4, 7, 1, 599.99, 599.99),
(7, 4, 6, 1, 249.99, 249.99),
(8, 5, 2, 1, 29.99, 29.99),
(9, 6, 7, 1, 599.99, 599.99),
(10, 7, 1, 1, 1299.99, 1299.99),
(11, 8, 9, 2, 12.99, 25.98),
(12, 8, 8, 2, 24.99, 49.00),
(13, 9, 6, 1, 249.99, 249.99),
(14, 10, 4, 1, 89.99, 89.99);
SET IDENTITY_INSERT dbo.OrderItems OFF;",

    @"SET IDENTITY_INSERT dbo.Employees ON;
INSERT INTO dbo.Employees (EmployeeId, FirstName, LastName, Email, Department, Salary, HireDate, ManagerId) VALUES
(1, N'Sarah', N'Anderson', 'sarah.anderson@company.com', 'Executive', 150000.00, '2020-01-15', NULL),
(2, N'Michael', N'Chen', 'michael.chen@company.com', 'Sales', 85000.00, '2021-03-20', 1),
(3, N'Emily', N'Davis', 'emily.davis@company.com', 'Sales', 72000.00, '2022-06-10', 2),
(4, N'David', N'Martinez', 'david.martinez@company.com', 'IT', 95000.00, '2021-09-01', 1),
(5, N'Jessica', N'Wilson', 'jessica.wilson@company.com', 'IT', 78000.00, '2023-02-15', 4),
(6, N'Robert', N'Taylor', 'robert.taylor@company.com', 'HR', 68000.00, '2022-11-20', 1),
(7, N'Amanda', N'Garcia', 'amanda.garcia@company.com', 'Sales', 65000.00, '2024-01-10', 2),
(8, N'James', N'Lee', 'james.lee@company.com', 'IT', 82000.00, '2023-07-05', 4);
SET IDENTITY_INSERT dbo.Employees OFF;",

    @"SET IDENTITY_INSERT dbo.Categories ON;
INSERT INTO dbo.Categories (CategoryId, Name, ParentCategoryId, Description) VALUES
(1, 'All', NULL, 'Root category'),
(2, 'Electronics', 1, 'Electronic devices and accessories'),
(3, 'Furniture', 1, 'Office and home furniture'),
(4, 'Office Supplies', 1, 'Stationery and office materials'),
(5, 'Computers', 2, 'Desktop and laptop computers'),
(6, 'Accessories', 2, 'Computer and electronic accessories'),
(7, 'Chairs', 3, 'Office and desk chairs'),
(8, 'Desks', 3, 'Office and standing desks');
SET IDENTITY_INSERT dbo.Categories OFF;",

    @"CREATE VIEW dbo.vCustomerOrders AS
SELECT c.CustomerId, c.FirstName, c.LastName,
    COUNT(o.OrderId) AS TotalOrders,
    CAST(COALESCE(SUM(o.TotalAmount), 0) AS DECIMAL(18,2)) AS TotalSpent
FROM dbo.Customers c
LEFT JOIN dbo.Orders o ON c.CustomerId = o.CustomerId
GROUP BY c.CustomerId, c.FirstName, c.LastName;",

    @"CREATE VIEW dbo.vOrderDetails AS
SELECT o.OrderId, o.OrderDate, o.Status, c.CustomerId,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    oi.ProductId, p.Name AS ProductName,
    oi.Quantity, oi.UnitPrice, oi.Subtotal
FROM dbo.Orders o
INNER JOIN dbo.Customers c ON o.CustomerId = c.CustomerId
INNER JOIN dbo.OrderItems oi ON o.OrderId = oi.OrderId
INNER JOIN dbo.Products p ON oi.ProductId = p.ProductId;",

    @"CREATE VIEW dbo.vProductSales AS
SELECT p.ProductId, p.Name AS ProductName, p.Category,
    p.Price AS CurrentPrice, p.Stock,
    COALESCE(CAST(SUM(oi.Quantity) AS INT), 0) AS TotalSold,
    COALESCE(CAST(SUM(oi.Subtotal) AS DECIMAL(18,2)), 0) AS TotalRevenue
FROM dbo.Products p
LEFT JOIN dbo.OrderItems oi ON p.ProductId = oi.ProductId
GROUP BY p.ProductId, p.Name, p.Category, p.Price, p.Stock;",

    @"CREATE VIEW dbo.vEmployeeHierarchy AS
SELECT e.EmployeeId, e.FirstName, e.LastName, e.Department,
    e.Salary, e.HireDate,
    m.FirstName + ' ' + m.LastName AS ManagerName
FROM dbo.Employees e
LEFT JOIN dbo.Employees m ON e.ManagerId = m.EmployeeId;",

    @"CREATE VIEW dbo.vMonthlySales AS
SELECT YEAR(OrderDate) AS SaleYear, MONTH(OrderDate) AS SaleMonth,
    COUNT(OrderId) AS TotalOrders,
    CAST(SUM(TotalAmount) AS DECIMAL(18,2)) AS TotalRevenue,
    CAST(AVG(TotalAmount) AS DECIMAL(18,2)) AS AvgOrderValue
FROM dbo.Orders
GROUP BY YEAR(OrderDate), MONTH(OrderDate);"
};

// ── Test queries ────────────────────────────────────────────────────
var queries = new string[]
{
    "SELECT 1 as n",
    "SELECT 'hello' as s",
    "SELECT 1 + 2 * 3 / 2 as result",
    "SELECT NULL as n",
    "SELECT CAST('123' AS INT) as n",

    "SELECT COUNT(*) as c FROM dbo.Customers",
    "SELECT SUM(Price) as s FROM dbo.Products",
    "SELECT AVG(Salary) as a FROM dbo.Employees",
    "SELECT Category, COUNT(*) as c FROM dbo.Products GROUP BY Category",
    "SELECT MIN(Price) as minp, MAX(Price) as maxp FROM dbo.Products",
    "SELECT COUNT(DISTINCT Category) as c FROM dbo.Products",

    "SELECT Name FROM dbo.Products WHERE Price > 100",
    "SELECT FirstName FROM dbo.Customers WHERE CustomerId IN (1, 3, 5)",
    "SELECT Name FROM dbo.Products WHERE Name LIKE 'Laptop%'",

    "SELECT c.FirstName, o.TotalAmount FROM dbo.Customers c INNER JOIN dbo.Orders o ON c.CustomerId = o.CustomerId",
    "SELECT c.FirstName, o.OrderId FROM dbo.Customers c LEFT JOIN dbo.Orders o ON c.CustomerId = o.CustomerId",
    "SELECT e.FirstName, m.FirstName as Manager FROM dbo.Employees e LEFT JOIN dbo.Employees m ON e.ManagerId = m.EmployeeId",

    "SELECT TOP 1 * FROM dbo.vCustomerOrders ORDER BY CustomerId",
    "SELECT TOP 1 * FROM dbo.vProductSales ORDER BY ProductId",
    "SELECT TOP 5 * FROM dbo.vOrderDetails ORDER BY OrderId, ProductId",
    "SELECT TOP 1 * FROM dbo.vEmployeeHierarchy ORDER BY EmployeeId",
    "SELECT TOP 1 * FROM dbo.vMonthlySales ORDER BY SaleYear",

    "SELECT Name, (SELECT COUNT(*) FROM dbo.OrderItems WHERE ProductId = p.ProductId) as TimesSold FROM dbo.Products p",
    "SELECT FirstName FROM dbo.Customers WHERE CustomerId IN (SELECT CustomerId FROM dbo.Orders WHERE TotalAmount > 500)",
    "SELECT FirstName FROM dbo.Customers c WHERE EXISTS (SELECT 1 FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId AND o.TotalAmount > 1000)",
    "SELECT Name, (SELECT SUM(Quantity) FROM dbo.OrderItems WHERE ProductId = p.ProductId) as s FROM dbo.Products p",

    "SELECT Name, LEN(Name) as l FROM dbo.Products",
    "SELECT UPPER(FirstName) as u, LOWER(LastName) as l FROM dbo.Customers",
    "SELECT FirstName + ' ' + LastName as n FROM dbo.Employees",
    "SELECT LTRIM('  hello') as a, RTRIM('world  ') as b, LTRIM(RTRIM('  both  ')) as c",
    "SELECT SUBSTRING('Hello World', 1, 5) as s",
    "SELECT CHARINDEX('W', 'Hello World') as c",

    "SELECT ABS(-42) as a",
    "SELECT ROUND(123.456, 2) as r",
    "SELECT 10 % 3 as m",
    "SELECT 5.0 / 2.0 as d",

    "SELECT YEAR('2025-01-01') as y",
    "SELECT DATEDIFF(day, '2025-01-01', '2025-01-10') as d",
    "SELECT CAST(DATEADD(day, 5, '2025-01-01') AS DATE) as d",
    "SELECT YEAR(GETDATE()) as y",

    "SELECT COALESCE(Phone, 'No Phone') as p FROM dbo.Customers",
    "SELECT ISNULL(Phone, 'N/A') as p FROM dbo.Customers",
    "SELECT NULLIF(10, 10) as a, NULLIF(10, 5) as b",

    "SELECT Name, CASE WHEN Price > 500 THEN 'Pro' WHEN Price > 100 THEN 'Consumer' ELSE 'Budget' END as t FROM dbo.Products",

    "SELECT TOP 3 Name FROM dbo.Products ORDER BY Price DESC",
    "SELECT Name FROM dbo.Products ORDER BY ProductId OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY",
    "SELECT Name FROM dbo.Products ORDER BY LEN(Name), Name",
    "SELECT FirstName, Phone FROM dbo.Customers ORDER BY Phone DESC",

    "SELECT CAST(123.45 AS DECIMAL(10,2)) as d",

    "DECLARE @x INT = 10; SET @x = @x + 5; SELECT @x as x",
    "DECLARE @x INT = 10; IF @x > 5 SELECT 'Greater' as r ELSE SELECT 'Smaller' as r",

    "SELECT DB_NAME() as current_db",
    "SELECT name, type, type_desc FROM sys.procedures",
    "SELECT name, type, type_desc FROM sys.functions",
    "SELECT name, system_type_id, user_type_id FROM sys.parameters",
    "SELECT name, is_user_defined FROM sys.types WHERE is_user_defined = 1",
    "SELECT name FROM sys.table_types",

    // -- Error cases for parity testing --
    "SELECT * FROM NonExistentTable",
    "SELECT CustomerId, FakeColumn FROM dbo.Customers",
    "SELECT 1/0",
    "SELECT CAST('not_a_number' AS INT)",
    "SELECT FROM WHERE;",
};

// ── Helpers ─────────────────────────────────────────────────────────
static QueryEnvelope ExecuteAzure(string connStr, string sql)
{
    try
    {
        using var conn = new SqlConnection(connStr);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 10;
        using var reader = cmd.ExecuteReader();
        var resultSets = new List<ResultSetEnvelope>();

        do
        {
            if (reader.FieldCount <= 0)
            {
                continue;
            }

            var columns = new string[reader.FieldCount];
            var columnTypes = new string[reader.FieldCount];
            var columnPrecisions = new byte?[reader.FieldCount];
            var columnScales = new byte?[reader.FieldCount];
            var columnLengths = new int?[reader.FieldCount];
            var columnNullabilities = new bool?[reader.FieldCount];

            var schemaTable = reader.GetSchemaTable();

            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns[i] = reader.GetName(i);
                columnTypes[i] = NormalizeTypeName(reader.GetDataTypeName(i));

                if (schemaTable != null)
                {
                    var row = schemaTable.Rows[i];
                    columnPrecisions[i] = row["NumericPrecision"] != DBNull.Value ? (byte?)Convert.ToByte(row["NumericPrecision"]) : null;
                    columnScales[i] = row["NumericScale"] != DBNull.Value ? (byte?)Convert.ToByte(row["NumericScale"]) : null;
                    columnLengths[i] = row["ColumnSize"] != DBNull.Value ? (int?)Convert.ToInt32(row["ColumnSize"]) : null;
                    columnNullabilities[i] = row["AllowDBNull"] != DBNull.Value ? (bool?)Convert.ToBoolean(row["AllowDBNull"]) : null;
                }
            }

            var rows = new List<string[]>();
            while (reader.Read())
            {
                var cols = new string[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    cols[i] = FormatAzureValue(reader.GetValue(i));
                }
                rows.Add(cols);
            }

            rows.Sort(CompareRows);
            resultSets.Add(new ResultSetEnvelope(
                columns,
                columnTypes,
                columnPrecisions,
                columnScales,
                columnLengths,
                columnNullabilities,
                rows.ToArray(),
                rows.Count));
        } while (reader.NextResult());

        return new QueryEnvelope(true, null, resultSets.ToArray());
    }
    catch (SqlException ex)
    {
        return new QueryEnvelope(
            false,
            new ErrorEnvelope(ex.Number, ex.Class, ex.State, "SqlException", ex.Message),
            Array.Empty<ResultSetEnvelope>()
        );
    }
    catch (Exception ex)
    {
        return new QueryEnvelope(
            false,
            new ErrorEnvelope(0, 0, 0, ex.GetType().Name, ex.Message),
            Array.Empty<ResultSetEnvelope>()
        );
    }
}

static QueryEnvelope ExecuteLocal(string bin, string sql)
{
    try
    {
        var psi = new ProcessStartInfo(bin, $"\"{sql}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        using var proc = Process.Start(psi)!;
        var stdout = proc.StandardOutput.ReadToEnd();
        var stderr = proc.StandardError.ReadToEnd();
        proc.WaitForExit(10_000);

        var parsed = TryParseQueryEnvelope(stdout);
        if (parsed is not null)
            return parsed;

        if (proc.ExitCode != 0 && !string.IsNullOrWhiteSpace(stderr))
        {
            var error = TryParseErrorLine(stderr.Trim());
            if (error is not null)
                return error;
        }

        if (proc.ExitCode != 0)
        {
            return new QueryEnvelope(
                false,
                new ErrorEnvelope(0, 0, 0, "ProcessExit", stderr.Trim()),
                Array.Empty<ResultSetEnvelope>()
            );
        }

        return new QueryEnvelope(true, null, Array.Empty<ResultSetEnvelope>());
    }
    catch (Exception ex)
    {
        return new QueryEnvelope(
            false,
            new ErrorEnvelope(0, 0, 0, ex.GetType().Name, ex.Message),
            Array.Empty<ResultSetEnvelope>()
        );
    }
}

static QueryEnvelope? TryParseQueryEnvelope(string payload)
{
    var trimmed = payload.Trim();
    if (string.IsNullOrWhiteSpace(trimmed))
        return null;

    try
    {
        return JsonSerializer.Deserialize<QueryEnvelope>(
            trimmed,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true }
        );
    }
    catch
    {
        return null;
    }
}

static QueryEnvelope? TryParseErrorLine(string line)
{
    if (!line.StartsWith("ERROR:", StringComparison.Ordinal))
        return null;

    var parts = line.Split(':', 5);
    if (parts.Length < 5)
        return null;

    _ = int.TryParse(parts[1], out var number);
    _ = int.TryParse(parts[2], out var classValue);
    _ = int.TryParse(parts[3], out var state);

    return new QueryEnvelope(
        false,
        new ErrorEnvelope(number, classValue, state, "ProcessError", parts[4]),
        Array.Empty<ResultSetEnvelope>()
    );
}

static string FormatAzureValue(object? value)
{
    if (value is null || value is DBNull)
        return "NULL";

    if (value is DateTime dt)
        return dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture).Trim();

    if (value is DateTimeOffset dto)
        return dto.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture).Trim();

    if (value is IFormattable fmt)
        return fmt.ToString(null, CultureInfo.InvariantCulture)!.Trim();

    return value.ToString()!.Trim();
}

static string NormalizeTypeName(string typeName)
{
    return typeName.Trim().ToLowerInvariant();
}

static string NormalizeText(string text)
{
    return string.Join(" ", text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
}

static string NormalizeRow(string[] row)
{
    return string.Join("\u001F", row);
}

static string FormatVector(IEnumerable<string> values)
{
    return "[" + string.Join(", ", values.Select(v => $"'{v}'")) + "]";
}

static int CompareRows(string[] left, string[] right)
{
    return StringComparer.Ordinal.Compare(NormalizeRow(left), NormalizeRow(right));
}

static bool CompareResponses(
    QueryEnvelope azure,
    QueryEnvelope local,
    out List<string> diffs
)
{
    diffs = new List<string>();

    if (azure.Ok != local.Ok)
    {
        diffs.Add($"status mismatch: Azure Ok={azure.Ok}, Local Ok={local.Ok}");
        return false;
    }

    if (!azure.Ok)
    {
        if (azure.Error is null || local.Error is null)
        {
            diffs.Add("one side returned an error envelope and the other did not");
            return false;
        }

        if (azure.Error.Number != local.Error.Number)
            diffs.Add($"error number mismatch: Azure={azure.Error.Number}, Local={local.Error.Number}");
        if (azure.Error.Class != local.Error.Class)
            diffs.Add($"error class mismatch: Azure={azure.Error.Class}, Local={local.Error.Class}");
        if (azure.Error.State != local.Error.State)
            diffs.Add($"error state mismatch: Azure={azure.Error.State}, Local={local.Error.State}");

        var azureMessage = NormalizeText(azure.Error.Message);
        var localMessage = NormalizeText(local.Error.Message);
        if (!string.Equals(azureMessage, localMessage, StringComparison.Ordinal))
        {
            diffs.Add($"error message mismatch:\n    Azure: {azure.Error.Message}\n    Local: {local.Error.Message}");
        }

        return diffs.Count == 0;
    }

    if (azure.ResultSets.Length != local.ResultSets.Length)
    {
        diffs.Add(
            $"result-set count mismatch: Azure={azure.ResultSets.Length}, Local={local.ResultSets.Length}"
        );
    }

    var maxSets = Math.Min(azure.ResultSets.Length, local.ResultSets.Length);
    for (int setIdx = 0; setIdx < maxSets; setIdx++)
    {
        var a = azure.ResultSets[setIdx];
        var l = local.ResultSets[setIdx];

        if (!a.Columns.SequenceEqual(l.Columns, StringComparer.Ordinal))
            diffs.Add(
                $"result set {setIdx} columns mismatch: Azure={FormatVector(a.Columns)}, Local={FormatVector(l.Columns)}"
            );

        if (!a.ColumnTypes.SequenceEqual(l.ColumnTypes, StringComparer.OrdinalIgnoreCase))
            diffs.Add(
                $"result set {setIdx} column types mismatch: Azure={FormatVector(a.ColumnTypes)}, Local={FormatVector(l.ColumnTypes)}"
            );

        if (!a.ColumnPrecisions.SequenceEqual(l.ColumnPrecisions))
            diffs.Add(
                $"result set {setIdx} column precisions mismatch: Azure={FormatVector(a.ColumnPrecisions.Select(p => p?.ToString() ?? \"null\"))}, Local={FormatVector(l.ColumnPrecisions.Select(p => p?.ToString() ?? \"null\"))}"
            );

        if (!a.ColumnScales.SequenceEqual(l.ColumnScales))
            diffs.Add(
                $"result set {setIdx} column scales mismatch: Azure={FormatVector(a.ColumnScales.Select(p => p?.ToString() ?? \"null\"))}, Local={FormatVector(l.ColumnScales.Select(p => p?.ToString() ?? \"null\"))}"
            );

        if (!a.ColumnLengths.SequenceEqual(l.ColumnLengths))
            diffs.Add(
                $"result set {setIdx} column lengths mismatch: Azure={FormatVector(a.ColumnLengths.Select(p => p?.ToString() ?? \"null\"))}, Local={FormatVector(l.ColumnLengths.Select(p => p?.ToString() ?? \"null\"))}"
            );

        if (!a.ColumnNullabilities.SequenceEqual(l.ColumnNullabilities))
            diffs.Add(
                $"result set {setIdx} column nullabilities mismatch: Azure={FormatVector(a.ColumnNullabilities.Select(p => p?.ToString() ?? \"null\"))}, Local={FormatVector(l.ColumnNullabilities.Select(p => p?.ToString() ?? \"null\"))}"
            );

        if (a.RowCount != l.RowCount)
            diffs.Add($"result set {setIdx} row count mismatch: Azure={a.RowCount}, Local={l.RowCount}");

        var aRows = a.Rows.Select(NormalizeRow).OrderBy(x => x, StringComparer.Ordinal).ToArray();
        var lRows = l.Rows.Select(NormalizeRow).OrderBy(x => x, StringComparer.Ordinal).ToArray();

        if (!aRows.SequenceEqual(lRows, StringComparer.Ordinal))
        {
            diffs.Add(
                $"result set {setIdx} row values mismatch: Azure={FormatVector(aRows)}, Local={FormatVector(lRows)}"
            );
        }
    }

    return diffs.Count == 0;
}

static string SummarizeDiffs(IEnumerable<string> diffs)
{
    var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
    {
        ["status"] = 0,
        ["error"] = 0,
        ["result-set-shape"] = 0,
        ["metadata"] = 0,
        ["rowcount"] = 0,
        ["values"] = 0,
        ["other"] = 0,
    };

    foreach (var diff in diffs)
    {
        var bucket = ClassifyDiff(diff);
        counts[bucket]++;
    }

    return string.Join(
        ", ",
        counts.Where(kv => kv.Value > 0).Select(kv => $"{kv.Key}={kv.Value}")
    );
}

static string ClassifyDiff(string diff)
{
    var lower = diff.ToLowerInvariant();
    if (lower.Contains("status mismatch"))
        return "status";
    if (lower.Contains("error number mismatch")
        || lower.Contains("error class mismatch")
        || lower.Contains("error state mismatch")
        || lower.Contains("error message mismatch"))
    {
        return "error";
    }
    if (lower.Contains("result-set count mismatch"))
        return "result-set-shape";
    if (lower.Contains("columns mismatch")
        || lower.Contains("column types mismatch")
        || lower.Contains("column precisions mismatch")
        || lower.Contains("column scales mismatch")
        || lower.Contains("column lengths mismatch"))
    {
        return "metadata";
    }
    if (lower.Contains("row count mismatch"))
        return "rowcount";
    if (lower.Contains("row values mismatch"))
        return "values";
    return "other";
}

static void WriteCompatReport(
    List<(string sql, QueryEnvelope azure, QueryEnvelope local, List<string> diffs)> failures,
    int passed,
    int failed,
    int skipped,
    int totalQueries,
    string azureConnStr,
    string compatBin
)
{
    var reportDir = Environment.GetEnvironmentVariable("IRIDIUM_COMPAT_REPORT_DIR")
        ?? Environment.GetEnvironmentVariable("TSQL_COMPAT_REPORT_DIR");
    if (string.IsNullOrWhiteSpace(reportDir))
    {
        return;
    }

    var reportStem = Environment.GetEnvironmentVariable("IRIDIUM_COMPAT_REPORT_STEM")
        ?? Environment.GetEnvironmentVariable("TSQL_COMPAT_REPORT_STEM");
    if (string.IsNullOrWhiteSpace(reportStem))
    {
        reportStem = $"compat-run-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
    }

    Directory.CreateDirectory(reportDir);

    var report = new CompatRunReport(
        DateTimeOffset.UtcNow,
        totalQueries,
        passed,
        failed,
        skipped,
        RedactConnectionString(azureConnStr),
        compatBin,
        failures.Select(failure => new CompatFailureReport(
            failure.sql,
            failure.azure,
            failure.local,
            failure.diffs.Select(diff => new CompatDiffReport(
                ClassifyDiff(diff),
                diff
            )).ToArray()
        )).ToArray()
    );

    var json = JsonSerializer.Serialize(report, new JsonSerializerOptions
    {
        WriteIndented = true
    });
    File.WriteAllText(Path.Combine(reportDir, $"{reportStem}.json"), json);
}

static string RedactConnectionString(string connectionString)
{
    try
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        if (!string.IsNullOrWhiteSpace(builder.Password))
        {
            builder.Password = "***";
        }
        return builder.ConnectionString;
    }
    catch
    {
        return connectionString;
    }
}

// ── Seed Azure SQL Edge ─────────────────────────────────────────────
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("Seeding Azure SQL Edge...");
Console.ResetColor();

try
{
    using var seedConn = new SqlConnection(azureConnStr);
    seedConn.Open();
    foreach (var stmt in seedStatements)
    {
        using var cmd = seedConn.CreateCommand();
        cmd.CommandText = stmt;
        cmd.CommandTimeout = 30;
        cmd.ExecuteNonQuery();
    }
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"Seeded {seedStatements.Length} statements.");
    Console.ResetColor();
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"Seeding failed: {ex.Message}");
    Console.ResetColor();
    return 1;
}

// ── Run tests ───────────────────────────────────────────────────────
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine($"\n=============================================");
Console.WriteLine($" Compatibility Tests ({queries.Length} queries)");
Console.WriteLine($" Azure: tcp:[::1]:11433   Local: compat-query.exe");
Console.WriteLine($"=============================================");
Console.ResetColor();

int passed = 0, failed = 0, skipped = 0;
var failures = new List<(string sql, QueryEnvelope azure, QueryEnvelope local, List<string> diffs)>();

foreach (var sql in queries)
{
    var label = sql.Length > 70 ? sql[..70] + "..." : sql;
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.Write($"  {label}...");

    var azResult = ExecuteAzure(azureConnStr, sql);
    var locResult = ExecuteLocal(compatBin, sql);

    if (CompareResponses(azResult, locResult, out var diffs))
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine(" [PASS]");
        passed++;
    }
    else
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(" [FAIL]");
        failed++;
        failures.Add((sql, azResult, locResult, diffs));
    }
    Console.ResetColor();
    }

// ── Summary ─────────────────────────────────────────────────────────
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine($"\n=============================================");
Console.WriteLine($" RESULTS: {passed} PASS | {failed} FAIL | {skipped} SKIP");
Console.WriteLine($"=============================================");
Console.ResetColor();

if (failures.Count > 0)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine("\nFAILURE DETAILS:");
    Console.WriteLine("────────────────────────────────────────────────────────");
    foreach (var (sql, azure, local, diffs) in failures)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"\n  SQL:   {sql}");
        foreach (var diff in diffs)
        {
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"  DIFF: {diff}");
        }

        if (!azure.Ok)
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine(
                $"  Azure error: {azure.Error!.Number}:{azure.Error.Class}:{azure.Error.State} {azure.Error.Message}"
            );
        }
        if (!local.Ok)
        {
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.WriteLine(
                $"  Local error: {local.Error!.Number}:{local.Error.Class}:{local.Error.State} {local.Error.Message}"
            );
        }
    }
    Console.ResetColor();

    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine("\nDIFF SUMMARY:");
    foreach (var failure in failures)
    {
        Console.WriteLine($"  {failure.sql}");
        Console.WriteLine($"    {SummarizeDiffs(failure.diffs)}");
    }
    Console.ResetColor();
}

WriteCompatReport(
    failures,
    passed,
    failed,
    skipped,
    queries.Length,
    azureConnStr,
    compatBin
);

return failed > 0 ? 1 : 0;

// ── Response types ──────────────────────────────────────────────────
record ErrorEnvelope(int Number, int Class, int State, string Code, string Message);

record ResultSetEnvelope(
    string[] Columns,
    string[] ColumnTypes,
    byte?[] ColumnPrecisions,
    byte?[] ColumnScales,
    int?[] ColumnLengths,
    bool?[] ColumnNullabilities,
    string[][] Rows,
    int RowCount
);

record QueryEnvelope(bool Ok, ErrorEnvelope? Error, ResultSetEnvelope[] ResultSets);

record CompatRunReport(
    DateTimeOffset GeneratedAtUtc,
    int TotalQueries,
    int Passed,
    int Failed,
    int Skipped,
    string AzureConnection,
    string LocalBinary,
    CompatFailureReport[] Failures
);

record CompatFailureReport(
    string Sql,
    QueryEnvelope Azure,
    QueryEnvelope Local,
    CompatDiffReport[] Diffs
);

record CompatDiffReport(string Category, string Message);
