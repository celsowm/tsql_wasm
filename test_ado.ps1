 = 'Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Test@12345;TrustServerCertificate=True;'
 = New-Object System.Data.SqlClient.SqlConnection($connStr1)
.Open()
 = $conn1.CreateCommand()
.CommandText = 'SELECT 1 as n'
 = $cmd1.ExecuteReader()
.Read() | Out-Null
$reader1[0]
.Close()
