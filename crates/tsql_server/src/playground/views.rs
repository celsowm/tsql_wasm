//! View definitions for playground database
//! 
//! Follows SRP: only responsible for view DDL

/// DDL statements to create playground views
pub const DDL_STATEMENTS: &[&str] = &[
    // Customer orders summary view
    r#"
CREATE VIEW dbo.vCustomerOrders AS
SELECT 
    c.CustomerId,
    c.FirstName,
    c.LastName,
    c.Email,
    COUNT(o.OrderId) AS TotalOrders,
    COALESCE(SUM(o.TotalAmount), 0) AS TotalSpent
FROM dbo.Customers c
LEFT JOIN dbo.Orders o ON c.CustomerId = o.CustomerId
GROUP BY c.CustomerId, c.FirstName, c.LastName, c.Email;
"#,
    // Order details view
    r#"
CREATE VIEW dbo.vOrderDetails AS
SELECT 
    o.OrderId,
    o.OrderDate,
    o.Status,
    c.CustomerId,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    oi.ProductId,
    p.Name AS ProductName,
    oi.Quantity,
    oi.UnitPrice,
    oi.Subtotal
FROM dbo.Orders o
INNER JOIN dbo.Customers c ON o.CustomerId = c.CustomerId
INNER JOIN dbo.OrderItems oi ON o.OrderId = oi.OrderId
INNER JOIN dbo.Products p ON oi.ProductId = p.ProductId;
"#,
    // Product sales summary view
    r#"
CREATE VIEW dbo.vProductSales AS
SELECT 
    p.ProductId,
    p.Name AS ProductName,
    p.Category,
    p.Price AS CurrentPrice,
    p.Stock,
    COALESCE(SUM(oi.Quantity), 0) AS TotalSold,
    COALESCE(SUM(oi.Subtotal), 0) AS TotalRevenue
FROM dbo.Products p
LEFT JOIN dbo.OrderItems oi ON p.ProductId = oi.ProductId
GROUP BY p.ProductId, p.Name, p.Category, p.Price, p.Stock;
"#,
    // Employee hierarchy view
    r#"
CREATE VIEW dbo.vEmployeeHierarchy AS
SELECT 
    e.EmployeeId,
    e.FirstName,
    e.LastName,
    e.Department,
    e.Salary,
    e.HireDate,
    m.FirstName + ' ' + m.LastName AS ManagerName
FROM dbo.Employees e
LEFT JOIN dbo.Employees m ON e.ManagerId = m.EmployeeId;
"#,
    // Monthly sales summary view
    r#"
CREATE VIEW dbo.vMonthlySales AS
SELECT 
    YEAR(OrderDate) AS SaleYear,
    MONTH(OrderDate) AS SaleMonth,
    COUNT(OrderId) AS TotalOrders,
    SUM(TotalAmount) AS TotalRevenue,
    AVG(TotalAmount) AS AvgOrderValue
FROM dbo.Orders
GROUP BY YEAR(OrderDate), MONTH(OrderDate);
"#,
];
