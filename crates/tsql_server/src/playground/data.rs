//! Sample data for playground database
//! 
//! Follows SRP: only responsible for INSERT statements

/// INSERT statements to populate playground tables
pub const INSERT_STATEMENTS: &[&str] = &[
    // Customers
    r#"
INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, IsActive) VALUES
(N'John', N'Doe', 'john.doe@email.com', '555-0101', 1),
(N'Jane', N'Smith', 'jane.smith@email.com', '555-0102', 1),
(N'Bob', N'Johnson', 'bob.johnson@email.com', '555-0103', 1),
(N'Alice', N'Williams', 'alice.w@email.com', '555-0104', 1),
(N'Charlie', N'Brown', 'charlie.b@email.com', '555-0105', 0);
"#,
    // Products
    r#"
INSERT INTO dbo.Products (Name, Description, Price, Stock, Category, IsAvailable) VALUES
(N'Laptop Pro 15', N'High-performance laptop with 16GB RAM', 1299.99, 50, 'Electronics', 1),
(N'Wireless Mouse', N'Ergonomic wireless mouse', 29.99, 200, 'Electronics', 1),
(N'USB-C Hub', N'7-in-1 USB-C hub adapter', 49.99, 150, 'Electronics', 1),
(N'Mechanical Keyboard', N'RGB mechanical gaming keyboard', 89.99, 75, 'Electronics', 1),
(N'Monitor 27"', N'4K UHD monitor 27 inches', 399.99, 30, 'Electronics', 1),
(N'Desk Chair', N'Ergonomic office chair', 249.99, 40, 'Furniture', 1),
(N'Standing Desk', N'Electric height-adjustable desk', 599.99, 20, 'Furniture', 1),
(N'Notebook Set', N'Pack of 5 premium notebooks', 24.99, 100, 'Office Supplies', 1),
(N'Pen Collection', N'Set of 10 gel pens', 12.99, 300, 'Office Supplies', 1),
(N'Webcam HD', N'1080p webcam with microphone', 79.99, 80, 'Electronics', 0);
"#,
    // Orders
    r#"
INSERT INTO dbo.Orders (CustomerId, OrderDate, TotalAmount, Status) VALUES
(1, '2025-01-15 10:30:00', 1329.98, 'Completed'),
(2, '2025-01-16 14:45:00', 119.98, 'Completed'),
(1, '2025-01-20 09:15:00', 399.99, 'Completed'),
(3, '2025-02-01 16:20:00', 849.98, 'Completed'),
(4, '2025-02-05 11:00:00', 29.99, 'Completed'),
(2, '2025-02-10 13:30:00', 599.99, 'Pending'),
(5, '2025-02-15 15:45:00', 1299.99, 'Cancelled'),
(3, '2025-03-01 10:00:00', 74.98, 'Pending'),
(1, '2025-03-05 12:00:00', 249.99, 'Pending'),
(4, '2025-03-10 17:30:00', 89.99, 'Completed');
"#,
    // OrderItems
    r#"
INSERT INTO dbo.OrderItems (OrderId, ProductId, Quantity, UnitPrice, Subtotal) VALUES
-- Order 1 (John Doe - Laptop + Mouse)
(1, 1, 1, 1299.99, 1299.99),
(1, 2, 1, 29.99, 29.99),
-- Order 2 (Jane Smith - Keyboard + Hub)
(2, 4, 1, 89.99, 89.99),
(2, 3, 1, 49.99, 49.99),
-- Order 3 (John Doe - Monitor)
(3, 5, 1, 399.99, 399.99),
-- Order 4 (Bob Johnson - Desk + Chair)
(4, 7, 1, 599.99, 599.99),
(4, 6, 1, 249.99, 249.99),
-- Order 5 (Alice Williams - Mouse)
(5, 2, 1, 29.99, 29.99),
-- Order 6 (Jane Smith - Standing Desk)
(6, 7, 1, 599.99, 599.99),
-- Order 7 (Charlie Brown - Laptop)
(7, 1, 1, 1299.99, 1299.99),
-- Order 8 (Bob Johnson - Pens + Notebooks)
(8, 9, 2, 12.99, 25.98),
(8, 8, 2, 24.99, 49.00),
-- Order 9 (John Doe - Chair)
(9, 6, 1, 249.99, 249.99),
-- Order 10 (Alice Williams - Keyboard)
(10, 4, 1, 89.99, 89.99);
"#,
    // Employees
    r#"
INSERT INTO dbo.Employees (FirstName, LastName, Email, Department, Salary, HireDate, ManagerId) VALUES
(N'Sarah', N'Anderson', 'sarah.anderson@company.com', 'Executive', 150000.00, '2020-01-15', NULL),
(N'Michael', N'Chen', 'michael.chen@company.com', 'Sales', 85000.00, '2021-03-20', 1),
(N'Emily', N'Davis', 'emily.davis@company.com', 'Sales', 72000.00, '2022-06-10', 2),
(N'David', N'Martinez', 'david.martinez@company.com', 'IT', 95000.00, '2021-09-01', 1),
(N'Jessica', N'Wilson', 'jessica.wilson@company.com', 'IT', 78000.00, '2023-02-15', 4),
(N'Robert', N'Taylor', 'robert.taylor@company.com', 'HR', 68000.00, '2022-11-20', 1),
(N'Amanda', N'Garcia', 'amanda.garcia@company.com', 'Sales', 65000.00, '2024-01-10', 2),
(N'James', N'Lee', 'james.lee@company.com', 'IT', 82000.00, '2023-07-05', 4);
"#,
    // Categories
    r#"
INSERT INTO dbo.Categories (Name, ParentCategoryId, Description) VALUES
('All', NULL, 'Root category'),
('Electronics', 1, 'Electronic devices and accessories'),
('Furniture', 1, 'Office and home furniture'),
('Office Supplies', 1, 'Stationery and office materials'),
('Computers', 2, 'Desktop and laptop computers'),
('Accessories', 2, 'Computer and electronic accessories'),
('Chairs', 3, 'Office and desk chairs'),
('Desks', 3, 'Office and standing desks');
"#,
];
