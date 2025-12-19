Create Database Northwind_DW;
GO

USE Northwind_DW;
GO

CREATE TABLE DimCustomer (
    CustomerID NVARCHAR(20) PRIMARY KEY,
    Country NVARCHAR(100) NULL,
    Company NVARCHAR(255) NULL,
    ContactName NVARCHAR(255) NULL
);
GO

CREATE TABLE DimEmployee (
    EmployeeID NVARCHAR(20) PRIMARY KEY,
    FullName NVARCHAR(255) NULL
);
GO

CREATE TABLE DimTime (
    DateKey NVARCHAR(20) PRIMARY KEY,
    FullDate DATE NULL,
    Month INT NULL,
    Year INT NULL
);
GO

CREATE TABLE Order_Fact (
    OrderID NVARCHAR(20) NOT NULL,
    CustomerID NVARCHAR(20) NOT NULL,
    EmployeeID NVARCHAR(20) NOT NULL,
    OrderDateKey NVARCHAR(20) NOT NULL,
    ShippedDateKey NVARCHAR(20) NULL,

    CONSTRAINT PK_OrderFact PRIMARY KEY (OrderID, CustomerID, EmployeeID, OrderDateKey),

    CONSTRAINT FK_OrderFact_Customer FOREIGN KEY (CustomerID)
        REFERENCES DimCustomer(CustomerID)
        ON DELETE CASCADE,

    CONSTRAINT FK_OrderFact_Employee FOREIGN KEY (EmployeeID)
        REFERENCES DimEmployee(EmployeeID)
        ON DELETE CASCADE,

    CONSTRAINT FK_OrderFact_OrderDate FOREIGN KEY (OrderDateKey)
        REFERENCES DimTime(DateKey)
        ON DELETE CASCADE,

    CONSTRAINT FK_OrderFact_ShippedDate FOREIGN KEY (ShippedDateKey)
        REFERENCES DimTime(DateKey)
        ON DELETE NO ACTION
);
GO