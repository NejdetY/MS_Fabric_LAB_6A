# MS_Fabric_LAB_6A
# Load Data into a Warehouse Using T-SQL (Microsoft Fabric)

This project demonstrates how to build a data warehouse in **Microsoft Fabric** using **T-SQL**. It covers data ingestion into a Lakehouse, creating staging tables, defining fact and dimension tables, and performing analytical queries.

## 📌 Objectives

- Create a workspace and Lakehouse in Microsoft Fabric  
- Upload and ingest data from a CSV file  
- Create a data warehouse with fact and dimension tables  
- Use a view and stored procedure to load data  
- Run analytical queries using T-SQL  

---

## 🚀 Steps

### 1. Create a Workspace
- Go to: [https://app.fabric.microsoft.com](https://app.fabric.microsoft.com)
- Create a new workspace (choose Fabric Trial or Premium)

### 2. Create a Lakehouse and Upload Data
- Create a new Lakehouse
- Download this file: [`sales.csv`](https://github.com/MicrosoftLearning/dp-data/raw/main/sales.csv)
- Upload `sales.csv` into the **Files** section of your Lakehouse
- Right-click `sales.csv` → **Load to Table** → Name it `staging_sales`

### 3. Create a Data Warehouse
- Create a new Data Warehouse resource from the Fabric menu
- In the SQL editor, run:

```sql
CREATE SCHEMA [Sales];
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Fact_Sales' AND SCHEMA_NAME(schema_id)='Sales')
CREATE TABLE Sales.Fact_Sales (
    CustomerID VARCHAR(255) NOT NULL,
    ItemID VARCHAR(255) NOT NULL,
    SalesOrderNumber VARCHAR(30),
    SalesOrderLineNumber INT,
    OrderDate DATE,
    Quantity INT,
    TaxAmount FLOAT,
    UnitPrice FLOAT
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Dim_Customer' AND SCHEMA_NAME(schema_id)='Sales')
CREATE TABLE Sales.Dim_Customer (
    CustomerID VARCHAR(255) NOT NULL,
    CustomerName VARCHAR(255) NOT NULL,
    EmailAddress VARCHAR(255) NOT NULL
);
ALTER TABLE Sales.Dim_Customer ADD CONSTRAINT PK_Dim_Customer PRIMARY KEY NONCLUSTERED (CustomerID) NOT ENFORCED;
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Dim_Item' AND SCHEMA_NAME(schema_id)='Sales')
CREATE TABLE Sales.Dim_Item (
    ItemID VARCHAR(255) NOT NULL,
    ItemName VARCHAR(255) NOT NULL
);
ALTER TABLE Sales.Dim_Item ADD CONSTRAINT PK_Dim_Item PRIMARY KEY NONCLUSTERED (ItemID) NOT ENFORCED;
GO
```

---

### 4. Create a View (Update Lakehouse Name)

```sql
CREATE VIEW Sales.Staging_Sales
AS
SELECT * FROM [<your lakehouse name>].[dbo].[staging_sales];
```

Replace `<your lakehouse name>` with your actual Lakehouse name.

---

### 5. Create Stored Procedure to Load Data

```sql
CREATE OR ALTER PROCEDURE Sales.LoadDataFromStaging (@OrderYear INT)
AS
BEGIN
    INSERT INTO Sales.Dim_Customer (CustomerID, CustomerName, EmailAddress)
    SELECT DISTINCT CustomerName, CustomerName, EmailAddress
    FROM Sales.Staging_Sales
    WHERE YEAR(OrderDate) = @OrderYear
    AND NOT EXISTS (
        SELECT 1 FROM Sales.Dim_Customer
        WHERE CustomerName = Sales.Staging_Sales.CustomerName
        AND EmailAddress = Sales.Staging_Sales.EmailAddress
    );

    INSERT INTO Sales.Dim_Item (ItemID, ItemName)
    SELECT DISTINCT Item, Item
    FROM Sales.Staging_Sales
    WHERE YEAR(OrderDate) = @OrderYear
    AND NOT EXISTS (
        SELECT 1 FROM Sales.Dim_Item
        WHERE ItemName = Sales.Staging_Sales.Item
    );

    INSERT INTO Sales.Fact_Sales (CustomerID, ItemID, SalesOrderNumber, SalesOrderLineNumber, OrderDate, Quantity, TaxAmount, UnitPrice)
    SELECT CustomerName, Item, SalesOrderNumber, CAST(SalesOrderLineNumber AS INT),
           CAST(OrderDate AS DATE), CAST(Quantity AS INT), CAST(TaxAmount AS FLOAT), CAST(UnitPrice AS FLOAT)
    FROM Sales.Staging_Sales
    WHERE YEAR(OrderDate) = @OrderYear;
END;
```

---

### 6. Run the Procedure

```sql
EXEC Sales.LoadDataFromStaging 2021;
```

---

### 7. Analytical Queries

**Total Sales by Customer:**
```sql
SELECT c.CustomerName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
FROM Sales.Fact_Sales s
JOIN Sales.Dim_Customer c ON s.CustomerID = c.CustomerID
WHERE YEAR(s.OrderDate) = 2021
GROUP BY c.CustomerName
ORDER BY TotalSales DESC;
```

**Top-Selling Items:**
```sql
SELECT i.ItemName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
FROM Sales.Fact_Sales s
JOIN Sales.Dim_Item i ON s.ItemID = i.ItemID
WHERE YEAR(s.OrderDate) = 2021
GROUP BY i.ItemName
ORDER BY TotalSales DESC;
```

**Top Customer by Category:**
```sql
WITH CategorizedSales AS (
    SELECT
        CASE
            WHEN i.ItemName LIKE '%Helmet%' THEN 'Helmet'
            WHEN i.ItemName LIKE '%Bike%' THEN 'Bike'
            WHEN i.ItemName LIKE '%Gloves%' THEN 'Gloves'
            ELSE 'Other'
        END AS Category,
        c.CustomerName,
        s.UnitPrice * s.Quantity AS Sales
    FROM Sales.Fact_Sales s
    JOIN Sales.Dim_Customer c ON s.CustomerID = c.CustomerID
    JOIN Sales.Dim_Item i ON s.ItemID = i.ItemID
    WHERE YEAR(s.OrderDate) = 2021
),
RankedSales AS (
    SELECT
        Category,
        CustomerName,
        SUM(Sales) AS TotalSales,
        ROW_NUMBER() OVER (PARTITION BY Category ORDER BY SUM(Sales) DESC) AS SalesRank
    FROM CategorizedSales
    WHERE Category IN ('Helmet', 'Bike', 'Gloves')
    GROUP BY Category, CustomerName
)
SELECT Category, CustomerName, TotalSales
FROM RankedSales
WHERE SalesRank = 1
ORDER BY TotalSales DESC;
```

---

## 🧹 Clean Up

To delete the workspace:
- Open the workspace in Microsoft Fabric
- Go to **Workspace Settings** → Scroll down and select **Remove this workspace**

---

## 🔗 Resources

- Microsoft Fabric: https://app.fabric.microsoft.com  
- CSV File: [sales.csv](https://github.com/MicrosoftLearning/dp-data/raw/main/sales.csv)  
- GitHub Repo: [Your GitHub Link Here]

---

## 🧾 License

MIT License
