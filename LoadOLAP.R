#title: Part 2 - Create Star Schema
#assignment: Practicum II - CS5200
#authors: Indrajeet Roy, Nhat Pham
#date: December 5 2023

library(DBI)
library(RSQLite)
library(RMySQL)

# 1-2. Connect with mySQL database and sqlite database
# Get mysql connection
close_all_connections <- function() {
  all_cons <- dbListConnections(RMySQL::MySQL())
  for(con in all_cons) {
    dbDisconnect(con)
  }
}
get_db_connection <- function() {
  close_all_connections()
  dbcon <- dbConnect(RMySQL::MySQL(), dbname = "cs5200", 
                     host = "34.69.127.246", port = 3306, 
                     user = "root", password = "cs5200") 
  return(dbcon)
}
mysqlconn <- get_db_connection()

# Get existing sqlite connection
current_directory_path <- getwd()
dbfile = "TxnDB.sqlite"
sqliteconn <- dbConnect(RSQLite::SQLite(), file.path(current_directory_path, dbfile))

# 3. Create and populate an analytical DB in mysql with fact tables and dimension tables

# Drop all tables
dbGetQuery(mysqlconn, "DROP TABLE IF EXISTS sales_facts;")
dbGetQuery(mysqlconn, "DROP TABLE IF EXISTS rep_facts;")
dbGetQuery(mysqlconn, "DROP TABLE IF EXISTS dimDate;")
dbGetQuery(mysqlconn, "DROP TABLE IF EXISTS dimRegion;")
dbGetQuery(mysqlconn, "DROP TABLE IF EXISTS dimProduct;")
dbGetQuery(mysqlconn, "DROP TABLE IF EXISTS dimRep;")


# 3.1 Create "sales_facts", "rep_facts" fact tables and associating dimension tables:
# dimRegion: contains unique territories/regions
dbExecute(mysqlconn, 
    "CREATE TABLE dimRegion (
    regionID INT AUTO_INCREMENT PRIMARY KEY,
    territory VARCHAR(100) UNIQUE
    )")

# dimDate: contains sale dates and associated year and quarter
dbExecute(mysqlconn, 
    "CREATE TABLE dimDate (
    dateID INT AUTO_INCREMENT PRIMARY KEY,
    saleDate DATE,
    year INT,
    quarter INT
    )")

# sales_facts: contains aggregated sales data - total sales and unites per region and date
dbExecute(mysqlconn, 
    "CREATE TABLE sales_facts (
    sfID INT AUTO_INCREMENT PRIMARY KEY,
    dateID INT,
    regionID INT,
    totalSales INT CHECK(totalSales > 0),
    totalUnits INT CHECK(totalUnits > 0),
    FOREIGN KEY (dateID) REFERENCES dimDate (dateID) ON DELETE SET NULL,
    FOREIGN KEY (regionID) REFERENCES dimRegion (regionID) ON DELETE SET NULL
    )")

# dimProduct: list all product names
dbExecute(mysqlconn,
    "CREATE TABLE dimProduct (
    pID INT PRIMARY KEY,
    productName VARCHAR(100) UNIQUE
    )")

# dimRep: lists all sales reps and their first and last names
dbExecute(mysqlconn,
    "CREATE TABLE dimRep (
    rID INT PRIMARY KEY,
    firstName VARCHAR(100),
    surName VARCHAR(100)
    )")

# rep_facts: aggregates total sales data by sales rep, product, and date
dbExecute(mysqlconn,
    "CREATE TABLE rep_facts (
    rfID INT AUTO_INCREMENT PRIMARY KEY,
    dateID INT,
    rID INT,
    pID INT,
    totalSales INT CHECK(totalSales > 0),
    totalUnits INT CHECK(totalUnits > 0),
    totalTxns INT CHECK(totalTxns > 0),
    FOREIGN KEY (dateID) REFERENCES dimDate (dateID) ON DELETE SET NULL,
    FOREIGN KEY (rID) REFERENCES dimRep (rID) ON DELETE SET NULL,
    FOREIGN KEY (pID) REFERENCES dimProduct (pID) ON DELETE SET NULL
    )")

# 3.2 Populate the tables

# dimDate
# Retrieve unique sale dates from the sqlite sales table
saleDates <- dbGetQuery(sqliteconn, "SELECT DISTINCT saleDate FROM sales")
# Convert sale dates to Date format, extract year and quarter
saleDateCv <- as.Date(saleDates$saleDate, format="%Y-%m-%d")
saleDates$year <- as.integer(format(saleDateCv, "%Y"))
saleDates$quarter <- as.integer(substr(quarters(saleDateCv),2,2)) 
# Insert processed date data into the dimDate table in MySQL
dbWriteTable(mysqlconn, "dimDate", saleDates, append = TRUE, row.names = FALSE)

#dimRegion
# Retrieve unique territories from the reps table
regions <- dbGetQuery(sqliteconn, "SELECT DISTINCT territory FROM reps")
# Insert unique territories into the dimRegion table in MySQL
dbWriteTable(mysqlconn, "dimRegion", regions, append = TRUE, row.names = FALSE)

#sales_facts 
# Query and aggregate sales data by territory and sale date from the sqlite db
salesData <- dbGetQuery(sqliteconn, 
  "SELECT r.territory, s.saleDate, SUM(s.total) AS totalSales, SUM(s.qty) AS totalUnits
  FROM sales s
  JOIN reps r ON s.repID = r.rID
  GROUP BY r.territory, s.saleDate")
# Get dateID and saleDate mappings from the dimDate table
dateIDs <- dbGetQuery(mysqlconn, "SELECT dateID, saleDate FROM dimDate")
# Get regionID and territory mappings from the dimRegion table
regionIDs <- dbGetQuery(mysqlconn, "SELECT regionID, territory FROM dimRegion")
# Merge sales data with dateIDs and regionIDs to get FK
salesData <- merge(salesData, dateIDs, by.x = "saleDate", by.y = "saleDate")
salesData <- merge(salesData, regionIDs, by.x = "territory", by.y = "territory")
# Prepare sales data and insert into sales_facts
salesFactData <- salesData[, c("dateID", "regionID", "totalSales", "totalUnits")]
dbWriteTable(mysqlconn, "sales_facts", salesFactData, append = TRUE, row.names = FALSE)

# dimProduct
# Retrieve product data from the products table in SQLite and load into dimProduct in MySQL
products <- dbGetQuery(sqliteconn, "SELECT pID, productName FROM products")
dbWriteTable(mysqlconn, "dimProduct", products, append = TRUE, row.names = FALSE)

# dimRep
# Retrieve rep data from the reps table in SQLite and load into dimRep in MySQL
reps <- dbGetQuery(sqliteconn, "SELECT rID, firstName, surName FROM reps")
dbWriteTable(mysqlconn, "dimRep", reps, append = TRUE, row.names = FALSE)

# rep_facts
# Query and aggregate sales data by rep, saleDate, and product from SQLite
repSalesData <- dbGetQuery(sqliteconn, 
  "SELECT r.rID, s.saleDate, s.pID, SUM(s.total) AS totalSales, SUM(s.qty) AS totalUnits, COUNT(s.sID) AS totalTxns
  FROM sales s
  JOIN reps r ON s.repID = r.rID
  JOIN products p ON s.pID = p.pID
  GROUP BY r.rID, s.saleDate, s.pID")
# Get date and product keys from MySQL
dateIDs <- dbGetQuery(mysqlconn, "SELECT dateID, saleDate FROM dimDate")
productIDs <- dbGetQuery(mysqlconn, "SELECT pID FROM dimProduct")
# Merge sales data with date and product IDs for FK associations
repSalesData <- merge(repSalesData, dateIDs, by.x = "saleDate", by.y = "saleDate")
repSalesData <- merge(repSalesData, productIDs, by.x = "pID", by.y = "pID")
# Prepare rep sales data and insert into the rep_facts
repFactData <- repSalesData[, c( "dateID", "rID", "pID", "totalSales", "totalUnits", "totalTxns")]
dbWriteTable(mysqlconn, "rep_facts", repFactData, append = TRUE, row.names = FALSE)


# 4. Testing queries to verify the star schema
      
# What is the total sold for each quarter of 2022 for all regions?
#query1 <- dbGetQuery(mysqlconn, 
#           "SELECT d.quarter, SUM(sf.totalSales) AS totalSales
#            FROM sales_facts sf
#            JOIN dimDate d USING (dateID)
#            WHERE d.year = 2022
#            GROUP BY d.quarter
#            ORDER BY d.quarter;")
#print(query1)

# How much was sold in 2022 in EMEA?
#query2 <- dbGetQuery(mysqlconn, 
#                     "SELECT d.year, r.territory, SUM(sf.totalSales) AS totalSales
#                      FROM sales_facts sf
#                      JOIN dimDate d USING (dateID)
#                      JOIN dimRegion r USING (regionID)
#                      WHERE d.year = 2022 AND r.territory = 'EMEA'
#                      GROUP BY r.territory;")
#print(query2)

# What is the total sold for each quarter of 2021 for 'Alaraphosol'?
#query3 <- dbGetQuery(mysqlconn, 
#           "SELECT d.quarter, p.productName, SUM(rf.totalSales) AS totalSales
#            FROM rep_facts rf
#            JOIN dimDate d USING (dateID)
#            JOIN dimProduct p USING (pID)
#            WHERE d.year = 2021 AND p.productName = 'Alaraphosol'
#            GROUP BY d.quarter
#            ORDER BY d.quarter;")
#print(query3)

# Which sales rep sold the most in 2022?
#query4 <- dbGetQuery(mysqlconn, 
#           "SELECT sub.rID, sub.firstName, sub.surName, sub.totalSales
#            FROM (
#                SELECT 
#                    rf.rID, 
#                    rep.firstName, 
#                    rep.surName, 
#                    SUM(rf.totalSales) AS totalSales,
#                    RANK() OVER (ORDER BY SUM(rf.totalSales) DESC) AS rankNum
#                FROM rep_facts rf
#                JOIN dimDate d USING (dateID)
#                JOIN dimRep rep USING (rID)
#                WHERE d.year = 2022
#                GROUP BY rf.rID, rep.firstName, rep.surName
#            ) sub
#            WHERE sub.rankNum = 1;")
#print(query4)

# Total sold per year per region
#query5 <- dbGetQuery(mysqlconn, 
#               "SELECT d.year, r.territory, SUM(sf.totalSales) as totalSales
#                FROM sales_facts sf
#                JOIN dimDate d USING(dateID)
#                JOIN dimRegion r USING(regionID)
#                GROUP BY d.year, r.territory
#                ORDER BY d.year, r.territory;")
#print(query5)

dbDisconnect(mysqlconn)
dbDisconnect(sqliteconn)