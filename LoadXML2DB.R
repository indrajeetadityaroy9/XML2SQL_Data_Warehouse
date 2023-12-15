# 0. Set up
library(DBI)
library(XML)
library(RSQLite)

# Connect with a sqlite database
current_directory_path <- getwd()
dbfile = "TxnDB.sqlite"
dbconn <- dbConnect(RSQLite::SQLite(), file.path(current_directory_path, dbfile))
dbExecute(dbconn, "PRAGMA foreign_keys = ON")

# 1-2. Create R project, download xml files
# 3-4. Create a normalized relational schema and realize the schema in sqlite db
dbExecute(dbconn, "DROP TABLE IF EXISTS sales;")
dbExecute(dbconn, "DROP TABLE IF EXISTS reps;") 
dbExecute(dbconn, "DROP TABLE IF EXISTS products;") 
dbExecute(dbconn, "DROP TABLE IF EXISTS customers;")
dbExecute(dbconn, "DROP TABLE IF EXISTS currencies;")
dbExecute(dbconn, "DROP TABLE IF EXISTS sources;")

# reps: list the sales reps' info and territory
dbExecute(dbconn, "
CREATE TABLE reps (
    rID INTEGER PRIMARY KEY,
    firstName TEXT,
    surName TEXT,
    territory TEXT,
    commission REAL
);")

# products: lists the products being sold by the pharma company
dbExecute(dbconn, "
CREATE TABLE products (
    pID INTEGER PRIMARY KEY AUTOINCREMENT,
    productName TEXT UNIQUE 
);")

# customers: lists the customers buying products and their countries
dbExecute(dbconn, "
CREATE TABLE customers (
    cID INTEGER PRIMARY KEY AUTOINCREMENT,
    customerName TEXT,
    country TEXT 
);")

# currencies: look up table for the currency of the sale transaction
dbExecute(dbconn, "
CREATE TABLE currencies (
    cuID INTEGER PRIMARY KEY AUTOINCREMENT,
    currency TEXT UNIQUE 
);")

# sources: tracks the origin of each sale record (which XML file). Having this table
# supports data governance purposes, such as integrity checks and regulatory compliance
# by recording the link between sales data and their source files. It also addresses
# the non-uniqueness of 'txnID' across different XML files.
dbExecute(dbconn, "
CREATE TABLE sources (
    sourceID INTEGER PRIMARY KEY AUTOINCREMENT,
    sourceFile TEXT UNIQUE
);")


# sales: contains information on every sale transaction 
dbExecute(dbconn, "
CREATE TABLE sales (
    sID INTEGER PRIMARY KEY AUTOINCREMENT,
    repID INTEGER,
    cID INTEGER,
    pID INTEGER,
    saleDate DATE,
    qty INTEGER CHECK (qty > 0),  
    total INTEGER CHECK (total > 0),   
    cuID INTEGER,
    txnID INTEGER,
    sourceID INTEGER,
    FOREIGN KEY (repID) REFERENCES reps(rID) ON DELETE SET NULL,
    FOREIGN KEY (pID) REFERENCES products(pID) ON DELETE SET NULL,
    FOREIGN KEY (cID) REFERENCES customers(cID) ON DELETE SET NULL,
    FOREIGN KEY (cuID) REFERENCES currencies(cuID) ON DELETE SET NULL,
    FOREIGN KEY (sourceID) REFERENCES sources(sourceID) ON DELETE SET NULL
);")


# 5-6. Load XML files, extract and transform the data from the XML files, then 
# save the data into the appropriate tables in the database.

# 6.1 Function takes a path to a pharmaRep XML file and a db connection, 
# parses the XML, extracts rep data, and inserts it into the 'reps' table

insertRepsData <- function(reps_xml_file_path, dbconn) {
  # Parse the XML file
  reps_xml_data <- xmlParse(reps_xml_file_path, validate = FALSE)
  # Extract all 'rep' nodes from the parsed XML data.
  reps <- getNodeSet(reps_xml_data, "//rep")
  # Use lapply to iterate over each 'rep' node and extract relevant data.
  rep_data <- lapply(reps, function(node) {
    # Extract the 'rID' attribute from the node.
    rID <- xmlGetAttr(node, "rID")
    # Extract the 'first_name' from the 'name/first' sub-node.
    firstName <- xmlValue(getNodeSet(node, "name/first")[[1]])
    # Extract the 'last_name' from the 'name/sur' sub-node.
    surName <- xmlValue(getNodeSet(node, "name/sur")[[1]])
    # Extract the 'territory' value from the 'territory' sub-node.
    territory <- xmlValue(getNodeSet(node, "territory")[[1]])
    # Extract the 'commission' value from the 'commission' sub-node and convert it to numeric.
    commission <- as.numeric(xmlValue(getNodeSet(node, "commission")[[1]]))
    # Return a data frame constructed with the extracted values.
    return(data.frame(rID, firstName, surName, territory, commission))
  })
  # Combine all data frames into a single data frame.
  rep_df <- do.call(rbind, rep_data)
  # Convert the 'rID' column to integers.
  rep_df$rID <- as.integer(gsub("r", "", rep_df$rID))
  # Write the data frame to the 'reps' table in the database.
  dbWriteTable(dbconn, "reps", rep_df, append = TRUE, row.names = FALSE)
}

# 6.2 Function converts format of a dateString to a standard format
convertDate <- function(dateString) {
  formattedDate <- as.Date(dateString, format="%m/%d/%Y")
  return(format(formattedDate, "%Y-%m-%d"))
}

# 6.3 Function takes a path to a pharmaSales XML file, parses it, 
#and extracts sales data into a dataframe containing transaction details

extractSalesData <- function(xml_file_path) {
  # Parse the XML file. Validation is turned off.
  test_xml_data <- xmlParse(xml_file_path, validate = FALSE)
  # Extract all 'txn' nodes from the parsed XML data.
  txns <- getNodeSet(test_xml_data, "//txn")
  
  # Extract the file name from the file path
  sourceFile <- basename(xml_file_path)
  
  # Use lapply to iterate over each 'txn' node and extract data.
  sales_data <- lapply(txns, function(node) {
    # Extract the 'txnID' attribute from the node and convert to integer.
    txnID <- as.integer(xmlGetAttr(node, "txnID"))
    
    # Extract the 'repID' attribute and convert to integer.
    repID <- as.integer(gsub("r", "", xmlGetAttr(node, "repID")))
    # Extract the 'customer' value from the 'customer' sub-node.
    customer <- xmlValue(getNodeSet(node, "customer")[[1]])
    # Extract the 'country' value from the 'country' sub-node.
    country <- xmlValue(getNodeSet(node, "country")[[1]])
    
    saleNode <- getNodeSet(node, "sale")[[1]]
    saleDate <- convertDate(xmlValue(getNodeSet(saleNode, "date")[[1]]))
    
    # Extract the 'product' information from the 'sale/product' sub-node.
    product <- xmlValue(getNodeSet(saleNode, "product")[[1]])
    # Extract the 'quantity' from the 'sale/qty' sub-node and convert to integer.
    qty <- as.integer(xmlValue(getNodeSet(saleNode, "qty")[[1]]))
    # Extract the 'total' from the 'sale/total' sub-node and convert to numeric.
    total <- as.numeric(xmlValue(getNodeSet(saleNode, "total")[[1]]))
    # Extract the 'currency' attribute from the 'sale/total' sub-node.
    currency <- xmlGetAttr(getNodeSet(saleNode, "total")[[1]], "currency")
    # Return a data frame constructed with the extracted values
    return(data.frame(txnID, repID, customer, country, product, qty, total, currency, saleDate, sourceFile))  
  })
  # Combine all data frames into a single data frame.
  do.call(rbind, sales_data)
}

# 6.4 Function takes a sales data frame and a database connection, 
#and inserts processed sales, customer, product, source and currency data into corresponding database tables.

processAndLoadSalesData <- function(all_sales_data, dbconn) {
  # Extract unique customer names and countries from the sales data and rename the columns of the customers dataframe
  customers_df <- unique(all_sales_data[, c("customer", "country")])
  colnames(customers_df) <- c("customerName", "country")
  # Write the customers data to the 'customers' table in the database.
  dbWriteTable(dbconn, "customers", customers_df, append = TRUE, row.names = FALSE)
  
  # Extract unique product names from the sales data and rename column of the products dataframe
  products_df <- unique(all_sales_data["product"])
  colnames(products_df) <- "productName"
  # Write the products data to the 'products' table in the database.
  dbWriteTable(dbconn, "products", products_df, append = TRUE, row.names = FALSE)
  
  # Insert or update the currencies lookup table
  currencies_df <- unique(all_sales_data["currency"])
  dbWriteTable(dbconn, "currencies", currencies_df, append = TRUE, row.names = FALSE)
  
  # Insert or update the sources lookup table
  sources_df <- unique(all_sales_data["sourceFile"])
  dbWriteTable(dbconn, "sources", sources_df, append = TRUE, row.names = FALSE)
  
  # Retrieve the updated customers data from the database to get customer IDs.
  customer_ids_df <- dbGetQuery(dbconn, "SELECT cID, customerName FROM customers")
  # Merge the sales data with the customer data on the 'customer' field.
  all_sales_data <- merge(all_sales_data, customer_ids_df, by.x = "customer", by.y = "customerName")
  
  # Retrieve the updated products data from the database to get product IDs.
  product_ids_df <- dbGetQuery(dbconn, "SELECT pID, productName FROM products")
  # Merge the sales data with the product data on the 'product' field.
  all_sales_data <- merge(all_sales_data, product_ids_df, by.x = "product", by.y = "productName")
  
  # Get currency IDs
  currency_ids_df <- dbGetQuery(dbconn, "SELECT cuID, currency FROM currencies")
  all_sales_data <- merge(all_sales_data, currency_ids_df, by.x = "currency", by.y = "currency")
  
  # Get source IDs
  source_ids_df <- dbGetQuery(dbconn, "SELECT sourceID, sourceFile FROM sources")
  all_sales_data <- merge(all_sales_data, source_ids_df, by.x = "sourceFile", by.y = "sourceFile")
  
  # Select columns for the final sales data frame.
  sales_df <- all_sales_data[, c("repID", "cID", "pID", "qty", "total", "cuID", "saleDate", "txnID", "sourceID")] # 
  # Write the final sales data to the 'sales' table.
  dbWriteTable(dbconn, "sales", sales_df, append = TRUE, row.names = FALSE)
}

# 6.5 Process the XML files and load the data into the database
folder_path <- "txn-xml"

# Load reps data 
reps_xml_file_path <- list.files(folder_path, pattern = "pharmaReps.*\\.xml$", full.names = TRUE)[1]
insertRepsData(reps_xml_file_path, dbconn)

# Load sales data
sales_xml_files <- list.files(folder_path, pattern = "pharmaSalesTxn.*\\.xml$", full.names = TRUE)
combined_sales_data <- do.call(rbind, lapply(sales_xml_files, extractSalesData))
processAndLoadSalesData(combined_sales_data, dbconn)



# 6.6 Conversion scheme for saleDate: 
# Choose the date format "YYYY-MM-DD" to use consistently throughout the assignment, since it is a standard format for both sqlite and mysql
# Convert current format into chosen format before loading data to sqlite tables

# 7. Check query
# View first 10 rows of each table
#rTbl <- dbGetQuery(dbconn, "SELECT * FROM reps LIMIT 10")
#sTbl <- dbGetQuery(dbconn, "SELECT * FROM sales LIMIT 10")
#cTbl <- dbGetQuery(dbconn, "SELECT * FROM customers LIMIT 10")
#pTbl <- dbGetQuery(dbconn, "SELECT * FROM products LIMIT 10")
#cuTbl <- dbGetQuery(dbconn, "SELECT * FROM currencies")
#sourceTbl <- dbGetQuery(dbconn, "SELECT * FROM sources")
#print(rTbl)
#print(sTbl)
#print(cTbl)
#print(pTbl)
#print(cuTbl)
#print(sourceTbl)

# Count of rows
#rCt <- dbGetQuery(dbconn, "SELECT COUNT(*) as rCt FROM reps;")
#pCt <- dbGetQuery(dbconn, "SELECT COUNT(*) as pCt FROM products;")
#cCt <- dbGetQuery(dbconn, "SELECT COUNT(*) as cCt FROM customers;")
#sCt <- dbGetQuery(dbconn, "SELECT COUNT(*) as sCt FROM sales;")
#sourceCt <- dbGetQuery(dbconn, "SELECT COUNT(*) as sourceCt FROM sources;")
#print(paste("Count of reps:", rCt$rCt))
#print(paste("Count of products:", pCt$pCt))
#print(paste("Count of customers:", cCt$cCt))
#print(paste("Count of sales:", sCt$sCt))
#print(paste("Count of sources:", sourceCt$sourceCt))

# Check table structure
#print(dbGetQuery(dbconn, "PRAGMA table_info(reps);"))
#print(dbGetQuery(dbconn, "PRAGMA table_info(products);"))
#print(dbGetQuery(dbconn, "PRAGMA table_info(customers);"))
#print(dbGetQuery(dbconn, "PRAGMA table_info(sales);"))
#print(dbGetQuery(dbconn, "PRAGMA table_info(currencies);"))
#print(dbGetQuery(dbconn, "PRAGMA table_info(sources);"))

# Validate foreign key relationships (example using sales and reps)
#result1 <-  dbGetQuery(dbconn, "SELECT s.*, r.firstName, r.surName FROM sales s JOIN reps r ON s.repID = r.rID LIMIT 10;")
#print(result1)
# Date
#result2 <- dbGetQuery(dbconn, "SELECT sID, saleDate, qty, total, cuID FROM sales WHERE saleDate BETWEEN '2022-01-01' AND '2022-03-31' LIMIT 10;")
#print(result2)

dbDisconnect(dbconn)