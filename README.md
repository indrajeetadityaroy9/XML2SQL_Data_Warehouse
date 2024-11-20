# XML Data Warehouse for Pharmaceutical Sales Data Forecasting and Analysis
Data warehouse implementation for pharmaceutical sales data, which integrates sales representative, transactional data, sales data, customer data and product data. The system handles complex XML structures and utilizes ETL pipelines for data extraction, transformation, and loading into both OLTP and OLAP systems for operational and analytical purposes.

## Key Components
- Data extraction from XML files.
- Tranformation of XML data into relational database compliant format.
- Design and implementation of an OLTP database schema.
- Data Warehouse Design for OLAP
- ETL Pipelines for Data Warehousing
- Data Analysis and Visualization

## DB Schema
#### reps
| Field       | Type   |
|-------------|--------|
| rID (PK)    | int    |
| firstName   | text   |
| surName     | text   |
| territory   | text   |
| comission   | text   |

#### transactions
| Field       | Type   |
|-------------|--------|
| tID (PK)    | int    |
| txnID       | int    |

#### currency
| Field       | Type   |
|-------------|--------|
| cuID (PK)   | int    |
| currency    | text   |

#### sales
| Field       | Type   |
|-------------|--------|
| sID (PK)    | int    |
| txnID (FK)  | int    |
| rID (FK)    | int    |
| cID (FK)    | int    |
| pID (FK)    | int    |
| date        | date   |
| qty         | int    |
| total       | int    |
| cuID (FK)   | int    |

#### customers
| Field         | Type   |
|---------------|--------|
| cID (PK)      | int    |
| customerName  | text   |
| country       | text   |

#### products
| Field        | Type   |
|--------------|--------|
| pID (PK)     | int    |
| productName  | text   |

## Data Warehouse Star Schema
#### dimRegion
| Field        | Type   |
|--------------|--------|
| regionID (PK)| int    |
| territory    | text   |

#### dimProduct
| Field        | Type   |
|--------------|--------|
| pID (PK)     | int    |
| productName  | text   |

#### dimDate
| Field        | Type   |
|--------------|--------|
| dateID (PK)  | int    |
| date         | date   |
| year         | int    |
| quarter      | int    |

#### dimRep
| Field        | Type   |
|--------------|--------|
| rID (PK)     | int    |
| firstName    | text   |
| surName      | text   |

#### sales_facts
| Field         | Type   |
|---------------|--------|
| sfID (PK)     | int    |
| dateID (FK)   | int    |
| regionID (FK) | int    |
| totalSales    | int    |
| totalUnits    | int    |

#### rep_facts
| Field         | Type   |
|---------------|--------|
| rfID (PK)     | int    |
| dateID (FK)   | int    |
| pID (FK)      | int    |
| rID (FK)      | int    |
| totalSales    | int    |

## Architecture

### DB Schema

  ![image](https://github.com/indrajeetadityaroy9/xml_data_warehouse/assets/53830950/0eb7770b-6cf1-445c-b0bc-a3eb7a0eae14)

### Data Warehouse Schema

![image](https://github.com/indrajeetadityaroy9/xml_data_warehouse/assets/53830950/c702981c-96d8-4dc2-aa5d-5cbbd79edcfa)
