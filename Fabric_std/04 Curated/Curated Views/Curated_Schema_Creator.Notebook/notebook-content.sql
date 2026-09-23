-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "27adf9a3-7d79-4d54-bb81-7e559e1442ef",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "27adf9a3-7d79-4d54-bb81-7e559e1442ef",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

USE Cur;
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pbi')
BEGIN
    EXEC('CREATE SCHEMA pbi')
END

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
