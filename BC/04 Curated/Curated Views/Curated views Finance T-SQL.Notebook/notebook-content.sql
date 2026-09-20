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

-- MARKDOWN ********************

-- ## Create views for PBI — Finance Module
-- 
-- This notebook creates views for Finance reporting.
-- Unnecessary columns are dropped and column names are set to business-friendly names.

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Finance Transactions] as 
SELECT  
            [accountKey] as [AccountKey],
			[genBusinessPostingGroupKey] as [GenBusinessPostingGroupKey],
			[genProductPostingGroupKey] as [GenProductPostingGroupKey],
			CAST('-1' AS NVARCHAR(10)) AS [ContactKey],
			[companyKey] as [CompanyKey],
			
			
			--Created Enriched Date Missing
			--Ledger Entry No Missing
			
			[companyCurrencyCode] as [Currency Code LCY],
			[amountReportingLCY] as [Finance Amount LCY],
			[amountReportingGCY] as [Finance Amount GCY],
			--[P&L/Balance] as [P&L/Balance],
			[description] as [Finance Description],
			[documentNo] as [Finance Document Number],
			[documentType] as [Finance Document Type],
			
			-- Dates
			[documentDate] as [Document Date],
			CAST([postingDate] AS DATETIME) as [PostingDateKey],
			
			[sourceNo] as [Finance Source Number],
			[sourceType] as [Finance Source Type],
			[sourceCode] as [Finance Source Code],
			[sourceSystem] as [Data Source System],
		

			-- Custom Dimensions -- Modify by customer.
			[dse_AfdelingKey] as [dse_AfdelingKey],
			[dse_CustomerContractKey] as [dse_CustomerContractKey],
			[dse_NetProviderKey] as [dse_NetProviderKey]
    		
			-- CAST('-1' AS NVARCHAR(10)) AS [subVendorKey]
FROM [Enr].[dbo].[enr_glentries]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse",
-- META   "frozen": false,
-- META   "editable": true
-- META }

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Finance Budgets] as SELECT  
			[accountKey] as [AccountKey],

			[date] as [Date],
            
			[budgetName] as [Budget Name],
			[account] as [Account Number],
			[dataSource] as [Budget Data Source],
			[amountReportingLCY] as [Budget Amount],
			[amountReportingGCY] as [Budget Amount GCY],
			[companyKey] as [CompanyKey],
            --[amountReportingVAT] as [Amount Incl. VAT],
			--case when [account] < 60000 then 'P&L' else 'Balance' end as [P&L/Balance],

			-- Custom Dimensions -- Modify by customer.
			[dse_AfdelingKey] as [dse_AfdelingKey],
			[dse_CustomerContractKey] as [dse_CustomerContractKey],
			[dse_NetProviderKey] as [dse_NetProviderKey]
FROM [Enr].[dbo].[enr_budget]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Account] as SELECT 
            
			[accountKey] as [AccountKey],
			[accountScheduleKey] as [AccountScheduleKey],
			
			[accountNumberAndName] as [Account Number And Name],
			[accountNo] as [Account Number],
			[name] as [Account Name],
			
			[accountCategory] as [Account Category],
			[P&LSignSwitch] as [Account P&L Sign Switch],
			[accountType] as [Account Type],
			[incomeBalance] as [Account Statement Type]

			-- [CompensationAccount] as [CompensationAccount]
FROM [Enr].[dbo].[enr_glaccount]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Account Schedule] as
-- Two interchangeable sources of the P&L / Balance hierarchy, unioned:
--   * enr_accountschedule       — driven by BC Account Schedule / Financial Report lines
--   * enr_glaccounthierarchy    — built directly from the GL Account chart of accounts
-- The model always filters to ONE [Account Schedule Name], so the union is semantically safe.
-- leaf_account_key is CAST to varchar(20) on BOTH branches: BC account No is Code[20] and may be
-- non-numeric (e.g. 'Z5431'), so without the cast T-SQL UNION type precedence would try to convert
-- it to bigint and fail at query time. Levels extended to level1..level10 (fixed schema contract).
SELECT
         CAST([leaf_account_key] AS varchar(20)) as [AccountScheduleKey],
         CAST([leaf_account_key] AS varchar(20)) as [Account Number],

         [accountScheduleName] as [Account Schedule Name],
         [level1_Key] as [Level1Key],
         [level2_Key] as [Level2Key],
         [level3_Key] as [Level3Key],
         [level4_Key] as [Level4Key],
         [level5_Key] as [Level5Key],
         [level6_Key] as [Level6Key],
         [level7_Key] as [Level7Key],
         [level8_Key] as [Level8Key],
         [level9_Key] as [Level9Key],
         [level10_Key] as [Level10Key],
         [level1_Name] as [Account Schedule - Level 1],
         [level2_Name] as [Account Schedule - Level 2],
         [level3_Name] as [Account Schedule - Level 3],
         [level4_Name] as [Account Schedule - Level 4],
         [level5_Name] as [Account Schedule - Level 5],
         [level6_Name] as [Account Schedule - Level 6],
         [level7_Name] as [Account Schedule - Level 7],
         [level8_Name] as [Account Schedule - Level 8],
         [level9_Name] as [Account Schedule - Level 9],
         [level10_Name] as [Account Schedule - Level 10],
         -1 as [row_id], --deprecated not in source
         [fullAccountName] as [Account Name],
         [incomeBalance] as [Account Statement Type]
FROM [Enr].[dbo].[enr_accountschedule]
UNION ALL
SELECT
         CAST([leaf_account_key] AS varchar(20)) as [AccountScheduleKey],
         CAST([leaf_account_key] AS varchar(20)) as [Account Number],

         [accountScheduleName] as [Account Schedule Name],
         [level1_Key] as [Level1Key],
         [level2_Key] as [Level2Key],
         [level3_Key] as [Level3Key],
         [level4_Key] as [Level4Key],
         [level5_Key] as [Level5Key],
         [level6_Key] as [Level6Key],
         [level7_Key] as [Level7Key],
         [level8_Key] as [Level8Key],
         [level9_Key] as [Level9Key],
         [level10_Key] as [Level10Key],
         [level1_Name] as [Account Schedule - Level 1],
         [level2_Name] as [Account Schedule - Level 2],
         [level3_Name] as [Account Schedule - Level 3],
         [level4_Name] as [Account Schedule - Level 4],
         [level5_Name] as [Account Schedule - Level 5],
         [level6_Name] as [Account Schedule - Level 6],
         [level7_Name] as [Account Schedule - Level 7],
         [level8_Name] as [Account Schedule - Level 8],
         [level9_Name] as [Account Schedule - Level 9],
         [level10_Name] as [Account Schedule - Level 10],
         -1 as [row_id], --deprecated not in source
         [fullAccountName] as [Account Name],
         [incomeBalance] as [Account Statement Type]
FROM [Enr].[dbo].[enr_glaccounthierarchy]



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
