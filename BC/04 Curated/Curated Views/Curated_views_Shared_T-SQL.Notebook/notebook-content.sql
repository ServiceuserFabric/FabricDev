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

-- ## Create views for PBI — Shared Dimensions
-- 
-- Views shared across all modules. Always kept on deployment.
-- Includes common dimensions such as calendar, company, item, contacts, posting groups, and custom dimensions.

-- MARKDOWN ********************

-- ### Calendar

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Calendar] as SELECT 
			[Date] as [CalendarKey],
			[Date],
			[PreviousDay] as [Previous Day],
			[NextDay] as [Next Day],
			[year] as [Year],
			[IsoYear] as [Iso Year],
			[IsLeapYear] as [Is Leap Year],
			[Semester] as [Semester],
			[SemesterCaption] as [Semester Caption],
			[YearSemesterCaption] as [Year Semester Caption],
			[Quarter] as [Quarter],
			[QuarterCaption] as [Quarter Caption],
			[YearQuarterCaption] as [Year Quarter Caption],
			[FirstDayOfQuarter] as [First Day Of Quarter], 
			[LastDayOfQuarter] as [Last Day of Quarter],
			[Month] as [Month],
			[MonthName] as [Month Name],
			[MonthAbbr] as [Month Abbr.],
			[FirstDayOfMonth] as [First Day of Month],
			[LastDayOfMonth] as [Last Day of Month],
			[YearMonthCaption] as [Year Month Caption],
			[Week] as [Week],
			[YearWeekCaption] as [Year Week Caption],
			[IsoWeek] as [Iso Week],
			[IsoYearWeekCaption] as [Iso Year Week Caption],
			[DayOfYear] as [Day of Year],
			[DayOfMonth] as [Day of Month],
			[DayOfWeek] as [Day of Week],
			[IsoWeekday] as [Iso Week Day],
			[WeekdayName] as [Weekday Name],
			[IsWeekend] as [Is Weekend],
			[WeekLabel] as [Week Label],
			[MonthLabel] as [Month Label],
			[DayLabel] as [Day Label],
			[currentMonth] as [Current Month],
			[currentYear] as [Current Year],
			[last12Months] as [Last 12 Months],
			[upToCurrentMonth] as [Up To Current Month],
			[yearmonthdaynum] as [Year Month day Number],
			[RelativeMonth] as [Relative Month]
FROM [Enr].[dbo].[enr_calendar]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ### Item

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Item] as SELECT 
    -- [companyKey],
    [itemNumber] AS [Item Number],
    [description] AS [Item Description],
    [description2] AS [Item Description 2],
    [inventoryPostingGroup] AS [Item Inventory Posting Group],
    [itemCategoryCode] AS [Item Category Code],
    -- [lastDateModified] AS [Last Date Modified],
    [lastDirectCost] AS [Item Last Direct Cost],
    [itemUnitCost] AS [Item Unit Cost],
    [unitPrice] AS [Item Unit Price],
    [ItemCodeAndDescription] AS [Item Code And Description],
    -- [ItemProductCodeAndDescription] as [Item ProductCode And Description],
    [itemKey] as [ItemKey]
FROM [Enr].[dbo].[enr_items]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ### Company

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Company] as 
SELECT   
			[companyKey] as [CompanyKey],
			[companyCurrency] as [Company Currency],
			[companyName] as [Company Name],
			[companyCountry] as [Company Country]
FROM [Enr].[dbo].[enr_company]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ### Contact

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Contact] AS SELECT
    
    [contactNumber] AS [Contact Number],
    [address] AS [Contact Address],
    [address2] AS [Contact Address 2],
    [countryRegionCode] AS [Contact Country/Region Code],
    [icPartnerCode] AS [Contact IC Partner Code],
    [name] AS [Contact Name],
    [paymentMethodCode] AS [Contact Payment Method Code],
    [paymentTermsCode] AS [Contact Payment Terms Code],
    [paymentTermsId] AS [Contact Payment Terms ID],
    [salespersonCode] AS [Contact Default Salesperson],
    [purchaserCode] AS [Contact Default Purchaser Code],
    [contactKey] as [ContactKey]
    
FROM [Enr].[dbo].[enr_contact]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ### Customer

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Customer] AS
SELECT
        -- Keys
        [customerKey]           AS [CustomerKey],

        -- Descriptive fields
        [no]                    AS [Customer Number],
        [name]                  AS [Customer Name],
        [customerCodeandName]   AS [Customer Code and Name],
        [address]               AS [Customer Address],
        [address2]              AS [Customer Address 2],
        [city]                  AS [Customer City],
        [countryRegionCode]     AS [Customer Country],
        [paymentTermsCode]      AS [Customer Payment Terms],
        [paymentMethodCode]     AS [Customer Payment Method],
        [salespersonCode]       AS [Customer Default Salesperson],
        [email]                 AS [Customer Email]

FROM [Enr].[dbo].[enr_customer]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ### Salesperson / Purchaser

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Salesperson Purchaser] AS
SELECT
        -- Keys
        [salespersonPurchaserKey]       AS [SalespersonPurchaserKey],

        -- Descriptive fields
        [code]                          AS [Salesperson Purchaser Code],
        [name]                          AS [Salesperson Purchaser Name],
        [email]                         AS [Salesperson Purchaser Email]

FROM [Enr].[dbo].[enr_salespersonpurchaser]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ### Posting Groups

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Gen Business Posting Group] AS
SELECT
	[genBusinessPostingGroupKey]         AS [GenBusinessPostingGroupKey],
	[genBusinessPostingGroupsCode]       AS [Gen Business Posting Group Code],
	[genBusinessPostingGroupsDescription] AS [Gen Business Posting Group Description]
FROM [Enr].[dbo].[enr_genbusinesspostinggroups]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[Gen Product Posting Group] AS
SELECT
	[genProductPostingGroupKey]         AS [GenProductPostingGroupKey],
	[genProductPostingGroupsCode]       AS [Gen Product Posting Group Code],
	[genProductPostingGroupsDescription] AS [Gen Product Posting Group Description]
FROM [Enr].[dbo].[enr_genproductpostinggroups]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ### Custom Dimensions

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[dse_Afdeling] as 
SELECT      
			[dse_AfdelingCode] as [Afdeling Code],
			[dse_AfdelingName] as [Afdeling Name],
			[dse_AfdelingKey] as [dse_AfdelingKey],
			[dse_AfdelingCode_And_Name] as [Afdeling Code And Name],
			[dse_AfdelingCode_And_Name_Reporting] as [Afdeling Code And Name Reporting]
FROM [Enr].[dbo].[enr_dse_afdeling]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[dse_CustomerContract] as 
SELECT      
			[dse_CustomerContractCode] as [CustomerContract Code],
			[dse_CustomerContractName] as [CustomerContract Name],
			[dse_CustomerContractKey] as [dse_CustomerContractKey],
			[dse_CustomerContractCode_And_Name] as [CustomerContract Code And Name],
			[dse_CustomerContractCode_And_Name_Reporting] as [CustomerContract Code And Name Reporting]
FROM [Enr].[dbo].[enr_dse_customercontract]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW [pbi].[dse_NetProvider] as 
SELECT      
			[dse_NetProviderCode] as [NetProvider Code],
			[dse_NetProviderName] as [NetProvider Name],
			[dse_NetProviderKey] as [dse_NetProviderKey],
			[dse_NetProviderCode_And_Name] as [NetProvider Code And Name],
			[dse_NetProviderCode_And_Name_Reporting] as [NetProvider Code And Name Reporting]
FROM [Enr].[dbo].[enr_dse_netprovider]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
