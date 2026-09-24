CREATE VIEW CustomerAccountMap AS
SELECT [AllAccounts].[id], [AllAccounts].[accounts_id],  accounts_id_toUse = [AccountToBeUsedForCustomer].[accounts_id]
FROM [Bronze_lakehouse].[dbo].[all_data_customer_table] AllAccounts
LEFT JOIN (
	SELECT [id], [accounts_id]
	FROM [Bronze_lakehouse].[dbo].[all_data_customer_table]
	WHERE [accounts_login] = [accounts_memberNumber] --Så har vi kun entydige kunder tilbage (Entydige på customer.id)
) AccountToBeUsedForCustomer
	ON [AllAccounts].[id] = [AccountToBeUsedForCustomer].[id]

GO