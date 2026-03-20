[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Customer_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "accounts_address", DestinationColumnName = "accounts_address"], [SourceColumnName = "accounts_company", DestinationColumnName = "accounts_company"], [SourceColumnName = "AccCustGroupKey", DestinationColumnName = "AccCustGroupKey"], [SourceColumnName = "accounts_email", DestinationColumnName = "accounts_email"], [SourceColumnName = "accounts_id", DestinationColumnName = "accounts_id"], [SourceColumnName = "accounts_invoiceBy", DestinationColumnName = "accounts_invoiceBy"], [SourceColumnName = "accounts_mobilepayTicket", DestinationColumnName = "accounts_mobilepayTicket"], [SourceColumnName = "accounts_modified", DestinationColumnName = "accounts_modified"], [SourceColumnName = "country", DestinationColumnName = "country"], [SourceColumnName = "customAttributes_internalsaleinitials", DestinationColumnName = "customAttributes_internalsaleinitials"], [SourceColumnName = "customerGroupKey", DestinationColumnName = "customerGroupKey"], [SourceColumnName = "dawaId", DestinationColumnName = "dawaId"], [SourceColumnName = "endDate", DestinationColumnName = "endDate"], [SourceColumnName = "floor", DestinationColumnName = "floor"], [SourceColumnName = "memberNumber", DestinationColumnName = "memberNumber"], [SourceColumnName = "mobile", DestinationColumnName = "mobile"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "number", DestinationColumnName = "number"], [SourceColumnName = "side", DestinationColumnName = "side"], [SourceColumnName = "startDate", DestinationColumnName = "startDate"], [SourceColumnName = "street", DestinationColumnName = "street"], [SourceColumnName = "Zip", DestinationColumnName = "Zip"], [SourceColumnName = "City", DestinationColumnName = "City"], [SourceColumnName = "ca_created", DestinationColumnName = "ca_created"], [SourceColumnName = "SalgVia", DestinationColumnName = "SalgVia"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Customer = let
  Source = Lakehouse.Contents(null){[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data]{[lakehouseId = "b1df83e8-4963-4129-b9a8-db8ced7800dd"]}[Data],
  #"Navigation 1" = Source{[Id = "all_data_customer_table", ItemKind = "Table"]}[Data],
  #"Choose columns" = Table.SelectColumns(#"Navigation 1", {"accounts_address", "accounts_company", "accounts_customerGroupKey", "accounts_email", "accounts_id", "accounts_invoiceBy", "accounts_mobilepayTicket", "accounts_modified", "country", "customAttributes_internalsaleinitials", "customerGroupKey", "dawaId", "endDate", "floor", "memberNumber", "mobile", "name", "number", "side", "startDate", "street", "zipCity", "customAttributes_created"}),
  #"Split column by character transition" = Table.SplitColumn(#"Choose columns", "zipCity", Splitter.SplitTextByCharacterTransition({"0".."9"}, (c) => not List.Contains({"0".."9"}, c)), {"zipCity.1", "zipCity.2"}),
  #"Invoked custom function" = Table.TransformColumnTypes(Table.AddColumn(#"Split column by character transition", "zipCity.1.1", each TryConvertToInt([zipCity.1])), {{"zipCity.1.1", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Invoked custom function", {"zipCity.1"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns", {{"zipCity.1.1", "zipCity.1"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns 3", {"accounts_customerGroupKey", "accounts_id", "accounts_invoiceBy", "accounts_mobilepayTicket", "accounts_modified", "country", "customAttributes_internalsaleinitials", "customerGroupKey", "endDate", "memberNumber", "name", "startDate", "zipCity.1", "zipCity.2", "customAttributes_created"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns", {{"zipCity.1", type number}, {"zipCity.2", type text}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "Custom", each if [customAttributes_internalsaleinitials] = null or [customAttributes_internalsaleinitials] = "" then "Web" else "Tlf"),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"Custom", "SalgVia"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"SalgVia", type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type 1", "", "Danmark", Replacer.ReplaceValue, {"country"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Replaced value", {{"zipCity.1", "Zip"}, {"zipCity.2", "City"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns 1", {{"endDate", Order.Descending}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Sorted rows", {{"endDate", type datetime}, {"startDate", type datetime}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Changed column type 2", {{"startDate", type date}, {"endDate", type date}, {"accounts_modified", type datetime}}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Changed column type 3", {{"accounts_modified", type date}, {"memberNumber", Int64.Type}}),
  #"Sorted rows 1" = Table.Sort(#"Changed column type 4", {{"accounts_id", Order.Ascending}, {"memberNumber", Order.Ascending}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Sorted rows 1", {{"accounts_customerGroupKey", "AccCustGroupKey"},{"customAttributes_created","ca_created"}}),
  #"Changed column type 5" = Table.TransformColumnTypes(#"Renamed columns 2", {{"memberNumber", type text}}),
  #"Fjernede dubletter" = Table.Distinct(#"Changed column type 5", {"accounts_id"})
in
  #"Fjernede dubletter";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Cust prod_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Cust prod" = let
  Source = Lakehouse.Contents(null){[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data]{[lakehouseId = "b1df83e8-4963-4129-b9a8-db8ced7800dd"]}[Data],
  #"Navigation 1" = Source{[Id = "all_data_cust_prod", ItemKind = "Table"]}[Data],
  #"Choose columns" = Table.SelectColumns(#"Navigation 1", {"accountKey", "activeProvisioning", "createdISO", "customAttributes_infrastructureownercode", "customerKey", "endDate", "hasBeenBilledOnce", "id", "modifiedISO", "orderedDate", "productConf_billingPeriodUnit", "productConf_customAttributes_infrastructureownercode", "productConf_customAttributes_pricecategorycode", "productConf_id", "productConf_provisioningModel", "productConf_selfserviceNames_da", "productConf_subscriptions_discount_rule", "productConf_subscriptions_discount_value", "productConf_subscriptions_price", "quantity", "startDate", "billDate"}),
  #"Removed duplicates" = Table.Distinct(#"Choose columns", {"id"}),
  #"Removed blank rows" = Table.SelectRows(#"Removed duplicates", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed blank rows", {{"startDate", type datetime}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type 2", "Custom", each [startDate] + #duration(0, 2, 0, 0)), {{"Custom", type datetime}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Added custom", {{"endDate", type datetime}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type 3", "Custom (2)", each [endDate] + #duration(0, 2, 0, 0)), {{"Custom (2)", type datetime}}),
  #"Sorted rows" = Table.Sort(#"Added custom 1", {{"Custom (2)", Order.Descending}}),
  #"Renamed columns 4" = Table.RenameColumns(#"Sorted rows", {{"endDate", "endDatePanther"}, {"Custom (2)", "EndDate"}}),
  #"Renamed columns 3" = Table.RenameColumns(#"Renamed columns 4", {{"startDate", "startDatePanther"}, {"Custom", "startDate"}}),
  #"Calculated date" = Table.TransformColumns(#"Renamed columns 3", {{"startDate", each DateTime.Date(_), type nullable date}}),
  #"Reordered columns 2" = Table.ReorderColumns(#"Calculated date", {"accountKey", "createdISO", "customAttributes_infrastructureownercode", "customerKey", "id", "modifiedISO", "productConf_customAttributes_infrastructureownercode", "productConf_id", "productConf_selfserviceNames_da", "quantity", "startDatePanther", "startDate", "endDatePanther", "EndDate"}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Reordered columns 2", {{"createdISO", type datetime}, {"modifiedISO", type datetime}}),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type 4", "Custom", each [createdISO] + #duration(0, 2, 0, 0)), {{"Custom", type datetime}}),
  #"Renamed columns 5" = Table.RenameColumns(#"Added custom 2", {{"createdISO", "CreatedPanther"}, {"Custom", "Created"}}),
  #"Reordered columns 3" = Table.ReorderColumns(#"Renamed columns 5", {"accountKey", "customAttributes_infrastructureownercode", "customerKey", "id", "modifiedISO", "productConf_customAttributes_infrastructureownercode", "productConf_id", "productConf_selfserviceNames_da", "quantity", "startDatePanther", "startDate", "endDatePanther", "EndDate", "CreatedPanther", "Created"}),
  #"Renamed columns 6" = Table.RenameColumns(#"Reordered columns 3", {{"Created", "createdISO"}}),
  #"Added custom 3" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns 6", "Custom", each [modifiedISO] + #duration(0, 2, 0, 0)), {{"Custom", type datetime}}),
  #"Renamed columns 7" = Table.RenameColumns(#"Added custom 3", {{"modifiedISO", "modifiedPanther"}, {"Custom", "modifiedISO"}}),
  #"Reordered columns 4" = Table.ReorderColumns(#"Renamed columns 7", {"accountKey", "customAttributes_infrastructureownercode", "customerKey", "id", "productConf_customAttributes_infrastructureownercode", "productConf_id", "productConf_selfserviceNames_da", "quantity", "startDatePanther", "startDate", "endDatePanther", "EndDate", "CreatedPanther", "createdISO", "modifiedPanther", "modifiedISO"}),
  #"Renamed columns 8" = Table.RenameColumns(#"Reordered columns 4", {{"EndDate", "endDate"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 8", {"accountKey", "customerKey", "productConf_id"}, false),
  #"Renamed columns" = Table.RenameColumns(#"Marked key columns", {{"modifiedISO", "modifiedDate"}, {"startDate", "startDate"}, {"endDate", "endDate"}, {"createdISO", "createdDate"}, {"productConf_customAttributes_infrastructureownercode", "IO"}, {"productConf_selfserviceNames_da", "produktName"}}),
  #"Merged queries" = Table.NestedJoin(#"Renamed columns", {"IO"}, IO, {"key"}, "all_data_io", JoinKind.LeftOuter),
  #"Expanded IO" = Table.ExpandTableColumn(#"Merged queries", "all_data_io", {"value"}, {"value"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded IO", {"accountKey"}, Customer, {"accounts_id"}, "all_data_customer_table", JoinKind.LeftOuter),
  #"Expanded Customer" = Table.ExpandTableColumn(#"Merged queries 1", "all_data_customer_table", {"memberNumber"}, {"memberNumber"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded Customer", {"id", "accountKey", "customerKey", "productConf_id", "createdDate", "modifiedDate", "startDate", "endDate", "produktName", "memberNumber", "quantity", "IO", "value"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"value", "Partner"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns 1", {{"productConf_id", Int64.Type}, {"customerKey", Int64.Type}, {"id", Int64.Type}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type 1", {"IO", "Partner"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"customAttributes_infrastructureownercode", "IO"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns 2", {"id", "IO", "accountKey", "customerKey", "productConf_id", "createdDate", "CreatedPanther", "modifiedDate", "modifiedPanther", "startDate", "startDatePanther", "endDate", "endDatePanther", "produktName", "memberNumber", "quantity"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns 1", {{"endDate", type date}, {"modifiedDate", type date}, {"createdDate", type date}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"CreatedPanther", "modifiedPanther", "startDatePanther", "endDatePanther"}),
  #"Added custom 4" = Table.TransformColumnTypes(Table.AddColumn(#"Removed columns", "Churn", each [endDate] - #duration(1, 0, 0, 0)), {{"Churn", type date}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Added custom 4", {"endDate"}),
  #"Renamed columns 9" = Table.RenameColumns(#"Removed columns 3", {{"Churn", "endDate"}}),
  #"Reordered columns 5" = Table.ReorderColumns(#"Renamed columns 9", {"id", "IO", "accountKey", "customerKey", "productConf_id", "createdDate", "modifiedDate", "startDate", "endDate", "produktName", "memberNumber", "quantity"}),
  #"Changed column type 5" = Table.TransformColumnTypes(#"Reordered columns 5", {{"orderedDate", type datetime}}),
  #"Added custom 5" = Table.AddColumn(#"Changed column type 5", "OrderedDate", each [orderedDate] + #duration(0, 2, 0, 0)),
  #"Reordered columns 6" = Table.ReorderColumns(#"Added custom 5", {"id", "activeProvisioning", "IO", "accountKey", "customerKey", "productConf_id", "hasBeenBilledOnce", "orderedDate", "OrderedDate", "createdDate", "modifiedDate", "startDate", "endDate", "produktName", "memberNumber", "quantity"}),
  #"Changed column type 6" = Table.TransformColumnTypes(#"Reordered columns 6", {{"OrderedDate", type datetime}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Changed column type 6", {"orderedDate"}),
  #"Renamed columns 10" = Table.RenameColumns(#"Removed columns 4", {{"OrderedDate", "orderedDate"}})
in
  #"Renamed columns 10";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "IO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "id", DestinationColumnName = "id"], [SourceColumnName = "key", DestinationColumnName = "key"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "value", DestinationColumnName = "value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared IO = let
  Source = Lakehouse.Contents(null){[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data]{[lakehouseId = "b1df83e8-4963-4129-b9a8-db8ced7800dd"]}[Data],
  #"Navigation 1" = Source{[Id = "all_data_io", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"id", Int64.Type}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Product config_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Product config" = let
  Source = Lakehouse.Contents(null){[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data]{[lakehouseId = "b1df83e8-4963-4129-b9a8-db8ced7800dd"]}[Data],
  #"Navigation 1" = Source{[Id = "all_data_product_config", ItemKind = "Table"]}[Data],
  #"Sorted rows 1" = Table.Sort(#"Navigation 1", {{"id", Order.Ascending}}),
  #"Choose columns" = Table.SelectColumns(#"Sorted rows 1", {"customAttributes_productbimaingroup", "customAttributes_productmaingroup", "id", "productGroup", "selfserviceNames_da"}),
  #"Reordered columns" = Table.ReorderColumns(#"Choose columns", {"id", "customAttributes_productbimaingroup", "productGroup", "selfserviceNames_da"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Reordered columns", {{"id", Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"id", Order.Ascending}}),
  #"Removed duplicates" = Table.Distinct(#"Sorted rows", {"id"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed duplicates", {{"customAttributes_productbimaingroup", "HovedProduktGruppe"}, {"customAttributes_productmaingroup", "ProductMainGroup"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns", {"id", "HovedProduktGruppe", "ProductMainGroup", "productGroup", "selfserviceNames_da"}),
  #"Duplicated column" = Table.DuplicateColumn(#"Reordered columns 1", "selfserviceNames_da", "selfserviceNames_da - Copy"),
  #"Split column by delimiter" = Table.SplitColumn(#"Duplicated column", "selfserviceNames_da - Copy", Splitter.SplitTextByDelimiter("/s "), {"selfserviceNames_da - Copy.1", "selfserviceNames_da - Copy.2"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Split column by delimiter", {{"selfserviceNames_da - Copy.1", type text}, {"selfserviceNames_da - Copy.2", type text}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"selfserviceNames_da - Copy.1", "Internet_speed"}, {"selfserviceNames_da - Copy.2", "Internet_category"}}),
  #"Replaced Value" 
        = Table.ReplaceValue(
                #"Renamed columns 1",
                each if [HovedProduktGruppe] <> "Internet" then [Internet_speed] else false,
                each "",
                Replacer.ReplaceValue,
                {"Internet_speed"}
            ),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced Value", {{"Internet_speed", type text}})
in
  #"Changed column type 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Status_I_dag__DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "id", DestinationColumnName = "id"], [SourceColumnName = "Dato", DestinationColumnName = "Dato"], [SourceColumnName = "accountKey", DestinationColumnName = "accountKey"], [SourceColumnName = "KundeNO", DestinationColumnName = "KundeNO"], [SourceColumnName = "ProduktID", DestinationColumnName = "ProduktID"], [SourceColumnName = "activeProvisioning", DestinationColumnName = "activeProvisioning"], [SourceColumnName = "hasBeenBilledOnce", DestinationColumnName = "hasBeenBilledOnce"], [SourceColumnName = "Partner", DestinationColumnName = "Partner"], [SourceColumnName = "Kunde_Status", DestinationColumnName = "Kunde_Status"], [SourceColumnName = "NySalg", DestinationColumnName = "NySalg"], [SourceColumnName = "Produkt_Status", DestinationColumnName = "Produkt_Status"], [SourceColumnName = "orderedDate", DestinationColumnName = "orderedDate"], [SourceColumnName = "orderedDateTime", DestinationColumnName = "orderedDateTime"], [SourceColumnName = "startDate", DestinationColumnName = "startDate"], [SourceColumnName = "endDate", DestinationColumnName = "endDate"], [SourceColumnName = "createdDate", DestinationColumnName = "createdDate"], [SourceColumnName = "modifiedDate", DestinationColumnName = "modifiedDate"], [SourceColumnName = "PC_DiscountValue", DestinationColumnName = "PC_DiscountValue"], [SourceColumnName = "PC_Sub_Price", DestinationColumnName = "PC_Sub_Price"], [SourceColumnName = "BillDate", DestinationColumnName = "BillDate"], [SourceColumnName = "PC_Pricecategorycode", DestinationColumnName = "PC_Pricecategorycode"], [SourceColumnName = "PC_Sub_discount_rule", DestinationColumnName = "PC_Sub_discount_rule"], [SourceColumnName = "PC_ProvisioningModel", DestinationColumnName = "PC_ProvisioningModel"], [SourceColumnName = "PC_BillingPeriodUnit", DestinationColumnName = "PC_BillingPeriodUnit"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Status_I_dag_ = let
  Source = Lakehouse.Contents(null){[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data]{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data],
  // The following is only in order to control execution dependencies - Start
  Dep1 = Customer,
  Dep2 = #"Cust prod",
  Dep3 = #"Product config",
  Dep4 = #"Cust_Group",
  #"Navigation 1" = Source{[Id = "Status_I_dag", ItemKind = "View"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 1", {{"KundeNO", Order.Ascending}}),
  #"From Value" = Table.FromValue(#"Sorted rows"),
  #"Remove Columns" = Table.RemoveColumns(#"From Value", Table.ColumnsOfType(#"From Value", {type table, type record, type list, type nullable binary, type binary, type function})),
  #"Sorted rows 1" = Table.Sort(#"Remove Columns", {{"id", Order.Descending}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Sorted rows 1", "orderedDate", "orderedDate - Copy"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column", {{"orderedDate", type date}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"orderedDate - Copy", "orderedDateTime"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"id", "Dato", "accountKey", "KundeNO", "ProduktID", "activeProvisioning", "hasBeenBilledOnce", "Partner", "Kunde_Status", "NySalg", "Produkt_Status", "orderedDate", "orderedDateTime", "startDate", "endDate", "createdDate", "modifiedDate", "PC_DiscountValue", "PC_Sub_Price", "BillDate", "PC_Pricecategorycode", "PC_Sub_discount_rule", "PC_ProvisioningModel", "PC_BillingPeriodUnit"}),
  #"Replaced value" = Table.ReplaceValue(#"Reordered columns", "NySalg", "1", Replacer.ReplaceText, {"NySalg"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value", {{"NySalg", Int64.Type}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Changed column type 1", null, 0, Replacer.ReplaceValue, {"NySalg"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 1", {{"NySalg", type logical}})
in
  #"Changed column type 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Cust_Group_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Cust_Group = let
  Source = Lakehouse.Contents(null){[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data]{[lakehouseId = "b1df83e8-4963-4129-b9a8-db8ced7800dd"]}[Data],
  #"Navigation 1" = Source{[Id = "all_data_customer_groups", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"paymentfeebspc_key", "paymentfeecardpc_key", "paymentfeeeanpc_key", "paymentfeeemailpc_key", "paymentfeepaperpc_key", "providerId", "residenttype", "note", "noDiscounts"})
in
  #"Removed columns";
shared DefaultStaging = let
  DefaultStaging = Pipeline.DefaultModelStorage()
in
  DefaultStaging;
shared IO_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data],
  TableNavigation = Navigation_2{[Id = "IO", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared DefaultDestination = let
  Kilde = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Kilde{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared TryConvertToInt = // Function: TryConvertToInt
// Input: Any value
// Output: The integer value if conversion succeeds, otherwise null
(value as any) as nullable number =>
let
    Converted = try Number.RoundDown(Number.From(value)) otherwise null
in
    Converted;
shared NavigationQuery = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation = Source{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data]
in
  #"Navigation 1";
shared #"Product config_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data],
  TableNavigation = Navigation_2{[Id = "Product_config", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Cust prod_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data],
  TableNavigation = Navigation_2{[Id = "Cust_prod", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Status_I_dag__DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data],
  TableNavigation = Navigation_2{[Id = "Status_I_dag_", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared Cust_Group_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data],
  TableNavigation = Navigation_2{[Id = "Cust_Group", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Customer_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ac6fbbda-655a-466b-abfa-ba456ca9605e"]}[Data],
  TableNavigation = Navigation_2{[Id = "Customer", ItemKind = "Table"]}[Data]
in
  TableNavigation;
