[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Altibox dk_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Altibox dk" = let
  Source = GoogleAnalytics.Accounts([Implementation = "2.0"]),
  #"Navigation 1" = Source{[Id = "accounts/119871073"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "properties/292166739"]}[Data],
  Altibox.dk = #"Navigation 2"{[Id = "properties/292166739"]}[Data],
  #"Added items" = Cube.Transform(Altibox.dk, {{Cube.AddMeasureColumn, "activeUsers", "activeUsers"}, {Cube.AddAndExpandDimensionColumn, "audienceName", {"audienceName"}, {"audienceName"}}, {Cube.AddAndExpandDimensionColumn, "country", {"country"}, {"country"}}, {Cube.AddAndExpandDimensionColumn, "date", {"date"}, {"date"}}, {Cube.AddAndExpandDimensionColumn, "unifiedPagePathScreen", {"unifiedPagePathScreen"}, {"unifiedPagePathScreen"}}})
in
  #"Added items";
shared #"Altibox dk_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6b06974a-4346-4a38-bc5a-d42e564a6bec"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "3ccdc5e8-f799-4ab1-87cc-80fbf06f7929"]}[Data],
  TableNavigation = Navigation_2{[Id = "ActiveUsers", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
