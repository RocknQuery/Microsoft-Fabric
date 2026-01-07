[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "indicator_value", DestinationColumnName = "indicator_value"], [SourceColumnName = "country_id", DestinationColumnName = "country_id"], [SourceColumnName = "country_value", DestinationColumnName = "country_value"], [SourceColumnName = "countryiso3code", DestinationColumnName = "countryiso3code"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "unit", DestinationColumnName = "unit"], [SourceColumnName = "obs_status", DestinationColumnName = "obs_status"], [SourceColumnName = "decimal", DestinationColumnName = "decimal"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Query = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json")),
  Navigation = Source{1},
  SourceTable = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(SourceTable, "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Expanded indicator" = Table.ExpandRecordColumn(#"Expanded Column1", "indicator", {"id", "value"}, {"indicator_id", "indicator_value"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded indicator", "country", {"id", "value"}, {"country_id", "country_value"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded country", {{"indicator_id", type text}, {"indicator_value", type text}, {"country_id", type text}, {"country_value", type text}, {"countryiso3code", type text}, {"unit", type text}, {"obs_status", type text}, {"date", Int64.Type}, {"decimal", Int64.Type}, {"value", type number}})
in
  #"Changed column type";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "ddfee769-f6e9-4ae9-b14e-55757d57d7ba"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "26ab4736-9299-4ffd-885b-8b13b5cf2af9"]}[Data],
  TableNavigation = Navigation_2{[Id = "GDP_USA_Bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
