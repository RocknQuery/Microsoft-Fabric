[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "D USD EUR SP00_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"D USD EUR SP00" = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"), [Delimiter = ",", Columns = 32, QuoteStyle = QuoteStyle.None]),
  SourceHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  SourcePrepared = Table.TransformColumnTypes(SourceHeaders, {{"KEY", type text}, {"FREQ", type text}, {"CURRENCY", type text}, {"CURRENCY_DENOM", type text}, {"EXR_TYPE", type text}, {"EXR_SUFFIX", type text}, {"TIME_PERIOD", type date}, {"OBS_VALUE", type number}, {"OBS_STATUS", type text}, {"OBS_CONF", type text}, {"OBS_PRE_BREAK", type text}, {"OBS_COM", type text}, {"TIME_FORMAT", type duration}, {"BREAKS", type text}, {"COLLECTION", type text}, {"COMPILING_ORG", type text}, {"DISS_ORG", type text}, {"DOM_SER_IDS", type text}, {"PUBL_ECB", type text}, {"PUBL_MU", type text}, {"PUBL_PUBLIC", type text}, {"UNIT_INDEX_BASE", type text}, {"COMPILATION", type text}, {"COVERAGE", type text}, {"DECIMALS", Int64.Type}, {"NAT_TITLE", type text}, {"SOURCE_AGENCY", type text}, {"SOURCE_PUB", type text}, {"TITLE", type text}, {"TITLE_COMPL", type text}, {"UNIT", type text}, {"UNIT_MULT", Int64.Type}}),
  SourceSorted = Table.Sort(SourcePrepared, {{"TIME_PERIOD", Order.Descending}}),
  Result = Table.TransformColumnTypes(SourceSorted, {{"TIME_FORMAT", type text}})
in
  Result;
shared #"D USD EUR SP00_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "ddfee769-f6e9-4ae9-b14e-55757d57d7ba"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "26ab4736-9299-4ffd-885b-8b13b5cf2af9"]}[Data],
  TableNavigation = Navigation_2{[Id = "USD_EUR_Rates_Bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
