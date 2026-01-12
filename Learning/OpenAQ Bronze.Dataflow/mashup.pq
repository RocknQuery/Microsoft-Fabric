[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "OpenAQ Query_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sensorId", DestinationColumnName = "sensorId"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "hasFlags", DestinationColumnName = "hasFlags"], [SourceColumnName = "metric_name", DestinationColumnName = "metric_name"], [SourceColumnName = "metric_units", DestinationColumnName = "metric_units"], [SourceColumnName = "period_label", DestinationColumnName = "period_label"], [SourceColumnName = "period_interval", DestinationColumnName = "period_interval"], [SourceColumnName = "utc_from", DestinationColumnName = "utc_from"], [SourceColumnName = "utc_to", DestinationColumnName = "utc_to"], [SourceColumnName = "summary.min", DestinationColumnName = "summary.min"], [SourceColumnName = "summary.q02", DestinationColumnName = "summary.q02"], [SourceColumnName = "summary.q25", DestinationColumnName = "summary.q25"], [SourceColumnName = "summary.median", DestinationColumnName = "summary.median"], [SourceColumnName = "summary.q75", DestinationColumnName = "summary.q75"], [SourceColumnName = "summary.q98", DestinationColumnName = "summary.q98"], [SourceColumnName = "summary.max", DestinationColumnName = "summary.max"], [SourceColumnName = "summary.avg", DestinationColumnName = "summary.avg"], [SourceColumnName = "summary.sd", DestinationColumnName = "summary.sd"], [SourceColumnName = "expectedCount", DestinationColumnName = "expectedCount"], [SourceColumnName = "expectedInterval", DestinationColumnName = "expectedInterval"], [SourceColumnName = "observedCount", DestinationColumnName = "observedCount"], [SourceColumnName = "observedInterval", DestinationColumnName = "observedInterval"], [SourceColumnName = "percentComplete", DestinationColumnName = "percentComplete"], [SourceColumnName = "percentCoverage", DestinationColumnName = "percentCoverage"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"OpenAQ Query" = let
    fnSensorMeasurements = (sensorId as number) =>
    let
        SourceMeasurements =
            Json.Document(
                Web.Contents(
                    "https://api.openaq.org",
                    [
                        RelativePath = "v3/sensors/" & Number.ToText(sensorId) & "/days/monthly",
                        Headers = [
                            #"X-API-Key" = p_API_Key
                        ],
                        Query = [
                            date_from = p_date_from,
                            date_to   = p_date_to
                        ]
                    ]
                )
            ),
        Results = SourceMeasurements[results]
    in
        Results,
    Source =
        Json.Document(
            Web.Contents(
                "https://api.openaq.org",
                [
                    RelativePath = "v3/locations",
                    Query = [
                      coordinates = "40.7, -74",
                      radius = "25000",
                      iso = "US"
                    ],
                    Headers = [
                        #"X-API-Key" = p_API_Key
                    ]
                ]
            )
        ),
  Navigation = Source[results],
  SourceRecords = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  SensorsList = Table.ExpandRecordColumn(SourceRecords, "Column1", {"sensors"}, {"sensors"}),
  SensorsRecords = Table.ExpandListColumn(SensorsList, "sensors"),
  Metrics = Table.ExpandRecordColumn(SensorsRecords, "sensors", {"id", "name", "parameter"}, {"id", "name", "parameter"}),
  MetricsTable = Table.ExpandRecordColumn(Metrics, "parameter", {"id", "name", "units", "displayName"}, {"metric_id", "metric_name", "metric_units", "metric_displayName"}),
  SensorsTable = Table.TransformColumnTypes(MetricsTable, {{"metric_id", Int64.Type}, {"metric_name", type text}, {"metric_units", type text}, {"metric_displayName", type text}}),
  SensorIdsAll = List.Distinct(SensorsTable[id]),
  SensorIds = List.FirstN(SensorIdsAll, p_max_API_limit),
  Tables = List.Transform(SensorIds, each [sensorId = _, rows = Function.InvokeAfter(() => fnSensorMeasurements(_), #duration(0, 0, 0, 1.5))]),
  SensorsResults = Table.FromList(Tables, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  SensorResultsExpanded = Table.ExpandRecordColumn(SensorsResults, "Column1", {"sensorId", "rows"}, {"sensorId", "rows"}),
  SensorResultsRows = Table.ExpandListColumn(SensorResultsExpanded, "rows"),
  #"Expanded rows" = Table.ExpandRecordColumn(SensorResultsRows, "rows", {"value", "flagInfo", "parameter", "period", "coordinates", "summary", "coverage"}, {"value", "flagInfo", "parameter", "period", "coordinates", "summary", "coverage"}),
  #"Expanded flagInfo" = Table.ExpandRecordColumn(#"Expanded rows", "flagInfo", {"hasFlags"}, {"hasFlags"}),
  #"Expanded parameter" = Table.ExpandRecordColumn(#"Expanded flagInfo", "parameter", {"name", "units"}, {"metric_name", "metric_units"}),
  #"Expanded period" = Table.ExpandRecordColumn(#"Expanded parameter", "period", {"label", "interval", "datetimeFrom", "datetimeTo"}, {"period_label", "period_interval", "datetimeFrom", "datetimeTo"}),
  #"Expanded datetimeFrom" = Table.ExpandRecordColumn(#"Expanded period", "datetimeFrom", {"utc"}, {"utc_from"}),
  #"Expanded datetimeTo" = Table.ExpandRecordColumn(#"Expanded datetimeFrom", "datetimeTo", {"utc"}, {"utc_to"}),
  #"Expanded summary" = Table.ExpandRecordColumn(#"Expanded datetimeTo", "summary", {"min", "q02", "q25", "median", "q75", "q98", "max", "avg", "sd"}, {"summary.min", "summary.q02", "summary.q25", "summary.median", "summary.q75", "summary.q98", "summary.max", "summary.avg", "summary.sd"}),
  #"Expanded coverage" = Table.ExpandRecordColumn(#"Expanded summary", "coverage", {"expectedCount", "expectedInterval", "observedCount", "observedInterval", "percentComplete", "percentCoverage"}, {"expectedCount", "expectedInterval", "observedCount", "observedInterval", "percentComplete", "percentCoverage"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded coverage", {{"sensorId", Int64.Type}, {"expectedCount", Int64.Type}, {"expectedInterval", type text}, {"observedCount", Int64.Type}, {"observedInterval", type text}, {"value", type number}, {"summary.min", type number}, {"summary.q02", type number}, {"summary.q25", type number}, {"summary.median", type number}, {"summary.q75", type number}, {"summary.q98", type number}, {"summary.max", type number}, {"summary.avg", type number}, {"summary.sd", type number}, {"percentComplete", type number}, {"percentCoverage", type number}, {"hasFlags", type logical}, {"metric_name", type text}, {"metric_units", type text}, {"period_label", type text}, {"period_interval", type text}, {"utc_from", type datetime}, {"utc_to", type datetime}})
in
    #"Changed column type";
[Description = "API Key for OpenAQ"]
shared p_API_Key = "91f189d9bfd43854108f4b2363ba73529c2a7ec5bc6471175eac440842fea391" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared p_date_from = "2024-01-01T00:00:00Z" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared p_date_to = "2026-01-01T00:00:00Z" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared p_max_API_limit = 1800 meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type any];
shared #"OpenAQ Query_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "ddfee769-f6e9-4ae9-b14e-55757d57d7ba"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "26ab4736-9299-4ffd-885b-8b13b5cf2af9"]}[Data],
  TableNavigation = Navigation_2{[Id = "OpenAQ_monthly_bronze", ItemKind = "Table"]}[Data]
in
  TableNavigation;
