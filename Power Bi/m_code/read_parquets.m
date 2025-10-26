// Main
let
    // Folder path
    SourceFolder = "Z:\Forecast-Accuracy-Analysis\SQL\outputs\parquet\",

    // Files
    Files = Folder.Files(SourceFolder),

    // Keep only .parquet files
    ParquetFiles = Table.SelectRows(#"Filtered Rows", each Text.EndsWith([Name], ".parquet")),

    // Add column that reads each Parquet file
    AddData = Table.AddColumn(
        ParquetFiles,
        "Data",
        each Parquet.Document([Content])
    ),

    // Extract the table name
    AddTableName = Table.AddColumn(
        AddData,
        "TableName",
        each Text.BeforeDelimiter([Name], ".parquet")
    ),

    // Keep only the name + data columns
    Clean = Table.SelectColumns(AddTableName, {"TableName", "Data"})
in
    Clean

// Rest of the queries reference Main!!!!

// dim_calendar
let
    Source = Main,
    #"Filtered Rows" = Table.SelectRows(Source, each ([TableName] = "dim_calendar")),
    Data = #"Filtered Rows"{0}[Data]
in
    Data

// dim_product
let
    Source = Main,
    #"Filtered Rows" = Table.SelectRows(Source, each ([TableName] = "dim_product")),
    Data = #"Filtered Rows"{0}[Data]
in
    Data

// dim_store
let
    Source = Main,
    #"Filtered Rows" = Table.SelectRows(Source, each ([TableName] = "dim_store")),
    Data = #"Filtered Rows"{0}[Data]
in
    Data

// dq_checks
let
    Source = Main,
    #"Filtered Rows" = Table.SelectRows(Source, each ([TableName] = "dq_results")),
    Data = #"Filtered Rows"{0}[Data]
in
    Data

// l2_sales_long_extended
let
    // Folder path
    SourceFolder = "Z:\Forecast-Accuracy-Analysis\SQL\outputs\parquet\splits\l2_sales_long_extended\",

    // Files
    Files = Folder.Files(SourceFolder),

    // Keep only .parquet files
    ParquetFiles = Table.SelectRows(#"Filtered Rows", each Text.EndsWith([Name], ".parquet")),

    // Add column that reads each Parquet file
    AddData = Table.AddColumn(
        ParquetFiles,
        "Data",
        each Parquet.Document([Content])
    ),

    // Extract the table name
    AddTableName = Table.AddColumn(
        AddData,
        "TableName",
        each Text.BeforeDelimiter([Name], ".parquet")
    ),

    // Keep only the name + data columns
    Clean = Table.SelectColumns(AddTableName, {"TableName", "Data"}),
    #"Expanded Data" = Table.ExpandTableColumn(Clean, "Data", {"product_key", "store_key", "date_key", "sell_price_key", "sales", "sales_value",
        "fc_r3m","fc_r12m","fc_naive","fc_snaive","fc_drift"}, {"product_key", "store_key", "date_key", "sell_price_key", "sales", "sales_value",
        "fc_r3m","fc_r12m","fc_naive","fc_snaive","fc_drift"}),
    #"Removed Columns" = Table.RemoveColumns(#"Expanded Data",{"TableName"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns",{{"product_key", Int64.Type}, {"store_key", Int64.Type}, {"date_key", Int64.Type}, {"sell_price_key", Int64.Type}, {"sales", Int64.Type}, {"sales_value", type number}, {"fc_r3m", type number}, {"fc_r12m", type number}, {"fc_naive", Int64.Type}, {"fc_snaive", Int64.Type}, {"fc_drift", type number}})
in
    #"Changed Type"

// Calendar Periods
let
    Source = Main,
    #"Filtered Rows" = Table.SelectRows(Source, each ([TableName] = "zdim_calendar_periods")),
    Data = #"Filtered Rows"{0}[Data]
in
    Data

// _measures
let
    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i44FAA==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [measures = _t])
in
    Source