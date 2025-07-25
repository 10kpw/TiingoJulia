# Database related functions
using DataFrames
using DBInterface
using DuckDB
using LibPQ
using Dates
using Logging
using Base: @kwdef

# Constants
module DBConstants
    const DEFAULT_DUCKDB_PATH = "tiingo_historical_data.duckdb"
    const DEFAULT_CSV_FILE = "supported_tickers.csv"
    const LOG_FILE = "stock.log"

    module Tables
        const US_TICKERS = "us_tickers"
        const US_TICKERS_FILTERED = "us_tickers_filtered"
        const HISTORICAL_DATA = "historical_data"
    end
end

# Custom error types
struct DatabaseConnectionError <: Exception
    msg::String
end

struct DatabaseQueryError <: Exception
    msg::String
    query::String
end

# Type aliases for clarity
const DuckDBConnection = DBInterface.Connection
const PostgreSQLConnection = LibPQ.Connection

# Set up logging to file
function setup_logging()
    logger = SimpleLogger(open(LOG_FILE, "a"))
    global_logger(logger)
end

"""
    verify_duckdb_integrity(path::String)

Verify if a DuckDB database is accessible and contains expected tables.
Returns (is_valid::Bool, error_message::Union{String,Nothing})
"""
function verify_duckdb_integrity(path::String)
    if !isfile(path)
        return false, "Database file does not exist"
    end

    try
        conn = DBInterface.connect(DuckDB.DB, path)
        try
            # Check if we can execute basic queries
            DBInterface.execute(conn, "SELECT 1")
            tables = DBInterface.execute(conn, """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name IN ('us_tickers', 'us_tickers_filtered', 'historical_data')
            """) |> DataFrame

            if nrow(tables) == 0
                return false, "No expected tables found in database"
            end

            return true, nothing
        finally
            DuckDB.close(conn)
        end
    catch e
        return false, "Database verification failed: $e"
    end
end

"""
    configure_database(conn::DuckDBConnection)

Configure database settings.
"""
function configure_database(conn::DuckDBConnection)
    DBInterface.execute(conn, "SET threads TO 4")
end

"""
    connect_duckdb(path::String = DBConstants.DEFAULT_DUCKDB_PATH)::DuckDBConnection

Connect to the DuckDB database and create necessary tables if they don't exist.
"""
function connect_duckdb(path::String = DBConstants.DEFAULT_DUCKDB_PATH)::DuckDBConnection
    try
        @info "Attempting to connect to DuckDB at path: $path"
        conn = DBInterface.connect(DuckDB.DB, path)
        configure_database(conn)
        create_tables(conn)
        return conn
    catch e
        @warn "Failed to connect to existing database: $e"

        @info "Attempting to create a new database at path: $path"
        try
            # Ensure directory exists
            mkpath(dirname(path))
            conn = DBInterface.connect(DuckDB.DB, path)
            configure_database(conn)
            create_tables(conn)
            return conn
        catch new_e
            @error "Failed to create new database" exception=(new_e, catch_backtrace())
            throw(DatabaseConnectionError("Failed to connect to or create DuckDB: $new_e"))
        end
    end
end


"""
    create_tables(conn::DuckDBConnection)

Create necessary tables in the DuckDB database if they don't exist.
"""
function create_tables(conn::DuckDBConnection)
    tables = [
        ("us_tickers", """
        CREATE TABLE IF NOT EXISTS us_tickers (
            ticker VARCHAR,
            exchange VARCHAR,
            assetType VARCHAR,
            priceCurrency VARCHAR,
            startDate DATE,
            endDate DATE
        )
        """),
        ("us_tickers_filtered", """
        CREATE TABLE IF NOT EXISTS us_tickers_filtered AS
        SELECT * FROM us_tickers
        WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
        AND assetType IN ('Stock', 'ETF')
        AND ticker NOT LIKE '%/%'
        """),
        ("historical_data", """
        CREATE TABLE IF NOT EXISTS historical_data (
            ticker VARCHAR,
            date DATE,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            open FLOAT,
            volume BIGINT,
            adjClose FLOAT,
            adjHigh FLOAT,
            adjLow FLOAT,
            adjOpen FLOAT,
            adjVolume BIGINT,
            divCash FLOAT,
            splitFactor FLOAT,
            UNIQUE (ticker, date)
        )
        """)
    ]

    for (table_name, query) in tables
        try
            DBInterface.execute(conn, query)
            @info "Created table if not exists: $table_name"
        catch e
            @error "Failed to create table: $table_name" exception=(e, catch_backtrace())
        end
    end
end


"""
    update_us_tickers(conn::DBConnection, csv_file::String = DBConstants.DEFAULT_CSV_FILE)

Update the us_tickers table in the database from a CSV file.
"""
function update_us_tickers(conn::DuckDBConnection, csv_file::String = DBConstants.DEFAULT_CSV_FILE)
    query = """
    CREATE OR REPLACE TABLE $(DBConstants.Tables.US_TICKERS) AS
    SELECT * FROM read_csv('$csv_file')
    """
    try
        DBInterface.execute(conn, query)
        @info "Updated us_tickers table from file: $csv_file"
    catch e
        @error "Failed to update us_tickers table" exception=(e, catch_backtrace())
        throw(DatabaseQueryError("Failed to update us_tickers: $e", query))
    end
end


"""
    update_historical(conn::DuckDBConnection, tickers::DataFrame, api_key::String = get_api_key(); add_missing::Bool = false)

Update historical data for multiple tickers.

Parameters:
- conn: DuckDB database connection
- tickers: DataFrame containing ticker information
- api_key: API key for fetching ticker data
- add_missing: If true, automatically add missing tickers to the historical_data table

Returns:
- A tuple containing two lists: (updated_tickers, missing_tickers)
"""
function update_historical(
    conn::DuckDBConnection,
    tickers::DataFrame,
    api_key::String = get_api_key();
    add_missing::Bool = true
)
    latest_dates_df = get_latest_dates(conn)
    end_date = DBInterface.execute(conn, """
    SELECT MAX(endDate) as end_date
    FROM us_tickers_filtered
    WHERE ticker = 'SPY'
    ORDER BY 1;
    """) |> DataFrame |> first |> first
    if isnothing(end_date)
        end_date = Date(now()) - Day(1)  # Default to yesterday if no data
    end

    updated_tickers = String[]
    missing_tickers = String[]
    error_tickers = String[]

    for (i, row) in enumerate(eachrow(tickers))
        symbol = row.ticker
        ticker_latest = filter(r -> r.ticker == symbol, latest_dates_df)

        if isempty(ticker_latest)
            handle_missing_ticker(conn, row, api_key, missing_tickers, updated_tickers)
        else
            latest_date = ticker_latest[1, :latest_date]
            if latest_date < end_date
                @info "$i : $symbol : $(latest_date + Day(1)) ~ $end_date"
                try
                    ticker_data = get_ticker_data(
                        row,
                        start_date = latest_date + Day(1),
                        end_date = end_date,
                        api_key = api_key
                    )
                    if !isempty(ticker_data)
                        upsert_stock_data(conn, ticker_data, symbol)
                        push!(updated_tickers, symbol)
                    end
                catch e
                    if isa(e, AssertionError) && occursin("No data returned", e.msg)
                        @info "$i : $symbol has no new data"
                    else
                        @warn "Failed to update $symbol: $e"
                        push!(error_tickers, symbol)
                    end
                end
            else
                @info "$i : $symbol is up to date"
            end
        end
    end

    log_update_results(missing_tickers, updated_tickers, error_tickers, add_missing)
    return (updated_tickers, missing_tickers)
end


# Helper functions for update_historical
function get_latest_dates(conn::DuckDBConnection)
    DBInterface.execute(conn, """
        SELECT ticker, MAX(date) as latest_date
        FROM historical_data
        GROUP BY ticker
    """) |> DataFrame
end


function update_existing_ticker(
    conn::DuckDBConnection,
    ticker_info::DataFrameRow,
    latest_date::Date,
    index::Int,
    updated_tickers::Vector{String},
    api_key::String
)
    symbol = ticker_info.ticker
    end_date = ticker_info.end_date

    if latest_date <= end_date
        @info "$index : $symbol : $latest_date ~ $end_date"
        try
            ticker_data = get_ticker_data(ticker_info; api_key=api_key)
            if !isempty(ticker_data)
                upsert_stock_data(conn, ticker_data, symbol)
                push!(updated_tickers, symbol)
            else
                @warn "No data retrieved for $symbol"
            end
        catch e
            @warn "Failed to update $symbol: $e"
        end
    else
        @info "$index : $symbol has the latest data"
    end
end

function get_latest_date(conn::DuckDBConnection, symbol::String)::DataFrame
    DBInterface.execute(conn, """
    SELECT ticker, max(date) + 1 AS latest_date
    FROM historical_data
    WHERE ticker = ?
    GROUP BY 1
    ORDER BY 1;
    """, [symbol]) |> DataFrame
end

function handle_missing_ticker(
    conn::DuckDBConnection,
    ticker_info::DataFrameRow,
    api_key::String,
    missing_tickers::Vector{String},
    updated_tickers::Vector{String}
)
    symbol = ticker_info.ticker
    push!(missing_tickers, symbol)
    @info "Adding missing ticker: $symbol"
    try
        ticker_data = get_ticker_data(ticker_info; api_key=api_key)
        if !isempty(ticker_data)
            upsert_stock_data(conn, ticker_data, symbol)
            push!(updated_tickers, symbol)
        else
            @warn "No data retrieved for $symbol"
        end
    catch e
        @warn "Failed to add historical data for $symbol: $e"
    end
end

"""
    add_historical_data(conn::DuckDBConnection, ticker::String, api_key::String = get_api_key())

Add historical data for a single ticker.
"""
function add_historical_data(
    conn::DuckDBConnection,
    ticker::String,
    api_key::String = get_api_key()
)
    data = get_ticker_data(ticker, api_key=api_key)
    if isempty(data)
        @warn "No data retrieved for $ticker"
        return
    end
    upsert_stock_data(conn, data, ticker)
    @info "Added historical data for $ticker"
end

"""
    upsert_stock_data(conn::DuckDBConnection, data::DataFrame, ticker::String)

Upsert stock data into the historical_data table.
"""
function upsert_stock_data(
    conn::DuckDBConnection,
    data::DataFrame,
    ticker::String
)
    upsert_stmt = """
    INSERT INTO historical_data (ticker, date, close, high, low, open, volume, adjClose, adjHigh, adjLow, adjOpen, adjVolume, divCash, splitFactor)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (ticker, date) DO UPDATE SET
        close = EXCLUDED.close,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        open = EXCLUDED.open,
        volume = EXCLUDED.volume,
        adjClose = EXCLUDED.adjClose,
        adjHigh = EXCLUDED.adjHigh,
        adjLow = EXCLUDED.adjLow,
        adjOpen = EXCLUDED.adjOpen,
        adjVolume = EXCLUDED.adjVolume,
        divCash = EXCLUDED.divCash,
        splitFactor = EXCLUDED.splitFactor
    """
    rows_updated = 0
    DBInterface.execute(conn, "BEGIN TRANSACTION")
    try
        for row in eachrow(data)
            values = (
                ticker,
                row.date,
                coalesce(row.close, NaN),
                coalesce(row.high, NaN),
                coalesce(row.low, NaN),
                coalesce(row.open, NaN),
                coalesce(row.volume, 0),
                coalesce(row.adjClose, NaN),
                coalesce(row.adjHigh, NaN),
                coalesce(row.adjLow, NaN),
                coalesce(row.adjOpen, NaN),
                coalesce(row.adjVolume, 0),
                coalesce(row.divCash, 0.0),
                coalesce(row.splitFactor, 1.0)
            )
            DBInterface.execute(conn, upsert_stmt, values)
            rows_updated += 1
        end
        DBInterface.execute(conn, "COMMIT")
    catch e
        DBInterface.execute(conn, "ROLLBACK")
        @error "Error upserting stock data for $ticker" exception=(e, catch_backtrace())
        rethrow(e)
    end

    return rows_updated
end

"""
    upsert_stock_data_bulk(conn::DuckDBConnection, data::DataFrame, ticker::String)

Bulk upsert stock data into the historical_data table using prepared statements for better performance.
"""
function upsert_stock_data_bulk(
    conn::DuckDBConnection,
    data::DataFrame,
    ticker::String
)
    if nrow(data) == 0
        return 0
    end

    # Filter data for the specific ticker if needed
    ticker_data = filter(row -> row.ticker == ticker, data)
    if nrow(ticker_data) == 0
        return 0
    end

    upsert_stmt = """
    INSERT INTO historical_data (ticker, date, close, high, low, open, volume, adjClose, adjHigh, adjLow, adjOpen, adjVolume, divCash, splitFactor)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (ticker, date) DO UPDATE SET
        close = EXCLUDED.close,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        open = EXCLUDED.open,
        volume = EXCLUDED.volume,
        adjClose = EXCLUDED.adjClose,
        adjHigh = EXCLUDED.adjHigh,
        adjLow = EXCLUDED.adjLow,
        adjOpen = EXCLUDED.adjOpen,
        adjVolume = EXCLUDED.adjVolume,
        divCash = EXCLUDED.divCash,
        splitFactor = EXCLUDED.splitFactor
    """

    rows_updated = 0
    DBInterface.execute(conn, "BEGIN TRANSACTION")
    try
        for row in eachrow(ticker_data)
            values = (
                ticker,
                row.date,
                coalesce(row.close, NaN),
                coalesce(row.high, NaN),
                coalesce(row.low, NaN),
                coalesce(row.open, NaN),
                coalesce(row.volume, 0),
                coalesce(row.adjClose, NaN),
                coalesce(row.adjHigh, NaN),
                coalesce(row.adjLow, NaN),
                coalesce(row.adjOpen, NaN),
                coalesce(row.adjVolume, 0),
                coalesce(row.divCash, 0.0),
                coalesce(row.splitFactor, 1.0)
            )
            DBInterface.execute(conn, upsert_stmt, values)
            rows_updated += 1
        end
        DBInterface.execute(conn, "COMMIT")
    catch e
        DBInterface.execute(conn, "ROLLBACK")
        @error "Error bulk upserting stock data for $ticker" exception=(e, catch_backtrace())
        rethrow(e)
    end

    return rows_updated
end

function log_update_results(missing_tickers::Vector{String}, updated_tickers::Vector{String}, error_tickers::Vector{String}, add_missing::Bool)
    if !isempty(missing_tickers)
        if add_missing
            @info "Attempted to add $(length(missing_tickers)) missing tickers to historical_data"
        else
            @warn "The following tickers are not in historical_data: $missing_tickers"
        end
    end

    if !isempty(error_tickers)
        @warn "The following tickers encountered errors during processing: $error_tickers"
    end

    @info "Historical data update completed" updated_count=length(updated_tickers) missing_count=length(missing_tickers) error_count=length(error_tickers)
end

"""
    update_splitted_ticker(conn::DuckDBConnection, tickers::DataFrame, api_key::String = get_api_key())

Update data for tickers that have undergone a split.
"""
function update_split_ticker(
    conn::DuckDBConnection,
    tickers::DataFrame, # all tickers is best
    api_key::String = get_api_key()
)
    end_date = maximum(skipmissing(tickers.end_date))

    split_tickers = DBInterface.execute(conn, """
    SELECT ticker, splitFactor, date
      FROM historical_data
     WHERE date = '$end_date'
       AND splitFactor <> 1.0
    """) |> DataFrame

    for (i, row) in enumerate(eachrow(split_tickers))
        symbol = row.ticker
        if ismissing(symbol) || symbol === nothing
            continue  # Skip this row if ticker is missing or null
        end
        ticker_info = tickers[tickers.ticker .== symbol, :]
        if isempty(ticker_info)
            @warn "No ticker info found for $symbol"
            continue
        end
        start_date = ticker_info[1, :start_date]
        @info "$i: Updating split ticker $symbol from $start_date to $end_date"
        ticker_data = get_ticker_data(ticker_info[1, :]; api_key=api_key)
        upsert_stock_data(conn, ticker_data, symbol)
    end
    @info "Updated split tickers"
end

"""
    get_tickers_all(conn::DBInterface.Connection)

Get all tickers from the us_tickers_filtered table.
"""
function get_tickers_all(conn::DBInterface.Connection)::DataFrame
    query = """
    SELECT ticker, exchange, assettype as asset_type, startdate as start_date, enddate as end_date
    FROM us_tickers_filtered
    ORDER BY ticker;
    """
    df = DBInterface.execute(conn, query) |> DataFrame
    return df
end

"""
    get_tickers_etf(conn::DBInterface.Connection)

Get all ETF tickers from the us_tickers_filtered table.
"""
function get_tickers_etf(conn::DBInterface.Connection)::DataFrame
    DBInterface.execute(conn, """
    SELECT ticker, exchange, assettype as asset_type, startdate as start_date, enddate as end_date
    FROM us_tickers_filtered
    WHERE assetType = 'ETF'
    ORDER BY ticker;
    """) |> DataFrame
end

"""
    get_tickers_stock(conn::DBInterface.Connection)

Get all stock tickers from the us_tickers_filtered table.
"""
function get_tickers_stock(conn::DBInterface.Connection)::DataFrame
    DBInterface.execute(conn, """
    SELECT ticker, exchange, assettype as asset_type, startdate as start_date, enddate as end_date
    FROM us_tickers_filtered
    WHERE assetType = 'Stock'
    ORDER BY ticker;
    """) |> DataFrame
end

"""
    connect_postgres(connection_string::String)

Connect to the PostgreSQL database.
"""
function connect_postgres(connection_string::String)::PostgreSQLConnection
    try
        return LibPQ.Connection(connection_string)
    catch e
        @error "Failed to connect to PostgreSQL" exception=(e, catch_backtrace())
        rethrow(e)
    end
end

"""
    close_postgres(conn::PostgreSQLConnection)

Close the PostgreSQL database connection.
"""
close_postgres(conn::PostgreSQLConnection) = LibPQ.close(conn)

"""
    export_to_postgres(duckdb_conn::DuckDBConnection, pg_conn::PostgreSQLConnection, tables::Vector{String}; pg_host::String="127.0.0.1", pg_user::String="otwn", pg_dbname::String="tiingo")

Export tables from DuckDB to PostgreSQL.
"""
function export_to_postgres(
    duckdb_conn::DuckDBConnection,
    pg_conn::PostgreSQLConnection,
    tables::Vector{String};
    parquet_file::String="historical_data.parquet",
    pg_host::String="127.0.0.1",
    pg_user::String="otwn",
    pg_dbname::String="tiingo",
    max_retries::Int=3,
    retry_delay::Int=5,
    use_dataframe::Union{Bool, Nothing}=nothing,
    max_rows_for_dataframe::Int = 1_000_000
)
    for table_name in tables
        retry_with_exponential_backoff(max_retries, retry_delay) do
            export_table_to_postgres(
                duckdb_conn, pg_conn, table_name, parquet_file, pg_host, pg_user, pg_dbname,
                use_dataframe=use_dataframe, max_rows_for_dataframe=max_rows_for_dataframe
            )
            @info "Successfully exported $table_name from DuckDB to PostgreSQL"
        end
    end
end

# New helper function for retrying with exponential backoff
function retry_with_exponential_backoff(f::Function, max_retries::Int, initial_delay::Int)
    for attempt in 1:max_retries
        try
            return f()
        catch e
            if attempt == max_retries
                @error "Failed after $max_retries attempts" exception=(e, catch_backtrace())
                rethrow(e)
            end
            delay = initial_delay * 2^(attempt - 1)
            @warn "Attempt $attempt failed. Retrying in $delay seconds..." exception=(e, catch_backtrace())
            sleep(delay)
        end
    end
end

"""
    export_table_to_postgres(duckdb_conn::DuckDBConnection, pg_conn::PostgreSQLConnection, table_name::String, pg_host::String, pg_user::String, pg_dbname::String)

Export a single table from DuckDB to PostgreSQL.
"""
function export_table_to_postgres(
    duckdb_conn::DuckDBConnection,
    pg_conn::PostgreSQLConnection,
    table_name::String,
    parquet_file::String,
    pg_host::String,
    pg_user::String,
    pg_dbname::String;
    use_dataframe::Union{Bool, Nothing}=nothing,
    max_rows_for_dataframe::Int = 1_000_000
)
    @info "Exporting table $table_name to PostgreSQL"

    # Check if the table exists in DuckDB
    table_exists = DBInterface.execute(
        duckdb_conn,
        """SELECT name FROM sqlite_master WHERE type='table' AND name='$table_name';""",
    ) |> DataFrame

    if isempty(table_exists)
        error("Table $table_name does not exist in DuckDB")
    end

    # Get row count
    row_count = DBInterface.execute(duckdb_conn, "SELECT COUNT(*) FROM $table_name") |> DataFrame
    row_count = row_count[1, 1]

    # Determine whether to use DataFrame or Parquet
    use_df = if isnothing(use_dataframe)
        row_count <= max_rows_for_dataframe
    else
        use_dataframe
    end

    if use_df
        export_table_to_postgres_dataframe(duckdb_conn, pg_conn, table_name, pg_host, pg_user, pg_dbname)
    else
        export_table_to_postgres_parquet(duckdb_conn, pg_conn, table_name, parquet_file, pg_host, pg_user, pg_dbname)
    end
end

function export_table_to_postgres_dataframe(
    duckdb_conn::DuckDBConnection,
    pg_conn::PostgreSQLConnection,
    table_name::String,
    pg_host::String,
    pg_user::String,
    pg_dbname::String
)
    @info "Exporting table $table_name to PostgreSQL using DataFrames"

    try
        # Read the entire table into a DataFrame
        df = DBInterface.execute(duckdb_conn, "SELECT * FROM $table_name") |> DataFrame
        @info "Loaded $table_name into DataFrame with $(nrow(df)) rows"

        # Get the schema and create table
        schema = DBInterface.execute(duckdb_conn, "DESCRIBE $table_name") |> DataFrame
        create_table_query = generate_create_table_query(table_name, schema)
        create_or_replace_table(pg_conn, table_name, create_table_query)

        # Insert data into PostgreSQL
        columns = join(lowercase.(names(df)), ", ")
        placeholders = join(["\$" * string(i) for i in 1:ncol(df)], ", ")
        insert_query = "INSERT INTO $table_name ($columns) VALUES ($placeholders)"

        LibPQ.load!(
            (col => df[!, col] for col in names(df)),
            pg_conn,
            insert_query
        )

        @info "Inserted $(nrow(df)) rows into PostgreSQL table $table_name"

    catch e
        @error "Error exporting table $table_name using DataFrames" exception=(e, catch_backtrace())
        rethrow(e)
    end
end

function export_table_to_postgres_parquet(
    duckdb_conn::DuckDBConnection,
    pg_conn::PostgreSQLConnection,
    table_name::String,
    parquet_file::String,
    pg_host::String,
    pg_user::String,
    pg_dbname::String
)
    @info "Exporting table $table_name to PostgreSQL using Parquet"

    try
        # Export to parquet
        DBInterface.execute(duckdb_conn, """COPY $table_name TO '$parquet_file';""")
        @info "Exported $table_name to parquet file"

        # Get the schema and create table
        table_name_lower = lowercase(table_name)
        schema = DBInterface.execute(duckdb_conn, "DESCRIBE $table_name_lower") |> DataFrame
        create_table_query = generate_create_table_query(table_name_lower, schema)
        create_or_replace_table(pg_conn, table_name_lower, create_table_query)

        # Copy data from parquet to PostgreSQL
        setup_postgres_connection(duckdb_conn, pg_host, pg_user, pg_dbname)
        DBInterface.execute(
            duckdb_conn,
            """COPY postgres_db.$table_name FROM '$parquet_file';"""
        )
        DBInterface.execute(duckdb_conn, "DETACH postgres_db;")
        @info "Copied data from parquet file to PostgreSQL table $table_name"
    catch e
        @error "Error exporting table $table_name using Parquet" exception=(e, catch_backtrace())
        rethrow(e)
    finally
        # if isfile(parquet_file)
        #     rm(parquet_file)
        #     @info "Removed temporary parquet file for $table_name"
        # end
        try
            DBInterface.execute(duckdb_conn, "DETACH postgres_db;")
        catch
        end
    end
end

function create_or_replace_table(pg_conn::PostgreSQLConnection, table_name::String, create_table_query::String)
    # Check if the table exists in PostgreSQL
    table_exists_pg = LibPQ.execute(
        pg_conn,
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = '$table_name';
        """
    ) |> DataFrame

    if isempty(table_exists_pg)
        # If the table doesn't exist, create it
        LibPQ.execute(pg_conn, create_table_query)
        @info "Created table $table_name in PostgreSQL"
    else
        # If the table exists, rename it as a backup
        LibPQ.execute(pg_conn, "DROP TABLE IF EXISTS $(table_name)_backup;")
        LibPQ.execute(pg_conn, "CREATE TABLE $(table_name)_backup AS TABLE $table_name;")
        LibPQ.execute(pg_conn, "DROP TABLE IF EXISTS $table_name;")
        LibPQ.execute(pg_conn, create_table_query)
        @info "Created new table $table_name in PostgreSQL, old table is stored as $(table_name)_backup"
    end
end


"""
    generate_create_table_query(table_name::String, schema::DataFrame)

Generate a CREATE TABLE query for PostgreSQL based on the DuckDB schema.
Converts all column names to lowercase to avoid case-sensitivity issues.
"""
function generate_create_table_query(table_name::String, schema::DataFrame)
    query = "CREATE TABLE IF NOT EXISTS $(lowercase(table_name)) ("
    columns = []
    for row in eachrow(schema)
        column_name = lowercase(row.column_name)
        data_type = row.column_type
        pg_type = map_duckdb_to_postgres_type(data_type)
        push!(columns, "$(column_name) $(pg_type)")
    end
    query *= join(columns, ", ")

    if lowercase(table_name) == "historical_data"
        query *= ", UNIQUE (ticker, date)"
    end

    query *= ")"
    return query
end


"""
    map_duckdb_to_postgres_type(duckdb_type::String)

Map DuckDB data types to PostgreSQL data types.
"""
function map_duckdb_to_postgres_type(duckdb_type::String)
    type_mapping = Dict(
        "VARCHAR" => "VARCHAR",
        "INTEGER" => "INTEGER",
        "BIGINT" => "BIGINT",
        "DOUBLE" => "DOUBLE PRECISION",
        "BOOLEAN" => "BOOLEAN",
        "DATE" => "DATE",
        "TIMESTAMP" => "TIMESTAMP"
    )

    for (key, value) in type_mapping
        if occursin(key, uppercase(duckdb_type))
            return value
        end
    end

    return duckdb_type  # Use the same type if no specific mapping
end

"""
    setup_postgres_connection(duckdb_conn::DuckDBConnection, pg_host::String, pg_user::String, pg_dbname::String)

Set up a PostgreSQL connection in DuckDB.
"""
function setup_postgres_connection(
    duckdb_conn::DuckDBConnection,
    pg_host::String,
    pg_user::String,
    pg_dbname::String
)
    try
        DBInterface.execute(duckdb_conn, "INSTALL postgres;")
        DBInterface.execute(duckdb_conn, "LOAD postgres;")
        DBInterface.execute(duckdb_conn, """
            ATTACH 'dbname=$pg_dbname user=$pg_user host=$pg_host' AS postgres_db (TYPE postgres);
        """)
        @info "Successfully set up PostgreSQL connection in DuckDB"
    catch e
        @error "Failed to set up PostgreSQL connection in DuckDB" exception=(e, catch_backtrace())
        rethrow(e)
    end
end

"""
    close_duckdb(conn::DuckDBConnection)

Safely close a DuckDB database connection.
"""
function close_duckdb(conn::DuckDBConnection)
    try
        DBInterface.close!(conn)
        @info "DuckDB connection closed successfully"
    catch e
        @warn "Error closing DuckDB connection" exception=e
    end
end

"""
    optimize_database(conn::DuckDBConnection)

Optimize the database by running VACUUM and ANALYZE commands for better performance.
"""
function optimize_database(conn::DuckDBConnection)
    try
        @info "Optimizing database..."
        DBInterface.execute(conn, "VACUUM")
        DBInterface.execute(conn, "ANALYZE")
        @info "Database optimization completed"
    catch e
        @warn "Database optimization failed" exception=e
        rethrow(e)
    end
end
