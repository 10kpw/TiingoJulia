# Database related functions
using DataFrames
using DuckDB
using LibPQ
using Dates
using Logging

# Constants
const DEFAULT_DUCKDB_PATH = "tiingo_historical_data.duckdb"
const DEFAULT_CSV_FILE = "supported_tickers.csv"
const LOG_FILE = "stock.log"

# Type aliases for clarity
const DuckDBConnection = DBInterface.Connection
const PostgreSQLConnection = LibPQ.Connection

# Set up logging to file
function setup_logging()
    logger = SimpleLogger(open(LOG_FILE, "a"))
    global_logger(logger)
end

"""
    connect_db(path::String = DEFAULT_DUCKDB_PATH)

Connect to the DuckDB database and create necessary tables if they don't exist.
"""
function connect_db(path::String = DEFAULT_DUCKDB_PATH)::DuckDBConnection
    conn = DBInterface.connect(DuckDB.DB, path)
    create_tables(conn)
    return conn
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
        DBInterface.execute(conn, query)
    end
end

"""
    close_db(conn::DuckDBConnection)

Close the DuckDB database connection.
"""
close_db(conn::DuckDBConnection) = DBInterface.close(conn)

"""
    update_us_tickers(conn::DuckDBConnection, csv_file::String = DEFAULT_CSV_FILE)

Update the us_tickers table in the database from a CSV file.
"""
function update_us_tickers(conn::DuckDBConnection, csv_file::String = DEFAULT_CSV_FILE)
    DBInterface.execute(conn, """
    CREATE OR REPLACE TABLE us_tickers AS
    SELECT * FROM read_csv('$csv_file')
    """)
    @info "Updated us_tickers table from file: $csv_file"
end

"""
    upsert_stock_data(conn::DBInterface.Connection, data::DataFrame, ticker::String)

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
    for row in eachrow(data)
        try
            DBInterface.execute(conn, upsert_stmt, (ticker, row.date, row.close, row.high, row.low, row.open, row.volume, row.adjClose, row.adjHigh, row.adjLow, row.adjOpen, row.adjVolume, row.divCash, row.splitFactor))
            rows_updated += 1
        catch e
            @error "Error upserting stock data for $ticker: $e"
        end
    end
    # @info "Upserted stock data for $ticker" rows_updated=rows_updated total_rows=nrow(data)
end

"""
    add_historical_data(conn::DuckDBConnection, ticker::String, api_key::String = get_api_key())

Add historical data for a single ticker.
"""
function add_historical_data(conn::DuckDBConnection, ticker::String, api_key::String = get_api_key())
    data = fetch_ticker_data(ticker, api_key=api_key)
    upsert_stock_data(conn, data, ticker)
    @info "Added historical data for $ticker"
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
    api_key::String = get_api_key(),
    add_missing::Bool = true
)
    end_date = maximum(skipmissing(tickers.endDate))
    missing_tickers = String[]
    updated_tickers = String[]

    for (i, row) in enumerate(eachrow(tickers))
        try
            symbol = row.ticker
            hist_data = DBInterface.execute(conn, """
            SELECT ticker, max(date) + INTERVAL '1 day' AS latest_date
            FROM historical_data
            WHERE ticker = '$symbol'
            GROUP BY 1
            ORDER BY 1;
            """) |> DataFrame

            if isempty(hist_data.latest_date)
                push!(missing_tickers, symbol)
                if add_missing
                    @info "Adding missing ticker: $symbol"
                    add_historical_data(conn, symbol, api_key)
                    push!(updated_tickers, symbol)
                else
                    @info "Skipping missing ticker: $symbol"
                end
                continue
            end

            start_date = Dates.format(hist_data.latest_date[1], "yyyy-mm-dd")

            if Date(start_date) <= end_date
                println("$i : $symbol : $start_date ~ $end_date")
                ticker_data = fetch_ticker_data(symbol; start_date=start_date, end_date=end_date, api_key=api_key)
                upsert_stock_data(conn, ticker_data, symbol)
                push!(updated_tickers, symbol)
            else
                println("$i : $symbol has the latest data")
            end
        catch e
            @error "Error processing $(row.ticker): $e"
        end
    end

    if !isempty(missing_tickers)
        if add_missing
            @info "Added $(length(missing_tickers)) missing tickers to historical_data"
        else
            @warn "The following tickers are not in historical_data: $missing_tickers"
        end
    end

    @info "Historical data update completed" updated_count=length(updated_tickers) missing_count=length(missing_tickers)

    return (updated_tickers, missing_tickers)
end

"""
    update_splitted_ticker(conn::DuckDBConnection, tickers::DataFrame, api_key::String = get_api_key())

Update data for tickers that have undergone a split.
"""
function update_splitted_ticker(
    conn::DuckDBConnection,
    tickers::DataFrame, # all tickers is best
    api_key::String = get_api_key()
)
    end_date = maximum(skipmissing(tickers.endDate))
    print(first(tickers))
    splitted_tickers = DBInterface.execute(conn, """
    SELECT ticker, splitFactor, date
      FROM historical_data
     WHERE date = '$end_date'
       AND splitFactor <> 1.0
    """) |> DataFrame

    print(first(splitted_tickers))
    for (i, row) in enumerate(eachrow(splitted_tickers))
        symbol = row.ticker
        if ismissing(symbol) || symbol === nothing
            continue  # Skip this row if ticker is missing or null
        end
        start_date = tickers[tickers.ticker .== symbol, :startDate][1]
        @info "$i: Updating split ticker $symbol from $start_date to $end_date"
        ticker_data = fetch_ticker_data(symbol; start_date=start_date, end_date=end_date, api_key=api_key)
        upsert_stock_data(conn, ticker_data, symbol)
    end
    @info "Updated split tickers"
end

"""
    get_tickers_all(conn::DBInterface.Connection)

Get all tickers from the us_tickers_filtered table.
"""
function get_tickers_all(conn::DBInterface.Connection)::DataFrame
    DBInterface.execute(conn, """
    SELECT ticker, exchange, assetType, startDate, endDate
    FROM us_tickers_filtered
    ORDER BY ticker;
    """) |> DataFrame
end

"""
    get_tickers_etf(conn::DBInterface.Connection)

Get all ETF tickers from the us_tickers_filtered table.
"""
function get_tickers_etf(conn::DBInterface.Connection)::DataFrame
    DBInterface.execute(conn, """
    SELECT ticker, exchange, assetType, startDate, endDate
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
    SELECT ticker, exchange, assetType, startDate, endDate
    FROM us_tickers_filtered
    WHERE assetType = 'Stock'
    ORDER BY ticker;
    """) |> DataFrame
end

"""
    connect_postgres(connection_string::String)

Connect to the PostgreSQL database.
"""
connect_postgres(connection_string::String)::PostgreSQLConnection = LibPQ.Connection(connection_string)

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
    pg_host::String="127.0.0.1",
    pg_user::String="otwn",
    pg_dbname::String="tiingo",
    max_retries::Int=3,
    retry_delay::Int=5
)
    for table_name in tables
        retries = 0
        while retries < max_retries
            try
                export_table_to_postgres(duckdb_conn, pg_conn, table_name, pg_host, pg_user, pg_dbname)
                @info "Successfully exported $table_name from DuckDB to PostgreSQL"
                break  # Exit the retry loop if successful
            catch e
                retries += 1
                @warn "Error exporting $table_name (Attempt $retries of $max_retries)" exception=(e, catch_backtrace())

                if retries < max_retries
                    @info "Retrying in $retry_delay seconds..."
                    sleep(retry_delay)
                else
                    @error "Failed to export $table_name after $max_retries attempts"
                    rethrow(e)
                end
            finally
                # Clean up any temporary files that might have been created
                parquet_file = "$(table_name).parquet"
                if isfile(parquet_file)
                    rm(parquet_file, force=true)
                    @info "Removed temporary parquet file for $table_name"
                end
            end
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
    pg_host::String,
    pg_user::String,
    pg_dbname::String
)
    @info "Exporting table $table_name to PostgreSQL"
    parquet_file = "$(table_name).parquet"

    try
        DBInterface.execute(duckdb_conn, """COPY $table_name TO '$parquet_file';""")
        @info "Exported $table_name to parquet file"

        schema = DBInterface.execute(duckdb_conn, "DESCRIBE $table_name") |> DataFrame
        create_table_query = generate_create_table_query(table_name, schema)

        LibPQ.execute(pg_conn, "DROP TABLE IF EXISTS $(table_name)_backup;")
        LibPQ.execute(pg_conn, "CREATE TABLE $(table_name)_backup AS TABLE $table_name;")
        LibPQ.execute(pg_conn, "DROP TABLE IF EXISTS $table_name;")
        LibPQ.execute(pg_conn, create_table_query)
        @info "Created table $table_name in PostgreSQL"

        setup_postgres_connection(duckdb_conn, pg_host, pg_user, pg_dbname)
        DBInterface.execute(duckdb_conn, """
            COPY postgres_db.$table_name FROM '$parquet_file';
        """)
        DBInterface.execute(duckdb_conn, "DETACH postgres_db;")
        @info "Copied data from parquet file to PostgreSQL table $table_name"
    finally
        # Clean up the parquet file, even if an error occurred
        if isfile(parquet_file)
            rm(parquet_file)
            @info "Removed temporary parquet file for $table_name"
        end
    end
end

"""
    generate_create_table_query(table_name::String, schema::DataFrame)

Generate a CREATE TABLE query for PostgreSQL based on the DuckDB schema.
"""
function generate_create_table_query(table_name::String, schema::DataFrame)
    query = "CREATE TABLE IF NOT EXISTS $table_name ("
    for row in eachrow(schema)
        column_name = row.column_name
        data_type = row.column_type
        pg_type = map_duckdb_to_postgres_type(data_type)
        query *= "\"$column_name\" $pg_type, "
    end
    query = chop(query, tail=2)

    if table_name == "historical_data"
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
function setup_postgres_connection(duckdb_conn::DuckDBConnection, pg_host::String, pg_user::String, pg_dbname::String)
    DBInterface.execute(duckdb_conn, "INSTALL postgres;")
    DBInterface.execute(duckdb_conn, "LOAD postgres;")
    DBInterface.execute(duckdb_conn, """
        ATTACH 'dbname=$pg_dbname user=$pg_user host=$pg_host' AS postgres_db (TYPE postgres);
    """)
end

