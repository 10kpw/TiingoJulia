# Database related functions
using DataFrames
using DBInterface
using DuckDB
using LibPQ
using Dates
using Logging
using Base: @kwdef
using Base.Threads

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
            tables =
                DBInterface.execute(
                    conn,
                    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name IN ('us_tickers', 'us_tickers_filtered', 'historical_data')
""",
                ) |> DataFrame

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
            @error "Failed to create new database" exception = (new_e, catch_backtrace())
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
        (
            "us_tickers",
            """
CREATE TABLE IF NOT EXISTS us_tickers (
    ticker VARCHAR,
    exchange VARCHAR,
    assetType VARCHAR,
    priceCurrency VARCHAR,
    startDate DATE,
    endDate DATE
)
""",
        ),
        (
            "us_tickers_filtered",
            """
CREATE TABLE IF NOT EXISTS us_tickers_filtered AS
SELECT * FROM us_tickers
WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
AND assetType IN ('Stock', 'ETF')
AND ticker NOT LIKE '%/%'
""",
        ),
        (
            "historical_data",
            """
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
""",
        ),
    ]

    for (table_name, query) in tables
        try
            DBInterface.execute(conn, query)
            @info "Created table if not exists: $table_name"
        catch e
            @error "Failed to create table: $table_name" exception = (e, catch_backtrace())
        end
    end
end


"""
    update_us_tickers(conn::DBConnection, csv_file::String = DBConstants.DEFAULT_CSV_FILE)

Update the us_tickers table in the database from a CSV file.
"""
function update_us_tickers(
    conn::DuckDBConnection,
    csv_file::String = DBConstants.DEFAULT_CSV_FILE,
)
    query = """
    CREATE OR REPLACE TABLE $(DBConstants.Tables.US_TICKERS) AS
    SELECT * FROM read_csv('$csv_file')
    """
    try
        DBInterface.execute(conn, query)
        @info "Updated us_tickers table from file: $csv_file"
    catch e
        @error "Failed to update us_tickers table" exception = (e, catch_backtrace())
        throw(DatabaseQueryError("Failed to update us_tickers: $e", query))
    end
end


"""
    update_historical(conn::DuckDBConnection, tickers::DataFrame, api_key::String = get_api_key();
                     add_missing::Bool = true, use_parallel::Bool = true, batch_size::Int = 50, max_concurrent::Int = 10)

Update historical data for multiple tickers. This is a wrapper that can use either the original sequential
method or the new parallel method for better performance.

Parameters:
- conn: DuckDB database connection
- tickers: DataFrame containing ticker information
- api_key: API key for fetching ticker data
- add_missing: If true, automatically add missing tickers to the historical_data table
- use_parallel: If true, use the optimized parallel version (recommended)
- batch_size: Number of tickers to process in each batch (parallel mode only)
- max_concurrent: Maximum number of concurrent API calls (parallel mode only)

Returns:
- A tuple containing two lists: (updated_tickers, missing_tickers)
"""
function update_historical(
    conn::DuckDBConnection,
    tickers::DataFrame,
    api_key::String = get_api_key();
    add_missing::Bool = true,
    use_parallel::Bool = true,
    batch_size::Int = 50,
    max_concurrent::Int = 10,
)
    if use_parallel
        return update_historical_parallel(conn, tickers, api_key;
                                        batch_size=batch_size, max_concurrent=max_concurrent, add_missing=add_missing)
    else
        return update_historical_sequential(conn, tickers, api_key; add_missing=add_missing)
    end
end

"""
    update_historical_sequential(conn::DuckDBConnection, tickers::DataFrame, api_key::String = get_api_key(); add_missing::Bool = true)

Original sequential version of update_historical for backward compatibility.
"""
function update_historical_sequential(
    conn::DuckDBConnection,
    tickers::DataFrame,
    api_key::String = get_api_key();
    add_missing::Bool = true,
)
    latest_dates_df = get_latest_dates(conn)
    latest_market_date = DBInterface.execute(
        conn,
        """
SELECT MAX(endDate)
FROM us_tickers_filtered
WHERE ticker = 'SPY'
ORDER BY 1;
""",
    ) |> DataFrame |> first |> first
    if isnothing(latest_market_date)
        latest_market_date = Date(now()) - Day(1)  # Default to yesterday if no data
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
            if latest_date < latest_market_date
                @info "$i : $symbol : $(latest_date + Day(1)) ~ $latest_market_date"
                try
                    ticker_data = get_ticker_data(
                        row,
                        start_date = latest_date + Day(1),
                        end_date = latest_market_date,
                        api_key = api_key,
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

# Helper functions for update_historical_sequential
function get_latest_dates(conn::DuckDBConnection)
    DBInterface.execute(
        conn,
        """
    SELECT ticker, MAX(date) as latest_date
    FROM historical_data
    GROUP BY ticker
""",
    ) |> DataFrame
end

function update_existing_ticker(
    conn::DuckDBConnection,
    ticker_info::DataFrameRow,
    latest_date::Date,
    index::Int,
    updated_tickers::Vector{String},
    api_key::String,
)
    symbol = ticker_info.ticker
    end_date = ticker_info.end_date

    if latest_date <= end_date
        @info "$index : $symbol : $latest_date ~ $end_date"
        try
            ticker_data = get_ticker_data(ticker_info; api_key = api_key)
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
    DBInterface.execute(
        conn,
        """
SELECT ticker, max(date) + 1 AS latest_date
FROM historical_data
WHERE ticker = ?
GROUP BY 1
ORDER BY 1;
""",
        [symbol],
    ) |> DataFrame
end

function handle_missing_ticker(
    conn::DuckDBConnection,
    ticker_info::DataFrameRow,
    api_key::String,
    missing_tickers::Vector{String},
    updated_tickers::Vector{String},
)
    symbol = ticker_info.ticker
    push!(missing_tickers, symbol)
    @info "Adding missing ticker: $symbol"
    try
        ticker_data = get_ticker_data(ticker_info; api_key = api_key)
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
    api_key::String = get_api_key(),
)
    data = get_ticker_data(ticker, api_key = api_key)
    if isempty(data)
        @warn "No data retrieved for $ticker"
        return
    end
    upsert_stock_data(conn, data, ticker)
    @info "Added historical data for $ticker"
end

"""
    upsert_stock_data(conn::DuckDBConnection, data::DataFrame, ticker::String)

Original upsert function for backward compatibility. For better performance, use upsert_stock_data_bulk.
"""
function upsert_stock_data(conn::DuckDBConnection, data::DataFrame, ticker::String)
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
                coalesce(row.splitFactor, 1.0),
            )
            DBInterface.execute(conn, upsert_stmt, values)
            rows_updated += 1
        end
        DBInterface.execute(conn, "COMMIT")
    catch e
        DBInterface.execute(conn, "ROLLBACK")
        @error "Error upserting stock data for $ticker" exception = (e, catch_backtrace())
        rethrow(e)
    end

    return rows_updated
end

"""
    update_historical_parallel(conn::DuckDBConnection, tickers::DataFrame, api_key::String = get_api_key();
                              batch_size::Int = 50, max_concurrent::Int = 10, add_missing::Bool = true)

High-performance version of update_historical using parallel API calls and batch database operations.

Parameters:
- conn: DuckDB database connection
- tickers: DataFrame containing ticker information
- api_key: API key for fetching ticker data
- batch_size: Number of tickers to process in each batch
- max_concurrent: Maximum number of concurrent API calls
- add_missing: If true, automatically add missing tickers to the historical_data table

Returns:
- A tuple containing two lists: (updated_tickers, missing_tickers)
"""
function update_historical_parallel(
    conn::DuckDBConnection,
    tickers::DataFrame,
    api_key::String = get_api_key();
    batch_size::Int = 50,
    max_concurrent::Int = 10,
    add_missing::Bool = true,
)
    @info "Starting parallel historical data update for $(nrow(tickers)) tickers"

    # Get all latest dates in one query
    latest_dates_dict = get_latest_dates_dict(conn)

    # Get latest market date
    latest_market_date = get_latest_market_date(conn)

    # Filter tickers that need updates
    tickers_to_update = filter_tickers_needing_update(tickers, latest_dates_dict, latest_market_date)

    @info "Found $(nrow(tickers_to_update)) tickers needing updates"

    if nrow(tickers_to_update) == 0
        return (String[], String[])
    end

    # Process in batches with parallel API calls
    updated_tickers = String[]
    missing_tickers = String[]
    error_tickers = String[]

    # Split into batches
    batches = [tickers_to_update[i:min(i+batch_size-1, nrow(tickers_to_update)), :]
               for i in 1:batch_size:nrow(tickers_to_update)]

    for (batch_idx, batch) in enumerate(batches)
        @info "Processing batch $batch_idx/$(length(batches)) with $(nrow(batch)) tickers"

        # Parallel API calls for this batch
        batch_results = fetch_batch_data_parallel(batch, latest_dates_dict, latest_market_date, api_key, max_concurrent)

        # Bulk insert successful results
        for (ticker, data, status) in batch_results
            if status == :success && !isempty(data)
                try
                    upsert_stock_data_bulk(conn, data, ticker)
                    push!(updated_tickers, ticker)
                catch e
                    @warn "Failed to insert data for $ticker: $e"
                    push!(error_tickers, ticker)
                end
            elseif status == :missing
                push!(missing_tickers, ticker)
            elseif status == :error
                push!(error_tickers, ticker)
            end
        end
    end

    log_update_results(missing_tickers, updated_tickers, error_tickers, add_missing)
    return (updated_tickers, missing_tickers)
end

"""
    get_latest_dates_dict(conn::DuckDBConnection)

Get latest dates for all tickers as a dictionary for fast lookup.
"""
function get_latest_dates_dict(conn::DuckDBConnection)
    latest_dates_df = DBInterface.execute(
        conn,
        """
        SELECT ticker, MAX(date) as latest_date
        FROM historical_data
        GROUP BY ticker
        """,
    ) |> DataFrame

    return Dict(row.ticker => row.latest_date for row in eachrow(latest_dates_df))
end

"""
    get_latest_market_date(conn::DuckDBConnection)

Get the latest market date from SPY or fallback to yesterday.
"""
function get_latest_market_date(conn::DuckDBConnection)
    result = DBInterface.execute(
        conn,
        """
        SELECT MAX(endDate) as max_date
        FROM us_tickers_filtered
        WHERE ticker = 'SPY'
        """,
    ) |> DataFrame

    if nrow(result) > 0 && !ismissing(result[1, :max_date])
        return result[1, :max_date]
    else
        return Date(now()) - Day(1)  # Default to yesterday if no data
    end
end

"""
    filter_tickers_needing_update(tickers::DataFrame, latest_dates_dict::Dict, latest_market_date::Date)

Filter tickers that need updates based on their latest dates.
"""
function filter_tickers_needing_update(tickers::DataFrame, latest_dates_dict::Dict, latest_market_date::Date)
    needs_update = Bool[]

    for row in eachrow(tickers)
        ticker = row.ticker
        if haskey(latest_dates_dict, ticker)
            latest_date = latest_dates_dict[ticker]
            push!(needs_update, latest_date < latest_market_date)
        else
            # Missing ticker - needs full historical data
            push!(needs_update, true)
        end
    end

    return tickers[needs_update, :]
end

"""
    fetch_batch_data_parallel(batch::DataFrame, latest_dates_dict::Dict, latest_market_date::Date,
                             api_key::String, max_concurrent::Int)

Fetch data for a batch of tickers using parallel API calls.
"""
function fetch_batch_data_parallel(batch::DataFrame, latest_dates_dict::Dict, latest_market_date::Date,
                                 api_key::String, max_concurrent::Int)

    # Create tasks for parallel execution
    tasks = []
    semaphore = Base.Semaphore(max_concurrent)  # Limit concurrent requests

    for row in eachrow(batch)
        task = @async begin
            Base.acquire(semaphore)
            try
                fetch_single_ticker_data(row, latest_dates_dict, latest_market_date, api_key)
            finally
                Base.release(semaphore)
            end
        end
        push!(tasks, task)
    end

    # Wait for all tasks to complete
    results = []
    for task in tasks
        try
            result = fetch(task)
            push!(results, result)
        catch e
            @warn "Task failed: $e"
            push!(results, ("UNKNOWN", DataFrame(), :error))
        end
    end

    return results
end

"""
    fetch_single_ticker_data(row::DataFrameRow, latest_dates_dict::Dict, latest_market_date::Date, api_key::String)

Fetch data for a single ticker with proper date range handling.
"""
function fetch_single_ticker_data(row::DataFrameRow, latest_dates_dict::Dict, latest_market_date::Date, api_key::String)
    ticker = row.ticker

    try
        if haskey(latest_dates_dict, ticker)
            # Update existing ticker
            start_date = latest_dates_dict[ticker] + Day(1)
            end_date = latest_market_date
        else
            # New ticker - get full history
            start_date = row.start_date
            end_date = min(row.end_date, latest_market_date)
        end

        if start_date > end_date
            return (ticker, DataFrame(), :up_to_date)
        end

        ticker_data = get_ticker_data(
            row,
            start_date = start_date,
            end_date = end_date,
            api_key = api_key,
        )

        if isempty(ticker_data)
            return (ticker, DataFrame(), :no_data)
        end

        status = haskey(latest_dates_dict, ticker) ? :success : :missing
        return (ticker, ticker_data, status)

    catch e
        if isa(e, AssertionError) && occursin("No data returned", string(e))
            return (ticker, DataFrame(), :no_data)
        else
            @warn "Failed to fetch data for $ticker: $e"
            return (ticker, DataFrame(), :error)
        end
    end
end

"""
    upsert_stock_data_bulk(conn::DuckDBConnection, data::DataFrame, ticker::String)

High-performance bulk upsert using DuckDB's native bulk operations.
"""
function upsert_stock_data_bulk(conn::DuckDBConnection, data::DataFrame, ticker::String)
    if isempty(data)
        return 0
    end

    # Add ticker column to data
    data_with_ticker = copy(data)
    data_with_ticker.ticker = fill(ticker, nrow(data))

    # Reorder columns to match table schema
    column_order = [:ticker, :date, :close, :high, :low, :open, :volume,
                   :adjClose, :adjHigh, :adjLow, :adjOpen, :adjVolume, :divCash, :splitFactor]

    # Ensure all required columns exist with proper defaults
    for col in column_order
        if !(col in names(data_with_ticker))
            if col in [:divCash]
                data_with_ticker[!, col] = fill(0.0, nrow(data_with_ticker))
            elseif col in [:splitFactor]
                data_with_ticker[!, col] = fill(1.0, nrow(data_with_ticker))
            elseif col in [:volume, :adjVolume]
                data_with_ticker[!, col] = fill(0, nrow(data_with_ticker))
            else
                data_with_ticker[!, col] = fill(NaN, nrow(data_with_ticker))
            end
        end
    end

    # Select and reorder columns
    data_ordered = data_with_ticker[!, column_order]

    # Create temporary table name
    temp_table = "temp_$(ticker)_$(rand(1000:9999))"

    try
        # Create temporary table
        DBInterface.execute(conn, """
            CREATE TEMPORARY TABLE $temp_table AS
            SELECT * FROM historical_data WHERE 1=0
        """)

        # Insert data into temporary table using DuckDB's efficient bulk insert
        # Convert DataFrame to the format expected by DuckDB
        rows_data = [tuple(row...) for row in eachrow(data_ordered)]

        placeholders = join(["?" for _ in 1:length(column_order)], ", ")
        insert_stmt = "INSERT INTO $temp_table VALUES ($placeholders)"

        DBInterface.execute(conn, "BEGIN TRANSACTION")

        # Bulk insert all rows
        for row_data in rows_data
            DBInterface.execute(conn, insert_stmt, row_data)
        end

        # Perform upsert using SQL
        DBInterface.execute(conn, """
            INSERT INTO historical_data
            SELECT * FROM $temp_table
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
        """)

        DBInterface.execute(conn, "COMMIT")

        return nrow(data_ordered)

    catch e
        DBInterface.execute(conn, "ROLLBACK")
        @error "Error in bulk upsert for $ticker" exception = (e, catch_backtrace())
        rethrow(e)
    finally
        # Clean up temporary table
        try
            DBInterface.execute(conn, "DROP TABLE IF EXISTS $temp_table")
        catch
            # Ignore cleanup errors
        end
    end
end

"""
    create_indexes(conn::DuckDBConnection)

Create performance indexes on the historical_data table.
"""
function create_indexes(conn::DuckDBConnection)
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_historical_ticker ON historical_data(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_historical_date ON historical_data(date)",
        "CREATE INDEX IF NOT EXISTS idx_historical_ticker_date ON historical_data(ticker, date)",
    ]

    for index_sql in indexes
        try
            DBInterface.execute(conn, index_sql)
            @info "Created index: $index_sql"
        catch e
            @warn "Failed to create index: $e"
        end
    end
end

"""
    optimize_database(conn::DuckDBConnection)

Optimize database settings for better performance.
"""
function optimize_database(conn::DuckDBConnection)
    optimizations = [
        "SET memory_limit = '4GB'",
        "SET threads = $(Threads.nthreads())",
        "SET enable_progress_bar = false",
        "SET preserve_insertion_order = false",
    ]

    for opt in optimizations
        try
            DBInterface.execute(conn, opt)
            @info "Applied optimization: $opt"
        catch e
            @warn "Failed to apply optimization $opt: $e"
        end
    end
end

function log_update_results(
    missing_tickers::Vector{String},
    updated_tickers::Vector{String},
    error_tickers::Vector{String},
    add_missing::Bool,
)
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

    @info "Historical data update completed" updated_count = length(updated_tickers) missing_count =
        length(missing_tickers) error_count = length(error_tickers)
end

"""
    update_splitted_ticker(conn::DuckDBConnection, tickers::DataFrame, api_key::String = get_api_key())

Update data for tickers that have undergone a split.
"""
function update_split_ticker(
    conn::DuckDBConnection,
    tickers::DataFrame, # all tickers is best
    api_key::String = get_api_key(),
)
    end_date = maximum(skipmissing(tickers.end_date))

    split_tickers = DBInterface.execute(
        conn,
        """
SELECT ticker, splitFactor, date
  FROM historical_data
 WHERE date = '$end_date'
   AND splitFactor <> 1.0
""",
    ) |> DataFrame

    for (i, row) in enumerate(eachrow(split_tickers))
        symbol = row.ticker
        if ismissing(symbol) || symbol === nothing
            continue  # Skip this row if ticker is missing or null
        end
        ticker_info = tickers[tickers.ticker.==symbol, :]
        if isempty(ticker_info)
            @warn "No ticker info found for $symbol"
            continue
        end
        start_date = ticker_info[1, :start_date]
        @info "$i: Updating split ticker $symbol from $start_date to $end_date"
        ticker_data = get_ticker_data(ticker_info[1, :]; api_key = api_key)
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
    DBInterface.execute(
        conn,
        """
SELECT ticker, exchange, assettype as asset_type, startdate as start_date, enddate as end_date
FROM us_tickers_filtered
WHERE assetType = 'ETF'
ORDER BY ticker;
""",
    ) |> DataFrame
end

"""
    get_tickers_stock(conn::DBInterface.Connection)

Get all stock tickers from the us_tickers_filtered table.
"""
function get_tickers_stock(conn::DBInterface.Connection)::DataFrame
    DBInterface.execute(
        conn,
        """
SELECT ticker, exchange, assettype as asset_type, startdate as start_date, enddate as end_date
FROM us_tickers_filtered
WHERE assetType = 'Stock'
ORDER BY ticker;
""",
    ) |> DataFrame
end

"""
    connect_postgres(connection_string::String)

Connect to the PostgreSQL database.
"""
function connect_postgres(connection_string::String)::PostgreSQLConnection
    try
        return LibPQ.Connection(connection_string)
    catch e
        @error "Failed to connect to PostgreSQL" exception = (e, catch_backtrace())
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
    parquet_file::String = "historical_data.parquet",
    pg_host::String = "127.0.0.1",
    pg_user::String = "otwn",
    pg_dbname::String = "tiingo",
    max_retries::Int = 3,
    retry_delay::Int = 5,
    use_dataframe::Union{Bool,Nothing} = nothing,
    max_rows_for_dataframe::Int = 1_000_000,
)
    for table_name in tables
        retry_with_exponential_backoff(max_retries, retry_delay) do
            export_table_to_postgres(
                duckdb_conn,
                pg_conn,
                table_name,
                parquet_file,
                pg_host,
                pg_user,
                pg_dbname,
                use_dataframe = use_dataframe,
                max_rows_for_dataframe = max_rows_for_dataframe,
            )
            @info "Successfully exported $table_name from DuckDB to PostgreSQL"
        end
    end
end

# New helper function for retrying with exponential backoff
function retry_with_exponential_backoff(f::Function, max_retries::Int, initial_delay::Int)
    for attempt = 1:max_retries
        try
            return f()
        catch e
            if attempt == max_retries
                @error "Failed after $max_retries attempts" exception =
                    (e, catch_backtrace())
                rethrow(e)
            end
            delay = initial_delay * 2^(attempt - 1)
            @warn "Attempt $attempt failed. Retrying in $delay seconds..." exception =
                (e, catch_backtrace())
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
    use_dataframe::Union{Bool,Nothing} = nothing,
    max_rows_for_dataframe::Int = 1_000_000,
)
    @info "Exporting table $table_name to PostgreSQL"

    # Check if the table exists in DuckDB
    table_exists =
        DBInterface.execute(
            duckdb_conn,
            """SELECT name FROM sqlite_master WHERE type='table' AND name='$table_name';""",
        ) |> DataFrame

    if isempty(table_exists)
        error("Table $table_name does not exist in DuckDB")
    end

    # Get row count
    row_count =
        DBInterface.execute(duckdb_conn, "SELECT COUNT(*) FROM $table_name") |> DataFrame
    row_count = row_count[1, 1]

    # Determine whether to use DataFrame or Parquet
    use_df = if isnothing(use_dataframe)
        row_count <= max_rows_for_dataframe
    else
        use_dataframe
    end

    if use_df
        export_table_to_postgres_dataframe(
            duckdb_conn,
            pg_conn,
            table_name,
            pg_host,
            pg_user,
            pg_dbname,
        )
    else
        export_table_to_postgres_parquet(
            duckdb_conn,
            pg_conn,
            table_name,
            parquet_file,
            pg_host,
            pg_user,
            pg_dbname,
        )
    end
end

function export_table_to_postgres_dataframe(
    duckdb_conn::DuckDBConnection,
    pg_conn::PostgreSQLConnection,
    table_name::String,
    pg_host::String,
    pg_user::String,
    pg_dbname::String,
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
        placeholders = join(["?" for _ = 1:ncol(df)], ", ")
        insert_query = "INSERT INTO $table_name ($columns) VALUES ($placeholders)"

        LibPQ.load!(
            (col => df[!, col] for col in names(df)),
            pg_conn,
            "INSERT INTO $table_name ($columns) VALUES ($placeholders)",
        )

        @info "Inserted $(nrow(df)) rows into PostgreSQL table $table_name"

    catch e
        @error "Error exporting table $table_name using DataFrames" exception =
            (e, catch_backtrace())
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
    pg_dbname::String,
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
            """COPY postgres_db.$table_name FROM '$parquet_file';""",
        )
        DBInterface.execute(duckdb_conn, "DETACH postgres_db;")
        @info "Copied data from parquet file to PostgreSQL table $table_name"
    catch e
        @error "Error exporting table $table_name using Parquet" exception =
            (e, catch_backtrace())
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

function create_or_replace_table(
    pg_conn::PostgreSQLConnection,
    table_name::String,
    create_table_query::String,
)
    # Check if the table exists in PostgreSQL
    table_exists_pg =
        LibPQ.execute(
            pg_conn,
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = '$table_name';
            """,
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
        "TIMESTAMP" => "TIMESTAMP",
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
    pg_dbname::String,
)
    try
        DBInterface.execute(duckdb_conn, "INSTALL postgres;")
        DBInterface.execute(duckdb_conn, "LOAD postgres;")
        DBInterface.execute(
            duckdb_conn,
            """
    ATTACH 'dbname=$pg_dbname user=$pg_user host=$pg_host' AS postgres_db (TYPE postgres);
""",
        )
        @info "Successfully set up PostgreSQL connection in DuckDB"
    catch e
        @error "Failed to set up PostgreSQL connection in DuckDB" exception =
            (e, catch_backtrace())
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
        @warn "Error closing DuckDB connection" exception = e
    end
end
