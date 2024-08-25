# Database related functions
using LibPQ
using Tables

"""
    connect_db(path::String)

Connect to the DuckDB database.
"""
function connect_db(path::String="tiingo_historical_data.duckdb")::DBInterface.Connection
    DBInterface.connect(DuckDB.DB, path)
end

"""
    close_db(conn::DBInterface.Connection)

Close the database connection.
"""
function close_db(conn::DBInterface.Connection)
    DBInterface.close(conn)
end

"""
    update_us_tickers(conn::DBInterface.Connection, csv_file::String)

Update the us_tickers table in the database.
"""
function update_us_tickers(
    conn::DBInterface.Connection, 
    csv_file::String="supported_tickers.csv"
)
    DBInterface.execute(conn, """
    CREATE OR REPLACE TABLE us_tickers AS
    SELECT * FROM read_csv('$csv_file')
    """)
    @info "Updated us_tickers table"
end

"""
    upsert_stock_data(conn::DBInterface.Connection, data::DataFrame, ticker::String)

Upsert stock data into the historical_data table.
"""
function upsert_stock_data(
    conn::DBInterface.Connection, 
    data::DataFrames.DataFrame, 
    ticker::String
)
    for row in eachrow(data)
        # UPSERT statement (insert or update on conflict)
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
        try
            DBInterface.execute(conn, upsert_stmt, (ticker, row.date, row.close, row.high, row.low, row.open, row.volume, row.adjClose, row.adjHigh, row.adjLow, row.adjOpen, row.adjVolume, row.divCash, row.splitFactor))
        catch e
            @error "Error upserting stock data for $ticker: $e"
        end
    end
    @info "Upserted stock data for $ticker"
end

"""
    add_historical_data(conn::DBInterface.Connection, ticker::String)

Add historical data for a single ticker.
"""
function add_historical_data(
    conn::DBInterface.Connection,
    ticker::String
)
    data = fetch_ticker_data(ticker)
    upsert_stock_data(conn, data, ticker)
    @info "Added historical data for $ticker"
end

"""
    update_historical(conn::DBInterface.Connection, tickers::DataFrame)

Update historical data for multiple tickers.
"""
function update_historical(
    conn::DBInterface.Connection,
    tickers::DataFrame
)
    end_date = maximum(skipmissing(tickers.endDate))
    not_in_hist_data = String[]

    for (i, row) in enumerate(eachrow(tickers))
        symbol = row.ticker
        hist_data = DBInterface.execute(conn, """
        SELECT ticker, max(date)+INTERVAL '1 day' AS latest_date
        FROM historical_data
        WHERE ticker = '$symbol'
        GROUP BY 1
        ORDER BY 1;
        """) |> DataFrame

        if isempty(hist_data.latest_date)
            push!(not_in_hist_data, symbol)
            continue
        else
            start_date = Dates.format(hist_data.latest_date[1], "yyyy-mm-dd")
        end

        if Date(start_date) <= end_date 
            ticker_data = fetch_ticker_data(symbol; startDate=start_date, endDate=end_date)
            upsert_stock_data(conn, ticker_data, symbol)
        else
            @info "$i : $symbol has the latest data"
        end
    end

    if isempty(not_in_hist_data)
        @info "Historical data update completed"
    else
        @warn "The following tickers are not in historical_data: $not_in_hist_data"
    end
end

"""
    update_splitted_ticker(conn::DBInterface.Connection, tickers::DataFrame)

Update data for tickers that have undergone a split.
"""
function update_splitted_ticker(
    conn::DBInterface.Connection,
    tickers::DataFrame
)
    end_date = maximum(skipmissing(tickers.endDate))
    splitted_tickers = DBInterface.execute(conn,"""
    SELECT ticker, splitFactor, date
    FROM historical_data
    WHERE date = '$end_date'
    AND splitFactor != 1.0
    """) |> DataFrame

    for (i, row) in enumerate(eachrow(splitted_tickers))
        ticker = row.ticker
        start_date = tickers[tickers.ticker .== ticker, :startDate][1]
        @info "$i: Updating split ticker $ticker from $start_date to $end_date"
        ticker_data = fetch_ticker_data(ticker; startDate=start_date, endDate=end_date)
        upsert_stock_data(conn, ticker_data, ticker)
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

# Function to store data in PostgreSQL
function store_data(df::DataFrames.DataFrame, table_name::String="")
    conn = LibPQ.Connection("postgresql://user:password@host:port/dbname")
    LibPQ.load!(df, table_name, conn)
end


"""
    connect_postgres(connection_string::String)

Connect to the PostgreSQL database.
"""
function connect_postgres(connection_string::String)::LibPQ.Connection
    LibPQ.Connection(connection_string)
end

"""
    close_postgres(conn::LibPQ.Connection)

Close the PostgreSQL database connection.
"""
function close_postgres(conn::LibPQ.Connection)
    LibPQ.close(conn)
end

"""
    export_to_postgres(duckdb_conn::DBInterface.Connection, pg_conn::LibPQ.Connection, table_name::String)

Export a table from DuckDB to PostgreSQL.
"""
function export_to_postgres(duckdb_conn::DBInterface.Connection, pg_conn::LibPQ.Connection, table_name::String)
    # Fetch data from DuckDB
    data = DBInterface.execute(duckdb_conn, "SELECT * FROM $table_name") |> DataFrame
    
    # Create table in PostgreSQL if it doesn't exist
    create_table_sql = DBInterface.execute(duckdb_conn, "SHOW CREATE TABLE $table_name") |> DataFrame
    create_table_sql = replace(create_table_sql[1, 1], "CREATE TABLE" => "CREATE TABLE IF NOT EXISTS")
    
    # Convert DuckDB types to PostgreSQL types
    create_table_sql = replace(create_table_sql, "BOOLEAN" => "BOOLEAN")
    create_table_sql = replace(create_table_sql, "INTEGER" => "INTEGER")
    create_table_sql = replace(create_table_sql, "BIGINT" => "BIGINT")
    create_table_sql = replace(create_table_sql, "DOUBLE" => "DOUBLE PRECISION")
    create_table_sql = replace(create_table_sql, "VARCHAR" => "TEXT")
    create_table_sql = replace(create_table_sql, "DATE" => "DATE")
    
    LibPQ.execute(pg_conn, create_table_sql)
    
    # Copy data to PostgreSQL
    LibPQ.load!(Tables.rowtable(data), table_name, pg_conn)
    
    @info "Exported $table_name from DuckDB to PostgreSQL"
end

"""
    export_all_to_postgres(duckdb_path::String, pg_connection_string::String)

Export all relevant tables from DuckDB to PostgreSQL.
"""
function export_all_to_postgres(duckdb_path::String, pg_connection_string::String)
    duckdb_conn = connect_db(duckdb_path)
    pg_conn = connect_postgres(pg_connection_string)
    
    try
        # Export us_tickers_filtered
        export_to_postgres(duckdb_conn, pg_conn, "us_tickers_filtered")
        
        # Export historical_data
        export_to_postgres(duckdb_conn, pg_conn, "historical_data")
        
        @info "All tables exported successfully"
    catch e
        @error "Error during export: $e"
    finally
        close_db(duckdb_conn)
        close_postgres(pg_conn)
    end
end
