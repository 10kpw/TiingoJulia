# Database related functions

"""
    connect_db(path::String)

Connect to the DuckDB database.
"""
function connect_db(path::String="tiingo_historical_data.duckdb")::DBInterface.Connection
    conn = DBInterface.connect(DuckDB.DB, path)
    
    # Create tables if they don't exist
    DBInterface.execute(conn, """
    CREATE TABLE IF NOT EXISTS us_tickers (
        ticker VARCHAR,
        exchange VARCHAR,
        assetType VARCHAR,
        priceCurrency VARCHAR,
        startDate DATE,
        endDate DATE
    )
    """)

    DBInterface.execute(conn, """
    CREATE TABLE IF NOT EXISTS us_tickers_filtered AS
    SELECT * FROM us_tickers
    WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
    AND assetType IN ('Stock', 'ETF')
    AND ticker NOT LIKE '%/%'
    """)

    DBInterface.execute(conn, """
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

    return conn
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
end

"""
    add_historical_data(conn::DBInterface.Connection, ticker::String)

Add historical data for a single ticker.
"""
function add_historical_data(
    conn::DBInterface.Connection,
    ticker::String
)
    data = fetch_ticker_data(ticker, api_key=get_api_key())
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
            @info "$i : $symbol : $start_date ~ $end_date"
            ticker_data = fetch_ticker_data(symbol; startDate=start_date, endDate=end_date, api_key=get_api_key())
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
    # tickers DataFrame should contain at least ticker name and endDate

    end_date = maximum(skipmissing(tickers.endDate))
    splitted_tickers = DBInterface.execute(conn,"""
    SELECT ticker, splitFactor, date
    FROM historical_data
    WHERE date = '$end_date'
    AND splitFactor != 1.0
    """) |> DataFrame

    tickers_all = DBInterface.execute(conn,"""
    SELECT ticker, startDate
    FROM us_tickers_filtered
    """) |> DataFrame

    for (i, row) in enumerate(eachrow(splitted_tickers))
        symbol = row.ticker
        start_date = tickers_all[tickers_all.ticker .== symbol, :startDate][1]
        @info "$i: Updating split ticker $symbol from $start_date to $end_date"
        ticker_data = fetch_ticker_data(symbol; startDate=start_date, endDate=end_date, api_key=get_api_key())
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
function export_to_postgres(
    duckdb_conn::DBInterface.Connection,
    pg_conn::LibPQ.Connection,
    tables::Vector{String};
    pg_host::String="127.0.0.1",
    pg_user::String="otwn",
    pg_dbname::String="tiingo"
)
    try
        for table_name in tables
            # Export DuckDB table to parquet
            parquet_file = "$(table_name).parquet"
            DBInterface.execute(duckdb_conn, """COPY $table_name TO '$parquet_file';""")
            
            # Get table schema from DuckDB
            schema_query = "DESCRIBE $table_name"
            schema = DBInterface.execute(duckdb_conn, schema_query) |> DataFrame
            
            # Create table in PostgreSQL
            create_table_query = "CREATE TABLE IF NOT EXISTS $table_name ("
            for row in eachrow(schema)
                column_name = row.column_name
                data_type = row.column_type
                # Map DuckDB types to PostgreSQL types
                pg_type = if occursin("VARCHAR", uppercase(data_type))
                    "VARCHAR"
                elseif occursin("INTEGER", uppercase(data_type))
                    "INTEGER"
                elseif occursin("BIGINT", uppercase(data_type))
                    "BIGINT"
                elseif occursin("DOUBLE", uppercase(data_type))
                    "DOUBLE PRECISION"
                elseif occursin("BOOLEAN", uppercase(data_type))
                    "BOOLEAN"
                elseif occursin("DATE", uppercase(data_type))
                    "DATE"
                elseif occursin("TIMESTAMP", uppercase(data_type))
                    "TIMESTAMP"
                else
                    data_type  # Use the same type if no specific mapping
                end
                create_table_query *= "\"$column_name\" $pg_type, "
            end
            create_table_query = chop(create_table_query, tail=2) * ")"
            
            # Add UNIQUE constraint for historical_data table
            if table_name == "historical_data"
                create_table_query = create_table_query[1:end-1] * ", UNIQUE (ticker, date))"
            end
            
            # Execute CREATE TABLE in PostgreSQL
            LibPQ.execute(pg_conn, "DROP TABLE IF EXISTS $(table_name)_backup;")
            LibPQ.execute(pg_conn, "CREATE TABLE $(table_name)_backup AS TABLE $table_name;")
            LibPQ.execute(pg_conn, "DROP TABLE IF EXISTS $table_name;")
            LibPQ.execute(pg_conn, create_table_query)
            
            # Set up PostgreSQL connection in DuckDB
            DBInterface.execute(duckdb_conn, "INSTALL postgres;")
            DBInterface.execute(duckdb_conn, "LOAD postgres;")
            DBInterface.execute(duckdb_conn, """
                ATTACH 'dbname=$pg_dbname user=$pg_user host=$pg_host' AS postgres_db (TYPE postgres);
            """)
            
            # Copy data from parquet to PostgreSQL
            DBInterface.execute(duckdb_conn, """
                COPY postgres_db.$table_name FROM '$parquet_file';
            """)
            
            # Detach PostgreSQL connection
            DBInterface.execute(duckdb_conn, "DETACH postgres_db;")
            
            # Remove parquet file
            rm(parquet_file)
            
            @info "Successfully exported $table_name from DuckDB to PostgreSQL"
        end
    catch e
        @error "Error exporting tables to PostgreSQL" exception=(e, catch_backtrace())
        rethrow(e)
    end
end

"""
    export_all_to_postgres(duckdb_path::String, pg_connection_string::String)

Export all relevant tables from DuckDB to PostgreSQL.
"""
function export_all_to_postgres(duckdb_path::String, pg_connection::String)
    duckdb_conn = connect_db(duckdb_path)
    pg_conn = connect_postgres(pg_connection)
    
    try
        # Export historical_data
        export_to_postgres(duckdb_conn, pg_conn, "historical_data")

        # Export us_tickers_filtered
        export_to_postgres(duckdb_conn, pg_conn, "us_tickers_filtered")
        
        @info "All tables exported successfully"
    catch e
        @error "Error during export: $e"
    finally
        close_db(duckdb_conn)
        close_postgres(pg_conn)
    end
end
