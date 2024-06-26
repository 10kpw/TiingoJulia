# Create a simple console logger
console_logger = LoggingExtras.TeeLogger(LoggingExtras.NullLogger(), ConsoleLogger(stderr, Logging.Info))
# Set the global logger to the console logger
global_logger(console_logger)

function store_data(df::DataFrames.DataFrame, table_name::String="")
    conn = LibPQ.Connection("postgresql://user:password@host:port/dbname")
    LibPQ.load!(df, table_name, conn)
end


function update_us_tickers(
    conn::DBInterface.Connection, 
    csv_file::String="supported_tickers.csv"
)
    DBInterface.execute(conn, """
    CREATE OR REPLACE TABLE us_tickers AS
    SELECT * FROM read_csv("$csv_file")
    """)
end


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
            println("Error upserting stock data: $e")
        end
    end
end


function add_historical_data(
    conn::DBInterface.Connection,
    ticker::String
)
    data = fetch_ticker_data(ticker)
    upsert_stock_data(conn, data, ticker)
end


function update_historical(
    conn::DBInterface.Connection,
    tickers::DataFrames.DataFrame=nothing,
)

    end_date = maximum(skipmissing(tickers.endDate))
    not_in_hist_data = []
    for (i, row) in enumerate(eachrow(tickers))
        symbol = row.ticker
        hist_data = DBInterface.execute(conn,
        """
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
            @info("$i : $symbol : you have the latest data")
        end
    end

    if isempty(not_in_hist_data)
        @info("Completed")
    else
        @warn("Chcekc ticker in this list since each ticker is not in historical_data: $not_in_hist_data")
    end
end


function update_splitted_ticker(
    conn::DBInterface.Connection,
    tickers::DataFrame
)

    end_date = maximum(skipmissing(tickers.endDate))
    splitted_tickers = DBInterface.execute(conn,
    """
    SELECT ticker, splitFactor, date
    FROM historical_data
    WHERE date = '$end_date'
    AND splitFactor != 1.0
    """) |> DataFrame

    for (i, row) in enumerate(eachrow(splitted_tickers))
        ticker = row.ticker
        tickers_all = get_tickers_all()
        start_date = tickers_all[tickers_all.ticker .== ticker, :startDate][1]
        @info("$i : $ticker : $start_date ~ $end_date")
        ticker_data = fetch_ticker_data(ticker, start_date=start_date, end_date=end_date)
        upsert_stock_data(conn, ticker_data, ticker)
    end
end


function get_tickers_etf(
    conn::DBInterface.Connection
)::DataFrame

    return DBInterface.execute(conn, """
    SELECT ticker, exchange, assetType, startDate, endDate
    FROM us_tickers_filtered
    WHERE assetType = 'ETF'
    ORDER BY ticker;
    """) |> DataFrame 
end


function get_tickers_stock(
    conn::DBInterface.Connection
)::DataFrame

    return DBInterface.execute(conn, """
    SELECT ticker, exchange, assetType, startDate, endDate
    FROM us_tickers_filtered
    WHERE assetType = 'Stock'
    ORDER BY ticker;
    """) |> DataFrame 
end