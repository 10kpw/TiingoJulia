module Operations
    using DBInterface
    using DuckDB
    using DataFrames
    using Dates
    using Logging

    using ..Config
    using ..Core: DuckDBConnection, validate_identifier

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

        # Filter data for the specific ticker only if the column exists
        has_ticker_col = :ticker in names(data)
        ticker_data = has_ticker_col ? filter(row -> row.ticker == ticker, data) : data
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
        get_table_count(conn::DBInterface.Connection, table_name::String)::Int

    Helper function to get the row count of a table.
    """
    function get_table_count(conn::DBInterface.Connection, table_name::String)::Int
        safe_name = validate_identifier(table_name)
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM $safe_name") |> DataFrame
        return result[1, 1]
    end

    """
        get_latest_dates(conn::DuckDBConnection)
    """
    function get_latest_dates(conn::DuckDBConnection)
        DBInterface.execute(conn, """
            SELECT ticker, MAX(date) as latest_date
            FROM historical_data
            GROUP BY ticker
        """) |> DataFrame
    end

    """
        get_latest_date(conn::DuckDBConnection, symbol::String)::DataFrame
    """
    function get_latest_date(conn::DuckDBConnection, symbol::String)::DataFrame
        DBInterface.execute(conn, """
        SELECT ticker, max(date) + 1 AS latest_date
        FROM historical_data
        WHERE ticker = ?
        GROUP BY 1
        ORDER BY 1;
        """, [symbol]) |> DataFrame
    end
    export upsert_stock_data, upsert_stock_data_bulk
    export get_tickers_all, get_tickers_etf, get_tickers_stock
    export get_table_count, get_latest_dates, get_latest_date
end
