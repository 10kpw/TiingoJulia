module Operations
    using DBInterface
    using DuckDB
    using DataFrames
    using Logging

    using ..Core: DuckDBConnection, validate_identifier

    # Name of the transient view used to expose a DataFrame to DuckDB for a
    # single set-based upsert. Registered just before the INSERT and removed
    # immediately after, so it never collides across calls on one connection.
    const UPSERT_SOURCE_VIEW = "_tiingo_upsert_source"
    const FUNDAMENTAL_DAILY_METRICS_KEY = [:perma_ticker, :metric_date]
    const SECURITY_OBSERVATIONS_KEY = [
        :perma_ticker, :observed_at, :ticker, :is_active,
    ]

    function _validate_eod_fetched_at(data::DataFrame)::Nothing
        :fetched_at in propertynames(data) || throw(ArgumentError(
            "historical_data input is missing required column: fetched_at",
        ))
        return nothing
    end

    function _validate_persistence_keys(
        data::DataFrame,
        table_name::String,
        key_columns::Vector{Symbol};
        require_columns::Bool,
    )::Nothing
        available_columns = propertynames(data)
        missing_columns = filter(column -> column ∉ available_columns, key_columns)
        if !isempty(missing_columns)
            require_columns && throw(ArgumentError(
                "$table_name is missing persistence key columns: " *
                join(String.(missing_columns), ", "),
            ))
            return nothing
        end

        duplicate_rows = findall(nonunique(data, key_columns))
        isempty(duplicate_rows) || throw(ArgumentError(
            "$table_name contains duplicate persistence key " *
            "($(join(String.(key_columns), ", "))) at rows: " *
            join(duplicate_rows, ", "),
        ))
        return nothing
    end

    """
        validate_fundamental_daily_metrics_keys(data; require_columns=true)

    Reject duplicate canonical Daily Metrics persistence keys before a sink
    starts writing. When `require_columns=false`, non-metric frames are ignored
    so generic sinks can retain their independent DataFrame contract.
    """
    function validate_fundamental_daily_metrics_keys(
        data::DataFrame;
        require_columns::Bool=true,
    )::Nothing
        return _validate_persistence_keys(
            data,
            "fundamental_daily_metrics",
            FUNDAMENTAL_DAILY_METRICS_KEY,
            require_columns=require_columns,
        )
    end

    """
        validate_security_observation_keys(data; require_columns=true)

    Reject duplicate canonical security-observation persistence keys before a
    sink starts writing.
    """
    function validate_security_observation_keys(
        data::DataFrame;
        require_columns::Bool=true,
    )::Nothing
        return _validate_persistence_keys(
            data,
            "security_observations",
            SECURITY_OBSERVATIONS_KEY,
            require_columns=require_columns,
        )
    end

    """
        execute_upsert_set_based(conn, data, ticker)

    Upsert `data` into `historical_data` with a single set-based statement.

    The DataFrame is registered as a zero-copy DuckDB view and inserted in one
    `INSERT ... SELECT ... ON CONFLICT` round trip, replacing the previous
    per-row prepared-statement loop. `COALESCE` mirrors the old per-row default
    handling (NaN for prices, 0 for volumes, 0.0 dividends, 1.0 split factor).

    Assumes one ticker's data with unique `date` values per call (true for a
    Tiingo daily response); duplicate keys within a batch would violate the
    `ON CONFLICT` target. Rows older than the stored `fetched_at` are skipped,
    and the return value is the number of inserted or updated rows.
    """
    function execute_upsert_set_based(
        conn::DuckDBConnection,
        data::DataFrame,
        ticker::String
    )::Int
        if nrow(data) == 0
            return 0
        end

        # JSON null decodes to `nothing`, and DuckDB has no logical type for
        # Nothing; only `missing` registers as NULL. An all-null column is
        # then `Vector{Missing}`, which DuckDB cannot type either, so give it
        # a concrete element type and let COALESCE and the column cast apply.
        data = mapcols(data) do column
            replaced = replace(column, nothing => missing)
            eltype(replaced) === Missing ? Vector{Union{Missing,Int64}}(replaced) : replaced
        end
        DuckDB.register_data_frame(conn, data, UPSERT_SOURCE_VIEW)
        try
            upsert_stmt = """
            INSERT INTO historical_data (ticker, date, close, high, low, open, volume, adjClose, adjHigh, adjLow, adjOpen, adjVolume, divCash, splitFactor, fetched_at)
            SELECT
                CAST(? AS VARCHAR),
                date,
                COALESCE(close, 'nan'::FLOAT),
                COALESCE(high, 'nan'::FLOAT),
                COALESCE(low, 'nan'::FLOAT),
                COALESCE(open, 'nan'::FLOAT),
                COALESCE(volume, 0),
                COALESCE(adjClose, 'nan'::FLOAT),
                COALESCE(adjHigh, 'nan'::FLOAT),
                COALESCE(adjLow, 'nan'::FLOAT),
                COALESCE(adjOpen, 'nan'::FLOAT),
                COALESCE(adjVolume, 0),
                COALESCE(divCash, 0.0),
                COALESCE(splitFactor, 1.0),
                fetched_at
            FROM $UPSERT_SOURCE_VIEW
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
                splitFactor = EXCLUDED.splitFactor,
                fetched_at = EXCLUDED.fetched_at
            WHERE EXCLUDED.fetched_at >= historical_data.fetched_at
            RETURNING 1
            """
            persisted = DBInterface.execute(conn, upsert_stmt, (ticker,)) |> DataFrame
            return nrow(persisted)
        finally
            DuckDB.unregister_data_frame(conn, UPSERT_SOURCE_VIEW)
        end
    end

    """
        upsert_stock_data(conn::DuckDBConnection, data::DataFrame, ticker::String)

    Upsert stock data into the historical_data table. `data` must include the
    call-scoped `fetched_at` provenance supplied by the collector.
    """
    function upsert_stock_data(
        conn::DuckDBConnection,
        data::DataFrame,
        ticker::String
    )
        _validate_eod_fetched_at(data)
        try
            return execute_upsert_set_based(conn, data, ticker)
        catch e
            @error "Error upserting stock data for $ticker" exception=(e, catch_backtrace())
            rethrow(e)
        end
    end

    """
        upsert_stock_data_bulk(conn::DuckDBConnection, data::DataFrame, ticker::String)

    Bulk upsert stock data into the historical_data table. `data` must include
    the call-scoped `fetched_at` provenance supplied by the collector. Filters
    to `ticker` when a `ticker` column is present, then delegates to the
    set-based upsert.
    """
    function upsert_stock_data_bulk(
        conn::DuckDBConnection,
        data::DataFrame,
        ticker::String
    )
        _validate_eod_fetched_at(data)
        if nrow(data) == 0
            return 0
        end

        # Filter data for the specific ticker only if the column exists.
        # Use propertynames (Symbols); names() returns Strings and would never match.
        has_ticker_col = :ticker in propertynames(data)
        ticker_data = has_ticker_col ? filter(row -> row.ticker == ticker, data) : data
        if nrow(ticker_data) == 0
            return 0
        end

        try
            return execute_upsert_set_based(conn, ticker_data, ticker)
        catch e
            @error "Error bulk upserting stock data for $ticker" exception=(e, catch_backtrace())
            rethrow(e)
        end
    end

    """
        upsert_security_observations(conn::DuckDBConnection, data::DataFrame)::Int

    Idempotently upsert observed security identity snapshots using
    `(perma_ticker, observed_at, ticker, is_active)` as the deterministic key.
    """
    function upsert_security_observations(conn::DuckDBConnection, data::DataFrame)::Int
        validate_security_observation_keys(data)
        nrow(data) == 0 && return 0

        DuckDB.register_data_frame(conn, data, UPSERT_SOURCE_VIEW)
        try
            persisted = DBInterface.execute(conn, """
                INSERT INTO security_observations (
                    perma_ticker, observed_at, ticker, is_active, is_adr,
                    daily_last_updated, exchange, asset_type,
                    price_coverage_start, price_coverage_end, is_leveraged,
                    join_status
                )
                SELECT
                    perma_ticker, observed_at, ticker, is_active, is_adr,
                    daily_last_updated, exchange, asset_type,
                    price_coverage_start, price_coverage_end, is_leveraged,
                    join_status
                FROM $UPSERT_SOURCE_VIEW
                ON CONFLICT (perma_ticker, observed_at, ticker, is_active)
                DO UPDATE SET
                    is_adr = EXCLUDED.is_adr,
                    daily_last_updated = EXCLUDED.daily_last_updated,
                    exchange = EXCLUDED.exchange,
                    asset_type = EXCLUDED.asset_type,
                    price_coverage_start = EXCLUDED.price_coverage_start,
                    price_coverage_end = EXCLUDED.price_coverage_end,
                    is_leveraged = EXCLUDED.is_leveraged,
                    join_status = EXCLUDED.join_status
                RETURNING 1
            """) |> DataFrame
            return nrow(persisted)
        finally
            DuckDB.unregister_data_frame(conn, UPSERT_SOURCE_VIEW)
        end
    end

    """
        upsert_fundamental_daily_metrics(conn::DuckDBConnection, data::DataFrame)::Int

    Idempotently upsert Daily Metrics rows using `(perma_ticker, metric_date)`
    as the deterministic key.
    """
    function upsert_fundamental_daily_metrics(
        conn::DuckDBConnection,
        data::DataFrame,
    )::Int
        nrow(data) == 0 && return 0
        validate_fundamental_daily_metrics_keys(data)

        DuckDB.register_data_frame(conn, data, UPSERT_SOURCE_VIEW)
        try
            persisted = DBInterface.execute(conn, """
                INSERT INTO fundamental_daily_metrics (
                    perma_ticker, metric_date, market_cap, enterprise_value,
                    pe_ratio, available_at, fetched_at, source_revision
                )
                SELECT
                    perma_ticker, metric_date, market_cap, enterprise_value,
                    pe_ratio, available_at, fetched_at, source_revision
                FROM $UPSERT_SOURCE_VIEW
                ON CONFLICT (perma_ticker, metric_date) DO UPDATE SET
                    market_cap = EXCLUDED.market_cap,
                    enterprise_value = EXCLUDED.enterprise_value,
                    pe_ratio = EXCLUDED.pe_ratio,
                    available_at = EXCLUDED.available_at,
                    fetched_at = EXCLUDED.fetched_at,
                    source_revision = EXCLUDED.source_revision
                WHERE EXCLUDED.fetched_at >= fundamental_daily_metrics.fetched_at
                RETURNING 1
            """) |> DataFrame
            return nrow(persisted)
        finally
            DuckDB.unregister_data_frame(conn, UPSERT_SOURCE_VIEW)
        end
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
    export upsert_security_observations, upsert_fundamental_daily_metrics
    export validate_fundamental_daily_metrics_keys, validate_security_observation_keys
    export get_tickers_all, get_tickers_etf, get_tickers_stock
    export get_table_count, get_latest_dates, get_latest_date
end
