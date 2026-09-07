using Test
using DataFrames
using Dates
using DuckDB
using DBInterface
using QuansiftMarketData
using QuansiftMarketData:
    DatabaseConnectionError,
    DatabaseQueryError,
    DuckDBConnection,
    PostgreSQLConnection,
    verify_duckdb_integrity,
    get_tickers_all,
    upsert_stock_data,
    upsert_stock_data_bulk,
    close_duckdb,
    connect_duckdb,
    connect_postgres,
    close_postgres,
    optimize_database
using QuansiftMarketData.DB.Postgres: connection_options_map, normalize_postgres_connection_string, postgres_env_vars

# Define mock function for fetch_single_ticker_data that works without Mocking
function mock_fetch_single_ticker_data(row, latest_dates_dict, latest_market_date, api_key)
    ticker = row.ticker

    # Create mock data
    mock_data = DataFrame(
        date = [
            latest_market_date - Day(5),
            latest_market_date - Day(4),
            latest_market_date - Day(3),
        ],
        close = [100.0, 101.0, 102.0],
        high = [105.0, 106.0, 107.0],
        low = [95.0, 96.0, 97.0],
        open = [98.0, 99.0, 100.0],
        volume = [1000000, 1100000, 1200000],
        adjClose = [100.0, 101.0, 102.0],
        adjHigh = [105.0, 106.0, 107.0],
        adjLow = [95.0, 96.0, 97.0],
        adjOpen = [98.0, 99.0, 100.0],
        adjVolume = [1000000, 1100000, 1200000],
        divCash = [0.0, 0.0, 0.0],
        splitFactor = [1.0, 1.0, 1.0],
    )

    if haskey(latest_dates_dict, ticker)
        status = :success
    else
        status = :missing
    end

    return (ticker, mock_data, status)
end

function _eod_freshness_fixture(dates; fetched_at=nothing, close=100.0)
    row_count = length(dates)
    frame = DataFrame(
        date = collect(dates),
        close = fill(close, row_count),
        high = fill(101.0, row_count),
        low = fill(99.0, row_count),
        open = fill(100.0, row_count),
        volume = fill(1_000, row_count),
        adjClose = fill(close, row_count),
        adjHigh = fill(101.0, row_count),
        adjLow = fill(99.0, row_count),
        adjOpen = fill(100.0, row_count),
        adjVolume = fill(1_000, row_count),
        divCash = fill(0.0, row_count),
        splitFactor = fill(1.0, row_count),
    )
    if !isnothing(fetched_at)
        insertcols!(frame, :fetched_at => fill(fetched_at, row_count))
    end
    return frame
end

@testset "Database Operations" begin
    # Use a temporary database file so tests don't rely on any existing DuckDB
    test_db_path = tempname() * ".duckdb"

    # Clean up any existing test database
    isfile(test_db_path) && rm(test_db_path)

    @testset "Database Connection" begin
        # Test database connection
        conn = nothing
        @test_nowarn conn = connect_duckdb(test_db_path)
        @test conn isa DuckDBConnection

        # Test database verification
        is_valid, error_msg = verify_duckdb_integrity(test_db_path)
        @test is_valid == true
        @test error_msg === nothing

        # Test invalid database path (use a path that definitely can't be created)
        @test_throws DatabaseConnectionError connect_duckdb("/dev/null/invalid.duckdb")

        # Clean up
        close_duckdb(conn)
    end

    @testset "Historical data fetched_at migration survives reopen" begin
        legacy_path = tempname() * ".duckdb"
        legacy_conn = DBInterface.connect(DuckDB.DB, legacy_path)
        DBInterface.execute(legacy_conn, """
            CREATE TABLE historical_data (
                ticker VARCHAR,
                date DATE
            )
        """)
        DBInterface.execute(
            legacy_conn,
            "INSERT INTO historical_data VALUES ('OLD', DATE '2024-01-02')",
        )
        DBInterface.close!(legacy_conn)

        migrated_conn = connect_duckdb(legacy_path)
        migrated = DBInterface.execute(
            migrated_conn,
            "SELECT fetched_at FROM historical_data WHERE ticker = 'OLD'",
        ) |> DataFrame
        @test only(migrated.fetched_at) == DateTime(1970, 1, 1)

        DBInterface.execute(
            migrated_conn,
            "INSERT INTO historical_data (ticker, date) " *
            "VALUES ('NEW', DATE '2024-01-03')",
        )
        defaulted = DBInterface.execute(
            migrated_conn,
            "SELECT fetched_at FROM historical_data WHERE ticker = 'NEW'",
        ) |> DataFrame
        @test only(defaulted.fetched_at) > DateTime(1970, 1, 1)
        utc_now = Dates.now(Dates.UTC)
        @test abs(Dates.value(only(defaulted.fetched_at) - utc_now)) < 10_000
        fetched_at_default = only((DBInterface.execute(
            migrated_conn,
            "SELECT column_default FROM information_schema.columns " *
            "WHERE table_schema = 'main' " *
            "AND table_name = 'historical_data' " *
            "AND column_name = 'fetched_at'",
        ) |> DataFrame).column_default)
        @test occursin(
            "make_timestamp_ms(epoch_ms(current_timestamp))",
            lowercase(replace(fetched_at_default, " " => "")),
        )
        close_duckdb(migrated_conn)

        is_valid, error_msg = verify_duckdb_integrity(legacy_path)
        @test is_valid
        @test error_msg === nothing

        reopened_conn = connect_duckdb(legacy_path)
        reopened = DBInterface.execute(
            reopened_conn,
            "SELECT count(*) AS row_count FROM historical_data",
        ) |> DataFrame
        @test only(reopened.row_count) == 2
        close_duckdb(reopened_conn)
    end

    @testset "Historical data fetched_at migration survives dependent indexes" begin
        legacy_path = tempname() * ".duckdb"
        legacy_conn = DBInterface.connect(DuckDB.DB, legacy_path)
        DBInterface.execute(legacy_conn, """
            CREATE TABLE historical_data (
                ticker VARCHAR,
                date DATE,
                UNIQUE (ticker, date)
            )
        """)
        for statement in (
            "CREATE INDEX idx_historical_ticker ON historical_data(ticker)",
            "CREATE INDEX idx_historical_date ON historical_data(date)",
            "CREATE INDEX idx_historical_ticker_date ON historical_data(ticker, date)",
        )
            DBInterface.execute(legacy_conn, statement)
        end
        DBInterface.execute(
            legacy_conn,
            "INSERT INTO historical_data VALUES ('OLD', DATE '2024-01-02')",
        )
        DBInterface.close!(legacy_conn)

        migrated_conn = connect_duckdb(legacy_path)
        nullability = DBInterface.execute(migrated_conn, """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = 'historical_data'
              AND column_name = 'fetched_at'
        """) |> DataFrame
        @test only(nullability.is_nullable) == "NO"

        restored_indexes = DBInterface.execute(migrated_conn, """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE schema_name = 'main' AND table_name = 'historical_data'
        """) |> DataFrame
        @test sort(String.(restored_indexes.index_name)) == [
            "idx_historical_date",
            "idx_historical_ticker",
            "idx_historical_ticker_date",
        ]

        backfilled = DBInterface.execute(
            migrated_conn,
            "SELECT fetched_at FROM historical_data WHERE ticker = 'OLD'",
        ) |> DataFrame
        @test only(backfilled.fetched_at) == DateTime(1970, 1, 1)
        close_duckdb(migrated_conn)
    end

    @testset "Historical data fetched_at migration restores a missing default" begin
        # Reachable by hand repair: an operator who read the failure, applied
        # the obvious ALTER, and stopped there leaves NOT NULL without the
        # canonical default. Keying the migration on nullability alone would
        # skip such a database forever.
        legacy_path = tempname() * ".duckdb"
        legacy_conn = DBInterface.connect(DuckDB.DB, legacy_path)
        DBInterface.execute(legacy_conn, """
            CREATE TABLE historical_data (
                ticker VARCHAR,
                date DATE,
                fetched_at TIMESTAMP NOT NULL,
                UNIQUE (ticker, date)
            )
        """)
        DBInterface.execute(
            legacy_conn,
            "CREATE INDEX idx_historical_ticker ON historical_data(ticker)",
        )
        DBInterface.execute(
            legacy_conn,
            "INSERT INTO historical_data " *
            "VALUES ('OLD', DATE '2024-01-02', TIMESTAMP '2024-01-02 00:00:00')",
        )
        DBInterface.close!(legacy_conn)

        migrated_conn = connect_duckdb(legacy_path)
        fetched_at_default = only((DBInterface.execute(
            migrated_conn,
            "SELECT column_default FROM information_schema.columns " *
            "WHERE table_schema = 'main' " *
            "AND table_name = 'historical_data' " *
            "AND column_name = 'fetched_at'",
        ) |> DataFrame).column_default)
        @test occursin(
            "make_timestamp_ms(epoch_ms(current_timestamp))",
            lowercase(replace(fetched_at_default, " " => "")),
        )

        nullability = DBInterface.execute(migrated_conn, """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = 'historical_data'
              AND column_name = 'fetched_at'
        """) |> DataFrame
        @test only(nullability.is_nullable) == "NO"

        restored_indexes = DBInterface.execute(migrated_conn, """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE schema_name = 'main' AND table_name = 'historical_data'
        """) |> DataFrame
        @test String.(restored_indexes.index_name) == ["idx_historical_ticker"]

        # The repair must not disturb provenance that was already correct.
        preserved = DBInterface.execute(
            migrated_conn,
            "SELECT fetched_at FROM historical_data WHERE ticker = 'OLD'",
        ) |> DataFrame
        @test only(preserved.fetched_at) == DateTime(2024, 1, 2)
        close_duckdb(migrated_conn)
    end

    @testset "Security observation key migration preserves state history" begin
        legacy_path = tempname() * ".duckdb"
        legacy_conn = DBInterface.connect(DuckDB.DB, legacy_path)
        DBInterface.execute(legacy_conn, """
            CREATE TABLE security_observations (
                perma_ticker VARCHAR NOT NULL,
                observed_at TIMESTAMP NOT NULL,
                ticker VARCHAR NOT NULL,
                is_active BOOLEAN NOT NULL,
                is_adr BOOLEAN,
                daily_last_updated TIMESTAMP,
                exchange VARCHAR,
                asset_type VARCHAR,
                price_coverage_start DATE,
                price_coverage_end DATE,
                is_leveraged BOOLEAN,
                join_status VARCHAR NOT NULL,
                PRIMARY KEY (perma_ticker, observed_at)
            )
        """)
        DBInterface.execute(legacy_conn, """
            INSERT INTO security_observations
                (perma_ticker, observed_at, ticker, is_active, join_status)
            VALUES ('perm-old', TIMESTAMP '2026-07-19 12:00:00',
                    'OLD', true, 'matched')
        """)
        DBInterface.close!(legacy_conn)

        migrated_conn = connect_duckdb(legacy_path)
        constraints = DBInterface.execute(migrated_conn, """
            SELECT constraint_column_names
            FROM duckdb_constraints()
            WHERE schema_name = 'main'
              AND table_name = 'security_observations'
              AND constraint_type = 'PRIMARY KEY'
        """) |> DataFrame
        @test only(constraints.constraint_column_names) ==
              ["perma_ticker", "observed_at", "ticker", "is_active"]
        DBInterface.execute(migrated_conn, """
            INSERT INTO security_observations
                (perma_ticker, observed_at, ticker, is_active, join_status)
            VALUES ('perm-old', TIMESTAMP '2026-07-19 12:00:00',
                    'OLD', false, 'inactive')
        """)
        stored = DBInterface.execute(migrated_conn, """
            SELECT ticker, is_active, join_status
            FROM security_observations
            ORDER BY is_active DESC
        """) |> DataFrame
        @test stored.is_active == [true, false]
        @test stored.join_status == ["matched", "inactive"]
        close_duckdb(migrated_conn)
    end

    @testset "Table Operations" begin
        conn = connect_duckdb(test_db_path)

        # Test if tables were created
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN (
                'us_tickers',
                'us_tickers_filtered',
                'historical_data',
                'security_observations',
                'fundamental_daily_metrics'
            )
        """
        tables = DBInterface.execute(conn, tables_query) |> DataFrame
        @test nrow(tables) == 5

        # Test table schemas
        historical_schema =
            DBInterface.execute(conn, "DESCRIBE historical_data") |> DataFrame
        @test "ticker" in historical_schema.column_name
        @test "date" in historical_schema.column_name
        @test "close" in historical_schema.column_name

        close_duckdb(conn)
    end

    @testset "Data Operations" begin
        conn = connect_duckdb(test_db_path)

        # Test data for us_tickers - match the actual schema
        # First, let's check the actual schema
        schema = DBInterface.execute(conn, "DESCRIBE us_tickers") |> DataFrame
        @info "us_tickers schema: $schema"

        # Test updating us_tickers with correct schema
        DBInterface.execute(
            conn,
            """
    INSERT INTO us_tickers (ticker, exchange, assetType, priceCurrency, startDate, endDate)
    VALUES ('AAPL', 'NASDAQ', 'Stock', 'USD', '2000-01-01', '2023-12-31'),
        ('GOOGL', 'NASDAQ', 'Stock', 'USD', '2004-08-19', '2023-12-31')
""",
        )

        # Refresh filtered tickers table after inserting
        DBInterface.execute(
            conn,
            """
CREATE OR REPLACE TABLE us_tickers_filtered AS
SELECT * FROM us_tickers
WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
  AND assetType IN ('Stock', 'ETF')
  AND ticker NOT LIKE '%/%'
""",
        )

        # Test retrieving tickers
        all_tickers = get_tickers_all(conn)
        @test nrow(all_tickers) > 0
        @test "AAPL" in all_tickers.ticker

        # Test stock data operations
        test_data = DataFrame(
            date = [Date("2023-01-01"), Date("2023-01-02")],
            close = [150.0, 151.0],
            high = [152.0, 153.0],
            low = [149.0, 150.0],
            open = [150.0, 151.0],
            volume = [1000000, 1100000],
            adjClose = [150.0, 151.0],
            adjHigh = [152.0, 153.0],
            adjLow = [149.0, 150.0],
            adjOpen = [150.0, 151.0],
            adjVolume = [1000000, 1100000],
            divCash = [0.0, 0.0],
            splitFactor = [1.0, 1.0],
            fetched_at = fill(DateTime(2026, 8, 16, 12), 2),
        )

        # Test upserting stock data
        original_chunk_size = get(ENV, "TIINGO_DUCKDB_UPSERT_CHUNK_SIZE", nothing)
        ENV["TIINGO_DUCKDB_UPSERT_CHUNK_SIZE"] = "1"
        try
            rows_updated = upsert_stock_data(conn, test_data, "AAPL")
            @test rows_updated == 2

            updated_data = copy(test_data)
            updated_data.close .= [155.0, 156.0]
            rows_updated = upsert_stock_data(conn, updated_data, "AAPL")
            @test rows_updated == 2
        finally
            if original_chunk_size === nothing
                delete!(ENV, "TIINGO_DUCKDB_UPSERT_CHUNK_SIZE")
            else
                ENV["TIINGO_DUCKDB_UPSERT_CHUNK_SIZE"] = original_chunk_size
            end
        end

        # Verify inserted data
        result = DBInterface.execute(
            conn,
            """
    SELECT * FROM historical_data
    WHERE ticker = 'AAPL'
    ORDER BY date
""",
        ) |> DataFrame
        @test nrow(result) == 2
        @test result[1, :close] ≈ 155.0
        @test result[2, :close] ≈ 156.0

        close_duckdb(conn)
    end

    @testset "Error Handling" begin
        conn = connect_duckdb(test_db_path)

        # Test invalid SQL
        @test_throws Exception DBInterface.execute(conn, "SELECT * FROM nonexistent_table")

        # Test data type mismatch
        @test_throws Exception DBInterface.execute(
            conn,
            """
    INSERT INTO historical_data (ticker, date, close)
    VALUES ('AAPL', 'invalid_date', 'invalid_price')
""",
        )

        close_duckdb(conn)
    end

    # Clean up test database
    rm(test_db_path; force=true)
end

@testset "PostgreSQL Export Operations" begin
    # Note: These tests would require a running PostgreSQL instance
    # They should be skipped if PostgreSQL is not available
    @test_skip begin
        # Test PostgreSQL connection
        pg_conn = connect_postgres("dbname=test_db user=test_user")
        @test pg_conn isa PostgreSQLConnection
        close_postgres(pg_conn)
    end
end

@testset "PostgreSQL Connection Helpers" begin
    normalized_uri = normalize_postgres_connection_string(
        "postgresql://alice:secret@db.example.com:5432/tiingo?sslmode=require";
        timeout_seconds=15,
    )
    normalized_uri_options = connection_options_map(normalized_uri)

    @test normalized_uri_options["host"] == "db.example.com"
    @test normalized_uri_options["port"] == "5432"
    @test normalized_uri_options["dbname"] == "tiingo"
    @test normalized_uri_options["user"] == "alice"
    @test normalized_uri_options["password"] == "secret"
    @test normalized_uri_options["sslmode"] == "require"
    @test normalized_uri_options["connect_timeout"] == "15"

    normalized_kv = normalize_postgres_connection_string(
        "host=localhost dbname=tiingo user=alice connect_timeout=7";
        timeout_seconds=15,
    )
    normalized_kv_options = connection_options_map(normalized_kv)
    @test normalized_kv_options["connect_timeout"] == "7"

    env_vars = postgres_env_vars(normalized_uri_options)
    @test env_vars["PGHOST"] == "db.example.com"
    @test env_vars["PGPORT"] == "5432"
    @test env_vars["PGDATABASE"] == "tiingo"
    @test env_vars["PGUSER"] == "alice"
    @test env_vars["PGPASSWORD"] == "secret"
    @test env_vars["PGSSLMODE"] == "require"
end

@testset "Parallel Processing Features" begin
    # Test database file path
    test_db_path_parallel = tempname() * "_parallel.duckdb"

    # Clean up any existing test database
    isfile(test_db_path_parallel) && rm(test_db_path_parallel)

    # Create mock test data
    test_data = DataFrame(
        ticker = repeat(["AAPL", "GOOGL"], outer = 10),
        date = repeat([Date("2023-01-01"), Date("2023-01-02")], outer = 10),
        close = rand(100.0:200.0, 20),
        high = rand(150.0:250.0, 20),
        low = rand(50.0:150.0, 20),
        open = rand(100.0:200.0, 20),
        volume = rand(1000000:5000000, 20),
        adjClose = rand(100.0:200.0, 20),
        adjHigh = rand(150.0:250.0, 20),
        adjLow = rand(50.0:150.0, 20),
        adjOpen = rand(100.0:200.0, 20),
        adjVolume = rand(1000000:5000000, 20),
        divCash = zeros(20),
        splitFactor = ones(20),
        fetched_at = fill(DateTime(2026, 8, 16, 12), 20),
    )

    # Create the test database
    conn = connect_duckdb(test_db_path_parallel)

    # Test database optimization
    original_threads = get(ENV, "TIINGO_DUCKDB_THREADS", nothing)
    original_worker_threads = get(ENV, "TIINGO_DUCKDB_WORKER_THREADS", nothing)
    original_memory_limit = get(ENV, "TIINGO_DUCKDB_MEMORY_LIMIT_GB", nothing)
    original_preserve_order = get(ENV, "TIINGO_DUCKDB_PRESERVE_INSERTION_ORDER", nothing)

    ENV["TIINGO_DUCKDB_THREADS"] = "1"
    ENV["TIINGO_DUCKDB_WORKER_THREADS"] = "1"
    ENV["TIINGO_DUCKDB_MEMORY_LIMIT_GB"] = "1"
    ENV["TIINGO_DUCKDB_PRESERVE_INSERTION_ORDER"] = "false"

    try
        @test_nowarn optimize_database(conn)
        settings = DBInterface.execute(
            conn,
            """
        SELECT name, value
        FROM duckdb_settings()
        WHERE name IN ('threads', 'worker_threads', 'external_threads', 'preserve_insertion_order')
        """,
        ) |> DataFrame
        threads_value = parse(Int, settings[settings.name .== "threads", :value][1])
        worker_threads_value = parse(Int, settings[settings.name .== "worker_threads", :value][1])
        @test threads_value >= 1
        @test worker_threads_value >= 1
        @test threads_value == worker_threads_value
        @test settings[settings.name .== "external_threads", :value][1] == "1"
        @test settings[settings.name .== "preserve_insertion_order", :value][1] == "false"
    finally
        if original_threads === nothing
            delete!(ENV, "TIINGO_DUCKDB_THREADS")
        else
            ENV["TIINGO_DUCKDB_THREADS"] = original_threads
        end
        if original_worker_threads === nothing
            delete!(ENV, "TIINGO_DUCKDB_WORKER_THREADS")
        else
            ENV["TIINGO_DUCKDB_WORKER_THREADS"] = original_worker_threads
        end
        if original_memory_limit === nothing
            delete!(ENV, "TIINGO_DUCKDB_MEMORY_LIMIT_GB")
        else
            ENV["TIINGO_DUCKDB_MEMORY_LIMIT_GB"] = original_memory_limit
        end
        if original_preserve_order === nothing
            delete!(ENV, "TIINGO_DUCKDB_PRESERVE_INSERTION_ORDER")
        else
            ENV["TIINGO_DUCKDB_PRESERVE_INSERTION_ORDER"] = original_preserve_order
        end
    end

    # Test parallel data insertion
    @test_nowarn upsert_stock_data_bulk(conn, test_data, "AAPL")

    # Verify data was inserted
    result = DBInterface.execute(
        conn,
        """
    SELECT COUNT(*) FROM historical_data
    WHERE ticker = 'AAPL'
""",
    ) |> DataFrame
    @test result[1, 1] > 0

    # Clean up test database
    close_duckdb(conn)
    rm(test_db_path_parallel; force=true)
end

@testset "PostgreSQL connect retries back off exponentially" begin
    # A fixed delay retries three times inside 15 seconds, which is short
    # enough that all three land inside the same failover or restart the
    # connection was waiting out. Doubling spreads the attempts across it,
    # and the cap keeps a large configured delay from stalling the run.
    backoff = QuansiftMarketData.DB.Postgres._connect_retry_delay_seconds

    @test backoff(5, 1) == 5
    @test backoff(5, 2) == 10
    @test backoff(5, 3) == 20
    @test backoff(5, 4) == 40

    # Monotonic, and capped rather than unbounded.
    delays = [backoff(5, attempt) for attempt in 1:12]
    @test issorted(delays)
    @test all(delay -> delay <= 60, delays)
    @test delays[end] == 60

    # A zero delay stays zero: opting out of waiting must not resurrect it.
    @test all(attempt -> backoff(0, attempt) == 0, 1:5)
    # A huge configured delay is clamped, not multiplied.
    @test backoff(1_000, 1) == 60
end

@testset "DuckDB EOD upsert maps JSON nothing to NULL" begin
    # Tiingo `null` decodes to `nothing`; DuckDB.register_data_frame has no
    # logical type for Nothing, so the raw vendor frame must be sanitized.
    conn = connect_duckdb(":memory:")
    try
        frame = _eod_freshness_fixture([Date(2024, 1, 2)]; fetched_at = DateTime(2026, 9, 7))
        frame[!, :volume] = [nothing]
        frame[!, :adjVolume] = Union{Nothing,Int64}[nothing]

        @test upsert_stock_data(conn, frame, "FLMI") == 1
        stored = DBInterface.execute(
            conn,
            "SELECT volume, adjVolume FROM historical_data WHERE ticker = 'FLMI'",
        ) |> DataFrame
        @test stored.volume == [0]
        @test stored.adjVolume == [0]
    finally
        close_duckdb(conn)
    end
end

@testset "DuckDB EOD upsert preserves the freshest observation" begin
    conn = connect_duckdb(":memory:")
    try
        first_date = Date(2024, 1, 2)
        second_date = Date(2024, 1, 3)
        initial_fetched_at = DateTime(2026, 8, 16, 12)
        initial = _eod_freshness_fixture(
            [first_date];
            fetched_at = initial_fetched_at,
            close = 100.0,
        )
        @test upsert_stock_data_bulk(conn, initial, "AAPL") == 1

        mixed = _eod_freshness_fixture(
            [first_date, second_date];
            fetched_at = DateTime(2026, 8, 16, 11),
            close = 90.0,
        )
        @test upsert_stock_data_bulk(conn, mixed, "AAPL") == 1

        equal = _eod_freshness_fixture(
            [first_date];
            fetched_at = initial_fetched_at,
            close = 105.0,
        )
        @test upsert_stock_data_bulk(conn, equal, "AAPL") == 1

        stored = DBInterface.execute(
            conn,
            "SELECT date, close, fetched_at FROM historical_data " *
            "WHERE ticker = 'AAPL' ORDER BY date",
        ) |> DataFrame
        @test stored.date == [first_date, second_date]
        @test stored.close == [105.0, 90.0]
        @test stored.fetched_at == [
            initial_fetched_at,
            DateTime(2026, 8, 16, 11),
        ]

        missing_provenance = _eod_freshness_fixture(
            [Date(2024, 1, 4)];
            close = 110.0,
        )
        for writer in (upsert_stock_data, upsert_stock_data_bulk)
            caught = try
                writer(conn, missing_provenance, "AAPL")
                nothing
            catch error
                error
            end
            @test caught isa ArgumentError
            @test sprint(showerror, caught) ==
                  "ArgumentError: historical_data input is missing required " *
                  "column: fetched_at"
        end
        preserved = DBInterface.execute(
            conn,
            "SELECT date, close, fetched_at FROM historical_data " *
            "WHERE ticker = 'AAPL' ORDER BY date",
        ) |> DataFrame
        @test preserved.date == [first_date, second_date]
        @test preserved.close == [105.0, 90.0]
        @test preserved.fetched_at == [
            initial_fetched_at,
            DateTime(2026, 8, 16, 11),
        ]
    finally
        close_duckdb(conn)
    end
end
