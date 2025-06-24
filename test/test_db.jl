using Test
using DataFrames
using Dates
using DuckDB
using DBInterface
using TiingoJulia
using TiingoJulia: DBConstants, DatabaseConnectionError, DatabaseQueryError

# Check if Mocking is available
const HAS_MOCKING = try
    @eval using Mocking
    true
catch e
    @warn "Mocking package not available, some tests will be skipped" exception=e
    false
end

# Define mock function for fetch_single_ticker_data that works with or without Mocking
function mock_fetch_single_ticker_data(row, latest_dates_dict, latest_market_date, api_key)
    ticker = row.ticker

    # Create mock data
    mock_data = DataFrame(
        date = [latest_market_date - Day(5), latest_market_date - Day(4), latest_market_date - Day(3)],
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
        splitFactor = [1.0, 1.0, 1.0]
    )

    if haskey(latest_dates_dict, ticker)
        status = :success
    else
        status = :missing
    end

    return (ticker, mock_data, status)
end

@testset "Database Operations" begin
    # Test database file path
    const TEST_DB_PATH = "test_tiingo.duckdb"

    # Clean up any existing test database
    isfile(TEST_DB_PATH) && rm(TEST_DB_PATH)

    @testset "Database Connection" begin
        # Test database connection
        conn = nothing
        @test_nowarn conn = connect_duckdb(TEST_DB_PATH)
        @test conn isa DuckDBConnection

        # Test database verification
        is_valid, error_msg = verify_duckdb_integrity(TEST_DB_PATH)
        @test is_valid == true
        @test error_msg === nothing

        # Test invalid database path
        @test_throws DatabaseConnectionError connect_duckdb("nonexistent/path/db.duckdb")

        # Clean up
        close_duckdb(conn)
    end

    @testset "Table Operations" begin
        conn = connect_duckdb(TEST_DB_PATH)

        # Test if tables were created
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN ('us_tickers', 'us_tickers_filtered', 'historical_data')
        """
        tables = DBInterface.execute(conn, tables_query) |> DataFrame
        @test nrow(tables) == 3

        # Test table schemas
        historical_schema = DBInterface.execute(conn, "DESCRIBE historical_data") |> DataFrame
        @test "ticker" in historical_schema.column_name
        @test "date" in historical_schema.column_name
        @test "close" in historical_schema.column_name

        close_duckdb(conn)
    end

    @testset "Data Operations" begin
        conn = connect_duckdb(TEST_DB_PATH)

        # Test data for us_tickers
        test_tickers = DataFrame(
            ticker = ["AAPL", "GOOGL"],
            exchange = ["NASDAQ", "NASDAQ"],
            assetType = ["Stock", "Stock"],
            priceCurrency = ["USD", "USD"],
            startDate = [Date("2000-01-01"), Date("2004-08-19")],
            endDate = [Date("2023-12-31"), Date("2023-12-31")]
        )

        # Test updating us_tickers
        DBInterface.execute(conn, """
            INSERT INTO us_tickers
            VALUES ('AAPL', 'NASDAQ', 'Stock', 'USD', '2000-01-01', '2023-12-31'),
                   ('GOOGL', 'NASDAQ', 'Stock', 'USD', '2004-08-19', '2023-12-31')
        """)

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
            splitFactor = [1.0, 1.0]
        )

        # Test upserting stock data
        rows_updated = upsert_stock_data(conn, test_data, "AAPL")
        @test rows_updated == 2

        # Verify inserted data
        result = DBInterface.execute(conn, """
            SELECT * FROM historical_data
            WHERE ticker = 'AAPL'
            ORDER BY date
        """) |> DataFrame
        @test nrow(result) == 2
        @test result[1, :close] ≈ 150.0
        @test result[2, :close] ≈ 151.0

        close_duckdb(conn)
    end

    @testset "Error Handling" begin
        conn = connect_duckdb(TEST_DB_PATH)

        # Test invalid SQL
        @test_throws Exception DBInterface.execute(conn, "SELECT * FROM nonexistent_table")

        # Test data type mismatch
        @test_throws Exception DBInterface.execute(conn, """
            INSERT INTO historical_data (ticker, date, close)
            VALUES ('AAPL', 'invalid_date', 'invalid_price')
        """)

        close_duckdb(conn)
    end

    # Clean up test database
    rm(TEST_DB_PATH)
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

@testset "Parallel Processing Features" begin
    # Test database file path
    const TEST_DB_PATH = "test_tiingo_parallel.duckdb"

    # Clean up any existing test database
    isfile(TEST_DB_PATH) && rm(TEST_DB_PATH)

    # Create mock test data
    test_ticker_data = DataFrame(
        ticker = ["AAPL", "MSFT", "GOOGL"],
        exchange = ["NASDAQ", "NASDAQ", "NASDAQ"],
        asset_type = ["Stock", "Stock", "Stock"],
        start_date = [Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01")],
        end_date = [Date("2023-01-01"), Date("2023-01-01"), Date("2023-01-01")]
    )

    # Create the test database
    conn = connect_duckdb(TEST_DB_PATH)

    # Test database optimization
    @test_nowarn optimize_database(conn)

    # Create tables
    @test_nowarn create_tables(conn)

    # Test bulk upsert
    sample_data = DataFrame(
        date = [Date("2023-01-01") + Day(i) for i in 1:10],
        close = rand(100:200, 10),
        high = rand(100:200, 10),
        low = rand(100:200, 10),
        open = rand(100:200, 10),
        volume = rand(1000000:5000000, 10),
        adjClose = rand(100:200, 10),
        adjHigh = rand(100:200, 10),
        adjLow = rand(100:200, 10),
        adjOpen = rand(100:200, 10),
        adjVolume = rand(1000000:5000000, 10),
        divCash = zeros(10),
        splitFactor = ones(10)
    )

    # Test bulk upsert with mock data
    @test_nowarn upsert_stock_data_bulk(conn, sample_data, "TEST_TICKER")

    # Verify the data was inserted
    result = DBInterface.execute(conn, "SELECT COUNT(*) FROM historical_data WHERE ticker = 'TEST_TICKER'") |> DataFrame
    @test result[1, 1] == 10

    # Test filter_tickers_needing_update function
    latest_dates_dict = Dict("TEST_TICKER" => Date("2023-01-05"))
    latest_market_date = Date("2023-01-10")

    filtered_tickers = TiingoJulia.filter_tickers_needing_update(
        DataFrame(ticker = ["TEST_TICKER", "NEW_TICKER"]),
        latest_dates_dict,
        latest_market_date
    )

    @test nrow(filtered_tickers) == 2
    @test "TEST_TICKER" in filtered_tickers.ticker
    @test "NEW_TICKER" in filtered_tickers.ticker

    # Test parallel processing with mock function - conditional on Mocking availability
    if HAS_MOCKING
        @info "Testing parallel processing with Mocking"
        Mocking.activate()

        # Create a patch for fetch_single_ticker_data
        patch = @patch TiingoJulia.fetch_single_ticker_data(row, latest_dates_dict, latest_market_date, api_key) =
            mock_fetch_single_ticker_data(row, latest_dates_dict, latest_market_date, api_key)

        apply(patch) do
            # Test the parallel data fetching
            test_batch = DataFrame(
                ticker = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"],
                exchange = fill("NASDAQ", 5),
                asset_type = fill("Stock", 5),
                start_date = fill(Date("2020-01-01"), 5),
                end_date = fill(Date("2023-01-01"), 5)
            )

            # Test with various concurrency settings
            for max_concurrent in [1, 2, Threads.nthreads()]
                results = TiingoJulia.fetch_batch_data_parallel(
                    test_batch,
                    latest_dates_dict,
                    latest_market_date,
                    "mock-api-key",
                    max_concurrent
                )

                # Verify results
                @test length(results) == 5
                for result in results
                    ticker, data, status = result
                    @test ticker in test_batch.ticker
                    @test status in [:success, :missing]
                    @test nrow(data) == 3
                end
            end
        end

        Mocking.deactivate()
    else
        @info "Skipping Mocking-based tests as Mocking is not available"
    end

    # Clean up test database
    close_duckdb(conn)
    rm(TEST_DB_PATH)
end
