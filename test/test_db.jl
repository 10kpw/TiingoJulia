using Test
using DataFrames
using Dates
using DuckDB
using DBInterface
using TiingoJulia
using TiingoJulia:
    DBConstants,
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

        # Test invalid database path
        @test_throws DatabaseConnectionError connect_duckdb("nonexistent/path/db.duckdb")

        # Clean up
        close_duckdb(conn)
    end

    @testset "Table Operations" begin
        conn = connect_duckdb(test_db_path)

        # Test if tables were created
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN ('us_tickers', 'us_tickers_filtered', 'historical_data')
        """
        tables = DBInterface.execute(conn, tables_query) |> DataFrame
        @test nrow(tables) == 3

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
        )

        # Test upserting stock data
        rows_updated = upsert_stock_data(conn, test_data, "AAPL")
        @test rows_updated == 2

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
        @test result[1, :close] ≈ 150.0
        @test result[2, :close] ≈ 151.0

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
    rm(test_db_path)
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
    )

    # Create the test database
    conn = connect_duckdb(test_db_path_parallel)

    # Test database optimization
    @test_nowarn optimize_database(conn)

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
    rm(test_db_path_parallel)
end
