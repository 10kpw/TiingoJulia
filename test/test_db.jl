using Test
using DataFrames
using Dates
using DuckDB
using DBInterface
using TiingoJulia
using TiingoJulia: DBConstants, DatabaseConnectionError, DatabaseQueryError

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
