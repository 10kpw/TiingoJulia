using TiingoJulia
using Test
using DataFrames
using DBInterface

@testset "TiingoJulia" begin
    if haskey(ENV, "TIINGO_API_KEY") && !isempty(ENV["TIINGO_API_KEY"])
        @test isa(get_api_key(), String)
        @test !isempty(get_api_key())
    else
        @test_throws ErrorException get_api_key()
    end

    # Use a temporary database file for testing
    test_db_path = tempname() * ".duckdb"

    # Clean up any existing test database
    isfile(test_db_path) && rm(test_db_path)

    conn = connect_duckdb(test_db_path)
    @test isa(conn, DBInterface.Connection)

    # Add some dummy data to us_tickers_filtered
    DBInterface.execute(
        conn,
        """
INSERT INTO us_tickers_filtered (ticker, exchange, assetType, priceCurrency, startDate, endDate)
VALUES ('AAPL', 'NASDAQ', 'Stock', 'USD', '1980-12-12', '2023-08-25')
""",
    )

    tickers_stock = get_tickers_stock(conn)
    @test isa(tickers_stock, DataFrame)
    @test !isempty(tickers_stock)

    close_duckdb(conn)

    # Clean up test database
    rm(test_db_path)
end

println("All tests completed successfully!")
