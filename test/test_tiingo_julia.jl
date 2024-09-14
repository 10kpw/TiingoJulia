using TiingoJulia
using Test
using DataFrames
using DBInterface

@testset "TiingoJulia" begin
    @test isa(get_api_key(), String)
    @test !isempty(get_api_key())

    conn = connect_db()
    @test isa(conn, DBInterface.Connection)

    # Add some dummy data to us_tickers_filtered
    DBInterface.execute(conn, """
    INSERT INTO us_tickers_filtered (ticker, exchange, assetType, priceCurrency, startDate, endDate)
    VALUES ('AAPL', 'NASDAQ', 'Stock', 'USD', '1980-12-12', '2023-08-25')
    """)

    tickers_stock = get_tickers_stock(conn)
    @test isa(tickers_stock, DataFrame)
    @test !isempty(tickers_stock)

    # Comment out this test for now, as it requires API access
    # data = fetch_ticker_data("AAPL", startDate="2023-01-01", endDate="2023-01-31")
    # @test isa(data, DataFrame)
    # @test !isempty(data)

    close_db(conn)
end

println("All tests completed successfully!")
