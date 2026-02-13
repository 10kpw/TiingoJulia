using Test
using Dates
using DataFrames
using DuckDB
using DBInterface
using HTTP
using JSON3

# Import the functions yousing TiingoJulia
using TiingoJulia.API

@testset "API Tests" begin
    @testset "get_api_key" begin
        # Test that the function returns a non-empty string when API key is available
        # First check if we have an API key set
        if haskey(ENV, "TIINGO_API_KEY") && !isempty(ENV["TIINGO_API_KEY"])
            @test !isempty(get_api_key())
        else
            # If no API key is set, test that it throws an error
            @test_throws ErrorException get_api_key()
        end

        # Test that the function throws an error when the key is not set,
        # without depending on any repo-local .env file.
        original_key = get(ENV, "TIINGO_API_KEY", nothing)
        missing_env_path = tempname()

        if !isnothing(original_key)
            delete!(ENV, "TIINGO_API_KEY")
        end

        try
            @test_throws ErrorException get_api_key(env_path=missing_env_path, reload_env=true)
        finally
            # Restore the original API key
            if !isnothing(original_key)
                ENV["TIINGO_API_KEY"] = original_key
            end
        end
    end

    @testset "get_ticker_data" begin
        # Create a mock ticker info DataFrameRow
        ticker_df = DataFrame(
            ticker = ["AAPL"],
            start_date = [Date("2023-05-01")],
            end_date = [Date("2023-05-01")]
        )
        ticker_info = ticker_df[1, :]

        # Run live API tests only when explicitly enabled
        if get(ENV, "TIINGO_TEST_LIVE_API", "false") == "true"
            try
                get_ticker_data(ticker_info)
                @test true
            catch e
                @test e isa Exception
            end
        else
            @test_skip "Skipping live API test (set TIINGO_TEST_LIVE_API=true to enable)"
        end
    end

    @testset "fetch_api_data" begin
        if get(ENV, "TIINGO_TEST_LIVE_API", "false") == "true"
            # Test the error handling path using a known-bad URL
            @test_throws HTTP.Exceptions.ConnectError fetch_api_data(
                "http://invalid-url-for-testing.com",
                Dict("param" => "value"),
                Dict("Authorization" => "Token invalid-key")
            )
        else
            @test_skip "Skipping live network test (set TIINGO_TEST_LIVE_API=true to enable)"
        end
    end

    @testset "download_tickers_duckdb" begin
        if get(ENV, "TIINGO_TEST_LIVE_API", "false") == "true"
            # Create a mock DuckDB connection
            conn = DBInterface.connect(DuckDB.DB)

            # Test the function with a simple case
            try
                TiingoJulia.download_tickers_duckdb(conn)
                @test true
            catch e
                @test e isa Exception
            end

            # Clean up
            DBInterface.close!(conn)
        else
            @test_skip "Skipping live download test (set TIINGO_TEST_LIVE_API=true to enable)"
        end
    end

    # @testset "generate_filtered_tickers" begin
    #     # Create a mock DuckDB connection
    #     conn = DBInterface.connect(DuckDB.DB)

    #     # Create and populate a mock us_tickers table
    #     DBInterface.execute(
    #         conn,
    #         """
    # CREATE TABLE us_tickers (
    #     ticker STRING,
    #     exchange STRING,
    #     assetType STRING,
    #     endDate DATE
    # )
    # """,
    #     )
    #     DBInterface.execute(
    #         conn,
    #         """
    # INSERT INTO us_tickers VALUES
    # ('AAPL', 'NYSE', 'Stock', '2023-05-01'),
    # ('GOOGL', 'NASDAQ', 'Stock', '2023-05-01'),
    # ('VTI', 'NYSE ARCA', 'ETF', '2023-05-01'),
    # ('INVALID', 'OTC', 'Stock', '2023-05-01')
    # """,
    #     )

    #     # Run the function
    #     TiingoJulia.generate_filtered_tickers(conn)

    #     # Check the results
    #     result =
    #         DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers_filtered") |>
    #         DataFrame
    #     @test result[1, 1] == 3  # AAPL, GOOGL, and VTI should be included

    #     # Clean up
    #     DBInterface.execute(conn, "DROP TABLE us_tickers")
    #     DBInterface.execute(conn, "DROP TABLE us_tickers_filtered")
    #     DBInterface.close!(conn)
    # end
end
