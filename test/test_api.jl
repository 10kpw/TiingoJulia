using Test
using Dates
using DataFrames
using DuckDB
using DBInterface
using HTTP
using JSON3

# Import the functions you want to test
include("../src/api.jl")

@testset "API Tests" begin
    @testset "get_api_key" begin
        # Test that the function returns a non-empty string
        @test !isempty(get_api_key())

        # Test that the function throws an error when the key is not set
        withenv("TIINGO_API_KEY" => nothing) do
            @test_throws ErrorException get_api_key()
        end
    end

    @testset "get_ticker_data" begin
        # Mock the HTTP.get function to return a predefined response
        function mock_http_get(url::String; headers::Dict, query::Dict = Dict())
            @test !isempty(headers["Authorization"])
            @test haskey(query, "startDate")
            @test haskey(query, "endDate")

            body = """
            [
                {
                    "date": "2023-05-01",
                    "close": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "open": 99.5,
                    "volume": 1000000,
                    "adjClose": 100.0,
                    "adjHigh": 101.0,
                    "adjLow": 99.0,
                    "adjOpen": 99.5,
                    "adjVolume": 1000000,
                    "divCash": 0.0,
                    "splitFactor": 1.0
                }
            ]
            """
            return HTTP.Response(200, body)
        end

        # Replace the actual HTTP.get with our mock function
        HTTP.get = mock_http_get

        df = get_ticker_data("AAPL")
        @test df isa DataFrame
        @test size(df, 1) == 1
        @test df.date[1] == Date("2023-05-01")
        @test df.close[1] == 100.0
    end

    @testset "fetch_api_data" begin
        # Test successful API call
        function mock_successful_http_get(
            url::String;
            headers::Dict,
            query::Union{Dict,Nothing} = nothing,
        )
            return HTTP.Response(200, """{"key": "value"}""")
        end

        HTTP.get = mock_successful_http_get
        result = fetch_api_data(
            "http://example.com",
            Dict("param" => "value"),
            Dict("Authorization" => "Token"),
        )
        @test result == Dict("key" => "value")

        # Test failed API call
        function mock_failed_http_get(
            url::String;
            headers::Dict,
            query::Union{Dict,Nothing} = nothing,
        )
            return HTTP.Response(404, "Not Found")
        end

        HTTP.get = mock_failed_http_get
        @test_throws ErrorException fetch_api_data(
            "http://example.com",
            Dict("param" => "value"),
            Dict("Authorization" => "Token"),
        )
    end

    @testset "download_tickers_duckdb" begin
        # Mock necessary functions
        global mock_download_called = false
        global mock_process_called = false
        global mock_cleanup_called = false

        function mock_download_latest_tickers(url::String, zip_file_path::String)
            global mock_download_called = true
        end

        function mock_process_tickers_csv(conn::DBInterface.Connection, csv_file::String)
            global mock_process_called = true
        end

        function mock_cleanup_files(zip_file_path::String)
            global mock_cleanup_called = true
        end

        # Replace actual functions with mocks
        download_latest_tickers = mock_download_latest_tickers
        process_tickers_csv = mock_process_tickers_csv
        cleanup_files = mock_cleanup_files

        # Create a mock DuckDB connection
        conn = DBInterface.connect(DuckDB.DB)

        # Test the function
        download_tickers_duckdb(conn)

        @test mock_download_called
        @test mock_process_called
        @test mock_cleanup_called

        # Clean up
        DBInterface.close!(conn)
    end

    @testset "generate_filtered_tickers" begin
        # Create a mock DuckDB connection
        conn = DBInterface.connect(DuckDB.DB)

        # Create and populate a mock us_tickers table
        DBInterface.execute(
            conn,
            """
    CREATE TABLE us_tickers (
        ticker STRING,
        exchange STRING,
        assetType STRING,
        endDate DATE
    )
""",
        )
        DBInterface.execute(
            conn,
            """
    INSERT INTO us_tickers VALUES
    ('AAPL', 'NYSE', 'Stock', '2023-05-01'),
    ('GOOGL', 'NASDAQ', 'Stock', '2023-05-01'),
    ('VTI', 'NYSE ARCA', 'ETF', '2023-05-01'),
    ('INVALID', 'OTC', 'Stock', '2023-05-01')
""",
        )

        # Run the function
        generate_filtered_tickers(conn)

        # Check the results
        result =
            DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers_filtered") |>
            DataFrame
        @test result[1, 1] == 3  # AAPL, GOOGL, and VTI should be included

        # Clean up
        DBInterface.execute(conn, "DROP TABLE us_tickers")
        DBInterface.execute(conn, "DROP TABLE us_tickers_filtered")
        DBInterface.close!(conn)
    end
end
