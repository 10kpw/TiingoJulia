using Test
using HTTP
using JSON3
using DataFrames
using Dates
using Mocking

include("../src/api.jl")  # Ensure this points to the file where `download_from_tiingo` is defined

Mocking.activate()

const test_ticker = "AAPL"
const test_start_date = "2020-01-01"
const test_end_date = "2020-01-10"
const test_api_key = "fake_api_key"
ENV["TIINGO_API_KEY"] = test_api_key  # Set a fake API key for testing
const test_url = "https://api.tiingo.com/tiingo/daily/$test_ticker/prices"  # Define test_url at a higher scope

@testset "Tiingo API Data Download Tests" begin
    @testset "Successful API Request" begin
        test_response_body = """[{"date":"2020-01-01","close":300.35},{"date":"2020-01-02","close":305.60}]"""
        test_response = HTTP.Response(200, Dict("Content-Type" => "application/json"), test_response_body; request=nothing)
        
        @mock HTTP.get(test_url, queryparams=Dict("startDate" => test_start_date, "endDate" => test_end_date, "token" => test_api_key)) do
            test_response
        end

        df = download_from_tiingo(test_ticker, test_start_date, test_end_date)
        @test isa(df, DataFrame)
        @test size(df, 1) == 2  # Check if two rows are returned
        @test df[1, :close] == 300.35  # Check the close value of the first row
    end

    @testset "API Key Not Set" begin
        delete!(ENV, "TIINGO_API_KEY")  # Remove the API key
        @test_throws AssertionError download_from_tiingo(test_ticker, test_start_date, test_end_date)
    end

    @testset "API Request Failure" begin
        ENV["TIINGO_API_KEY"] = test_api_key  # Reset API key
        test_response = HTTP.Response(404, Dict("Content-Type" => "application/json"), "", version=VersionNumber(1,1))
        
        @mock HTTP.get(test_url, queryparams=Dict("startDate" => test_start_date, "endDate" => test_end_date, "token" => test_api_key)) do
            test_response
        end

        @test_throws AssertionError download_from_tiingo(test_ticker, test_start_date, test_end_date)
    end
end