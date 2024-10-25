using HTTP
using JSON3
using DataFrames
using TimeSeries
using Dates
using DotEnv
using DuckDB
using DBInterface
using ZipFile

"""
    get_api_key()

Retrieve the Tiingo API key from environment variables or .env file.
"""
function get_api_key()::String
    # Try to load .env file
    env_path = joinpath(dirname(@__DIR__), ".env")
    if isfile(env_path)
        DotEnv.load!(env_path)
    else
        @warn "No .env file found at $env_path"
    end

    api_key = get(ENV, "TIINGO_API_KEY", nothing)

    if isnothing(api_key)
        @warn "TIINGO_API_KEY not found in ENV. Current ENV keys: $(keys(ENV))"
        error("TIINGO_API_KEY not found. Please set it in your .env file at the root of your project.")
    end

    return api_key
end

"""
    fetch_daily_fundamental(ticker::String; api_key=get_api_key(), base_url="https://api.tiingo.com/tiingo/fundamentals")

Fetch daily fundamental data for a given ticker from Tiingo API.
"""
function fetch_daily_fundamental(
    ticker::String;
    api_key::String = get_api_key(),
    base_url::String = "https://api.tiingo.com/tiingo/fundamentals",
    return_type::String = "original"
)
    headers = Dict("Content-Type" => "application/json")
    url = "$base_url/$ticker/daily"
    query = Dict("token" => api_key)

    data = fetch_api_data(url, query, headers)
    if return_type == "original"
        return data
    elseif return_type == "dataframe"
        return DataFrame(data)
    elseif return_type == "timearray"
        df = DataFrame(data)
        return TimeArray(df.date, Matrix(df[:, Not(:date)]), Symbol.(names(df[:, Not(:date)])))
    else
        error("Invalid return_type. Choose 'original', 'dataframe', or 'timearray'.")
    end
end
