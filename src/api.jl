using HTTP
using JSON3
using DataFrames
using TimeSeries
using Dates
using DotEnv

using ..Config

"""
    load_env_file(env_path::String)::Bool

Load environment variables from .env file.
Returns true if successful, false otherwise.
"""
function load_env_file(env_path::String)::Bool
    try
        if isfile(env_path)
            # Force override to ensure correct API key is loaded even if ENV is already set
            DotEnv.load!(env_path; override=true)
            return true
        end
        return false
    catch e
        @warn "Failed to load .env file" exception=e
        return false
    end
end

"""
    get_api_key()::String

Retrieve the Tiingo API key from environment variables or .env file.
Throws an error if the API key is not found.
"""
function get_api_key()::String
    # Try to load .env file
    env_path = joinpath(dirname(@__DIR__), Config.API.ENV_FILE)
    if !load_env_file(env_path)
        @warn "No .env file found at $env_path"
    end

    api_key = get(ENV, Config.API.API_KEY_NAME, nothing)
    if isnothing(api_key)
        available_keys = join(keys(ENV), ", ")
        error("""
            $(Config.API.API_KEY_NAME) not found in environment variables.
            Available keys: $available_keys
            Please set it in your .env file at: $env_path
        """)
    end

    return api_key
end


"""
    get_ticker_data(
        ticker_info::DataFrameRow;
        start_date::Union{Date,Nothing} = nothing,
        end_date::Union{Date,Nothing} = nothing,
        api_key::String = get_api_key(),
        base_url::String = "https://api.tiingo.com/tiingo/daily"
    )::DataFrame

Get historical data for a given ticker from Tiingo API.
"""
function get_ticker_data(
    ticker_info::DataFrameRow;
    start_date::Union{Date,Nothing} = nothing,
    end_date::Union{Date,Nothing} = nothing,
    api_key::String = get_api_key(),
    base_url::String = Config.API.BASE_URL
)::DataFrame
    ticker = ticker_info.ticker
    actual_start_date = something(start_date, ticker_info.start_date)
    actual_end_date = something(end_date, ticker_info.end_date)

    headers = Dict("Authorization" => "Token $api_key")
    url = "$base_url/$ticker/prices"
    query = Dict(
        "startDate" => Dates.format(actual_start_date, "yyyy-mm-dd"),
        "endDate" => Dates.format(actual_end_date, "yyyy-mm-dd")
    )

    @info "Fetching price data for $ticker from $actual_start_date to $actual_end_date"
    data = fetch_api_data(url, query, headers)

    return DataFrame(data)
end


"""
    fetch_api_data(url::String, query::Dict, headers::Dict; max_retries::Int=3)

Fetch data from API with retry logic and error handling.
"""
function fetch_api_data(
    url::String,
    query::Dict,
    headers::Dict;
    max_retries::Int = Config.API.MAX_RETRIES,
    retry_delay::Int = Config.API.RETRY_DELAY
)
    for attempt in 1:max_retries
        @info "API request attempt $attempt for URL: $url"
        try
            response = HTTP.get(url, query=query, headers=headers)
            if response.status != 200
                throw(ErrorException("Failed to fetch data: $(String(response.body))"))
            end

            data = JSON3.read(String(response.body))
            if isempty(data)
                throw(ErrorException("No data returned from $url"))
            end

            return data
        catch e
            @warn "API request attempt $attempt failed" exception=e

            if attempt < max_retries
                sleep(retry_delay * 2^(attempt - 1))  # Exponential backoff
            else
                rethrow(e)
            end
        end
    end
end
