using HTTP
using JSON3
using DataFrames
using TimeSeries
using Dates
using DotEnv

using ..Config

const ENV_LOAD_ATTEMPTED = Ref(false)

function resolve_env_path(env_file::String)::String
    return isabspath(env_file) ? env_file : joinpath(dirname(@__DIR__), env_file)
end

"""
    load_env_file(env_path::String)::Bool

Load environment variables from .env file.
Returns true if successful, false otherwise.
"""
function load_env_file(env_path::String; override::Bool=false)::Bool
    try
        if isfile(env_path)
            DotEnv.load!(env_path; override=override)
            return true
        end
        return false
    catch e
        @warn "Failed to load .env file" exception=e
        return false
    end
end

"""
    get_api_key(; env_path::Union{String,Nothing}=nothing, reload_env::Bool=false)::String

Retrieve the Tiingo API key from environment variables or .env file.
Throws an error if the API key is not found.
"""
function get_api_key(; env_path::Union{String,Nothing}=nothing, reload_env::Bool=false)::String
    resolved_env_path = isnothing(env_path) ? resolve_env_path(Config.API.ENV_FILE) : env_path

    if reload_env || !ENV_LOAD_ATTEMPTED[]
        load_env_file(resolved_env_path)
        ENV_LOAD_ATTEMPTED[] = true
    end

    api_key = strip(get(ENV, Config.API.API_KEY_NAME, ""))
    if isempty(api_key)
        error("""
            $(Config.API.API_KEY_NAME) not found in environment variables.
            Set it in your shell environment or in: $resolved_env_path
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
    retry_delay::Int = Config.API.RETRY_DELAY,
    connect_timeout::Real = 10,
    readtimeout::Real = 30
)
    last_error = nothing

    for attempt in 1:max_retries
        @info "API request attempt $attempt for URL: $url"
        response = nothing
        try
            response = HTTP.get(
                url,
                query=query,
                headers=headers,
                connect_timeout=connect_timeout,
                readtimeout=readtimeout
            )
        catch e
            last_error = e
            @warn "API request attempt $attempt failed" exception=e

            if attempt < max_retries
                sleep(retry_delay * 2^(attempt - 1))  # Exponential backoff
                continue
            else
                rethrow(e)
            end
        end

        status = response.status
        if status == 200
            data = JSON3.read(String(response.body))
            if isempty(data)
                throw(ErrorException("No data returned from $url"))
            end
            return data
        end

        retryable = status == 429 || (status >= 500 && status <= 599)
        body = String(response.body)
        err = ErrorException("HTTP $status for $url: $body")
        last_error = err

        if retryable && attempt < max_retries
            retry_after = HTTP.header(response, "Retry-After")
            delay = retry_delay * 2^(attempt - 1)
            if retry_after !== nothing
                parsed = try
                    parse(Int, retry_after)
                catch
                    nothing
                end
                if parsed !== nothing
                    delay = max(delay, parsed)
                end
            end
            @warn "API request retryable failure" status=status delay_seconds=delay
            sleep(delay)
        else
            throw(err)
        end
    end

    # Fallback; this should be unreachable but keeps return path explicit
    if last_error !== nothing
        throw(last_error)
    end
end
