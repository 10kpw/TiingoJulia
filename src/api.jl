using HTTP
using JSON3
using DataFrames
using TimeSeries
using Dates
using DotEnv
using DuckDB
using DBInterface
using ZipFile

module APIConfig
using DotEnv

const ENV_FILE = ".env"
const API_KEY_NAME = "TIINGO_API_KEY"
end

"""
    load_env_file(env_path::String)::Bool

Load environment variables from .env file.
Returns true if successful, false otherwise.
"""
function load_env_file(env_path::String)::Bool
    try
        if isfile(env_path)
            DotEnv.load!(env_path)
            return true
        end
        return false
    catch e
        @warn "Failed to load .env file" exception = e
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
    env_path = joinpath(dirname(@__DIR__), APIConfig.ENV_FILE)
    if !load_env_file(env_path)
        @warn "No .env file found at $env_path"
    end

    api_key = get(ENV, APIConfig.API_KEY_NAME, nothing)
    if isnothing(api_key) || isempty(api_key)
        available_keys = join(keys(ENV), ", ")
        error("""
            $(APIConfig.API_KEY_NAME) not found in environment variables.
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
    base_url::String = "https://api.tiingo.com/tiingo/daily",
)::DataFrame
    ticker = ticker_info.ticker
    actual_start_date = something(start_date, ticker_info.start_date)
    actual_end_date = something(end_date, ticker_info.end_date)

    headers = Dict("Authorization" => "Token $api_key")
    url = "$base_url/$ticker/prices"
    query = Dict(
        "startDate" => Dates.format(actual_start_date, "yyyy-mm-dd"),
        "endDate" => Dates.format(actual_end_date, "yyyy-mm-dd"),
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
    max_retries::Int = 3,
    retry_delay::Int = 2,
)
    for attempt = 1:max_retries
        @info "API request attempt $attempt for URL: $url"
        try
            response = HTTP.get(url, query = query, headers = headers)
            @assert response.status == 200 "Failed to fetch data: $(String(response.body))"

            data = JSON3.read(String(response.body))
            @assert !isempty(data) "No data returned from $url"

            return data
        catch e
            @warn "API request attempt $attempt failed" exception = e

            if attempt < max_retries
                sleep(retry_delay * 2^(attempt - 1))  # Exponential backoff
            else
                rethrow(e)
            end
        end
    end
end


"""
    download_tickers_duckdb(
    tickers_url::String="https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip",
    duckdb_path::String="tiingo_historical_data.duckdb", zip_file_path::String="supported_tickers.zip")

Download and process the latest tickers from Tiingo.
"""
function download_tickers_duckdb(
    conn::DBInterface.Connection;
    tickers_url::String = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip",
    zip_file_path::String = "supported_tickers.zip",
    csv_file::String = "supported_tickers.csv",
)
    try
        # Step 1: Download and unzip
        @info "Starting ticker download and processing..."
        download_latest_tickers(tickers_url, zip_file_path)
        process_tickers_csv(conn, csv_file)

        # Step 2: Create filtered table and verify
        @info "Generating filtered tickers table..."
        create_filtered_tickers(conn)

        # Step 3: Verify the tables were created and have rows
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers") |> DataFrame
        us_tickers_count = result[1, 1]

        result =
            DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers_filtered") |>
            DataFrame
        filtered_count = result[1, 1]

        @info "Ticker processing completed" original_count = us_tickers_count filtered_count =
            filtered_count

        if filtered_count == 0
            @warn "us_tickers_filtered table was created but contains no rows"
        end
    catch e
        @error "Error in download_tickers_duckdb" exception = (e, catch_backtrace())
        rethrow(e)
    finally
        cleanup_files(zip_file_path)
    end
end


"""
    download_and_unzip(url::String, zip_file_path::String)

Helper function to download and unzip a file.
"""
function download_latest_tickers(
    url::String = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip",
    zip_file_path::String = "supported_tickers.zip",
)
    HTTP.download(url, zip_file_path)
    r = ZipFile.Reader(zip_file_path)
    for f in r.files
        open(f.name, "w") do io
            write(io, read(f))
        end
    end
    close(r)
    @info "Downloaded and unzipped: supported_tickers.csv"
end


"""
    create_filtered_tickers(conn::DBInterface.Connection)

Create filtered US tickers table.
"""
function create_filtered_tickers(conn::DBInterface.Connection)
    @info "Generating filtered tickers table..."
    DBInterface.execute(
        conn,
        """
    CREATE OR REPLACE TABLE us_tickers_filtered AS
    SELECT * FROM us_tickers
     WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
       AND endDate >= (SELECT max(endDate) FROM us_tickers WHERE assetType = 'Stock' and exchange = 'NYSE')
       AND assetType IN ('Stock', 'ETF')
       AND ticker NOT LIKE '%/%'
""",
    )
end


"""
    process_tickers_csv(
        conn::DBInterface.Connection,
        csv_file::String="supported_tickers.csv"
    )

Helper function to process the tickers CSV file and insert into DuckDB.
"""
function process_tickers_csv(conn::DBInterface.Connection, csv_file::String)
    try
        DBInterface.execute(
            conn,
            """
CREATE OR REPLACE TABLE us_tickers AS
SELECT * FROM read_csv($csv_file)
""",
        )
        @info "Update us_tickers in DuckDB with the CSV"
    catch e
        rethrow(e)
    end
end

"""
    cleanup_files(zip_file_path::String)

Helper function to clean up temporary files.
"""
function cleanup_files(zip_file_path::String)
    for file in [zip_file_path, "supported_tickers.csv"]
        if isfile(file)
            rm(file)
            @info "Cleaned up temporary file: $file"
        end
    end
end

"""
    generate_filtered_tickers(duckdb_path::String="tiingo_historical_data.duckdb")

Generate a filtered list of US tickers.
"""
function generate_filtered_tickers(conn::DBInterface.Connection)
    try
        # Check if us_tickers table exists and has data
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers")
        us_tickers_count = DBInterface.fetch(result) |> first |> only

        if us_tickers_count == 0
            error("us_tickers table is empty or does not exist")
        end

        # Create and populate the filtered table
        create_filtered_tickers(conn)

        # Verify the table was created and has rows
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers_filtered")
        filtered_count = DBInterface.fetch(result) |> first |> only

        @info "Original us_tickers count: $us_tickers_count"
        @info "Filtered us_tickers_filtered count: $filtered_count"

        if filtered_count == 0
            @warn "us_tickers_filtered table was created but contains no rows"
        else
            @info "Generated filtered list of US tickers with $filtered_count rows"
        end

    catch e
        @error "Error in generate_filtered_tickers: $(e)"
        rethrow(e)
    end

end
