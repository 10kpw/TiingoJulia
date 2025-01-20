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
    get_ticker_data(ticker::String; start_date=nothing, end_date=nothing, api_key=get_api_key(), base_url="https://api.tiingo.com/tiingo/daily")

Get historical data for a given ticker from Tiingo API.
"""
function get_ticker_data(
    ticker::String;
    start_date::Union{String,Date,Nothing} = nothing,
    end_date::Union{String,Date,Nothing} = nothing,
    api_key::String = get_api_key(),
    base_url::String = "https://api.tiingo.com/tiingo/daily"
)::DataFrame
    # Set default dates if none provided
    end_date = isnothing(end_date) ? Date(now()) : Date(end_date)
    start_date = isnothing(start_date) ? end_date - Year(5) : Date(start_date)

    # Ensure dates are formatted correctly for the API
    start_date_str = Dates.format(start_date, "yyyy-mm-dd")
    end_date_str = Dates.format(end_date, "yyyy-mm-dd")

    headers = Dict("Authorization" => "Token $api_key")

    # Get metadata to verify date range
    meta_url = "$base_url/$ticker"
    meta_data = fetch_api_data(meta_url, nothing, headers)

    # Adjust dates based on available data
    end_date = min(Date(end_date_str), Date(meta_data.endDate))
    start_date = max(Date(start_date_str), Date(meta_data.startDate))

    # Fetch price data
    url = "$base_url/$ticker/prices"
    query = Dict(
        "startDate" => Dates.format(start_date, "yyyy-mm-dd"),
        "endDate" => Dates.format(end_date, "yyyy-mm-dd")
    )

    data = fetch_api_data(url, query, headers)
    return DataFrame(data)
end

"""
    get_date_range(ticker::String, start_date, end_date, headers::Dict, base_url::String)

Helper function to get the date range for the ticker data.
"""
function get_date_range(ticker::String, start_date, end_date, headers::Dict, base_url::String)
    if isnothing(end_date)
        meta_url = "$base_url/$ticker"
        meta_data = fetch_api_data(meta_url, nothing, headers)
        end_date = meta_data.endDate
        start_date = isnothing(start_date) ? meta_data.startDate : start_date
    end
    return start_date, end_date
end

"""
    fetch_api_data(url::String, query::Union{Dict,Nothing}, headers::Dict)

Helper function to fetch data from the API.
"""
function fetch_api_data(
    url::String,
    query::Union{Dict,Nothing},
    headers::Dict;
    max_retries::Int = 3,
    retry_delay::Int = 2
)
    last_error = nothing

    for attempt in 1:max_retries
        try
            response = isnothing(query) ?
                HTTP.get(url, headers=headers) :
                HTTP.get(url, query=query, headers=headers)

            @assert response.status == 200 "Failed to fetch data from $url: $(String(response.body))"

            data = JSON3.read(String(response.body))
            @assert !isempty(data) "No data returned from $url"

            return data
        catch e
            last_error = e
            @warn "API request attempt $attempt failed" exception=e

            if attempt < max_retries
                sleep(retry_delay * 2^(attempt - 1))  # Exponential backoff
            end
        end
    end

    error("Failed to fetch data after $max_retries attempts: $last_error")
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
    csv_file::String = "supported_tickers.csv"
)
    try
        # Step 1: Download and unzip
        @info "Starting ticker download and processing..."
        download_latest_tickers(tickers_url, zip_file_path)

        # Step 2: Process CSV into us_tickers
        process_tickers_csv(conn, csv_file)

        # Step 3: Generate filtered tickers
        @info "Generating filtered tickers table..."
        DBInterface.execute(conn, """
        CREATE OR REPLACE TABLE us_tickers_filtered AS
        SELECT * FROM us_tickers
        WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
        AND endDate >= (SELECT max(endDate) FROM us_tickers WHERE assetType = 'Stock' and exchange = 'NYSE')
        AND assetType IN ('Stock', 'ETF')
        AND ticker NOT LIKE '%/%'
        """)

        # Verify the tables were created and have rows
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers") |> DataFrame
        us_tickers_count = result[1, 1]

        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers_filtered") |> DataFrame
        filtered_count = result[1, 1]

        @info "Ticker processing completed" original_count=us_tickers_count filtered_count=filtered_count

        if filtered_count == 0
            @warn "us_tickers_filtered table was created but contains no rows"
        end

    catch e
        @error "Error in download_tickers_duckdb" exception=(e, catch_backtrace())
        rethrow(e)
    finally
        # Step 4: Clean up temporary files
        cleanup_files(zip_file_path)
    end
end

"""
    download_and_unzip(url::String, zip_file_path::String)

Helper function to download and unzip a file.
"""
function download_latest_tickers(
    url::String = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip",
    zip_file_path::String = "supported_tickers.zip"
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
    process_tickers_csv(
        conn::DBInterface.Connection,
        csv_file::String="supported_tickers.csv"
    )

Helper function to process the tickers CSV file and insert into DuckDB.
"""
function process_tickers_csv(
    conn::DBInterface.Connection,
    csv_file::String
)
    try
        DBInterface.execute(conn, """
        CREATE OR REPLACE TABLE us_tickers AS
        SELECT * FROM read_csv($csv_file)
        """)
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
function generate_filtered_tickers(
    conn::DBInterface.Connection
)
    try
        # Check if us_tickers table exists and has data
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers")
        us_tickers_count = DBInterface.fetch(result) |> first |> only

        if us_tickers_count == 0
            error("us_tickers table is empty or does not exist")
        end

        # Create and populate the filtered table
        DBInterface.execute(conn, """
        CREATE OR REPLACE TABLE us_tickers_filtered AS
        SELECT * FROM us_tickers
         WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
           AND endDate >= (SELECT max(endDate) FROM us_tickers WHERE assetType = 'Stock' and exchange = 'NYSE')
           AND assetType IN ('Stock', 'ETF')
           AND ticker NOT LIKE '%/%'
        """)

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
