using HTTP
using JSON3
using DataFrames
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
    fetch_ticker_data(ticker::String; start_date=nothing, end_date=nothing, api_key=get_api_key(), base_url="https://api.tiingo.com/tiingo/daily")

Fetch historical data for a given ticker from Tiingo API.
"""
function fetch_ticker_data(
    ticker::String;
    start_date::Union{Date,String,Nothing} = nothing,
    end_date::Union{Date,String,Nothing} = nothing,
    api_key::String = get_api_key(),
    base_url::String = "https://api.tiingo.com/tiingo/daily"
)::DataFrame
    headers = Dict("Authorization" => "Token $api_key")

    start_date, end_date = get_date_range(ticker, start_date, end_date, headers, base_url)

    url = "$base_url/$ticker/prices"
    query = Dict("startDate" => Dates.format(Date(start_date), "yyyy-mm-dd"),
                 "endDate" => Dates.format(Date(end_date), "yyyy-mm-dd"))

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
function fetch_api_data(url::String, query::Union{Dict,Nothing}, headers::Dict)
    try
        response = isnothing(query) ? HTTP.get(url, headers=headers) : HTTP.get(url, query=query, headers=headers)
        @assert response.status == 200 "Failed to fetch data from $url"

        data = JSON3.read(String(response.body))
        @assert !isempty(data) "No data returned from $url"

        return data
    catch e
        error("Error fetching data from $url: $(e)")
    end
end

"""
    download_latest_tickers(tickers_url::String="https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip", duckdb_path::String="tiingo_historical_data.duckdb", zip_file_path::String="supported_tickers.zip")

Download and process the latest tickers from Tiingo.
"""
function download_latest_tickers(
    tickers_url::String = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip";
    duckdb_path::String = "tiingo_historical_data.duckdb",
    zip_file_path::String = "supported_tickers.zip"
)
    try
        download_and_unzip(tickers_url, zip_file_path)
        process_tickers_csv(duckdb_path)
        # cleanup_files(zip_file_path)
    catch e
        error("Error in download_latest_tickers: $(e)")
    end
end

"""
    download_and_unzip(url::String, zip_file_path::String)

Helper function to download and unzip a file.
"""
function download_and_unzip(url::String, zip_file_path::String)
    HTTP.download(url, zip_file_path)
    r = ZipFile.Reader(zip_file_path)
    for f in r.files
        open(f.name, "w") do io
            write(io, read(f))
        end
    end
    close(r)
end

"""
    process_tickers_csv(duckdb_path::String)

Helper function to process the tickers CSV file and insert into DuckDB.
"""
function process_tickers_csv(duckdb_path::String)
    conn = DBInterface.connect(DuckDB.DB, duckdb_path)
    try
        DBInterface.execute(conn, """
        CREATE OR REPLACE TABLE us_tickers AS
        SELECT * FROM read_csv("supported_tickers.csv")
        """)
        @info "Downloaded and processed the latest tickers from Tiingo"
    catch e
        DBInterface.execute(conn, "ROLLBACK;")
        rethrow(e)
    finally
        DBInterface.close(conn)
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
            @info "File '$file' has been deleted."
        else
            @warn "File '$file' does not exist."
        end
    end
end

"""
    generate_filtered_tickers(duckdb_path::String="tiingo_historical_data.duckdb")

Generate a filtered list of US tickers.
"""
function generate_filtered_tickers(
    duckdb_path::String = "tiingo_historical_data.duckdb"
)
    conn = nothing
    try
        # Connect to the duckdb database
        conn = DBInterface.connect(DuckDB.DB, duckdb_path)

        # Check if us_tickers table exists and has data
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers")
        us_tickers_count = DBInterface.fetch(result)[1]
        if us_tickers_count == 0
            error("us_tickers table is empty or does not exist")
        end

        # Filter the table to only include US tickers
        DBInterface.execute(conn, """
        DROP TABLE IF EXISTS us_tickers_filtered;
        CREATE TABLE us_tickers_filtered AS
        SELECT * FROM us_tickers
         WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
           AND endDate >= (SELECT max(endDate) FROM us_tickers WHERE assetType = 'Stock')
           AND assetType IN ('Stock', 'ETF')
           AND ticker NOT LIKE '%/%'
        """)

        # Verify the table was created and has rows
        result = DBInterface.execute(conn, "SELECT COUNT(*) FROM us_tickers_filtered")
        filtered_count = DBInterface.fetch(result)[1]

        @info "Original us_tickers count: $us_tickers_count"
        @info "Filtered us_tickers_filtered count: $filtered_count"

        if filtered_count == 0
            @warn "us_tickers_filtered table was created but contains no rows"
        else
            @info "Generated filtered list of US tickers with $filtered_count rows"
        end

        # Commit the changes
        DBInterface.execute(conn, "COMMIT;")

    catch e
        @error "Error in generate_filtered_tickers: $(e)"
        rethrow(e)
    finally
        if conn !== nothing
            DBInterface.close(conn)
        end
    end
end
