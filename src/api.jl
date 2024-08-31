using HTTP
using JSON3
using DataFrames
using Dates
using DotEnv

const BASE_URL = "https://api.tiingo.com/tiingo/daily"

"""
    get_api_key()

Retrieve the Tiingo API key from environment variables or .env file.
"""
function get_api_key()
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
    fetch_ticker_data(ticker::String; startDate=nothing, endDate=nothing)

Fetch historical data for a given ticker from Tiingo API.
"""
function fetch_ticker_data(
    ticker::String;
    startDate::Union{Date,String,Nothing}=nothing,
    endDate::Union{Date,String,Nothing}=nothing,
    api_key::String=get_api_key()
)::DataFrame
    headers = Dict("Authorization" => "Token $api_key")

    if isnothing(endDate)
        meta_url = "$BASE_URL/$ticker"
        meta_resp = HTTP.get(meta_url, headers=headers)
        @assert meta_resp.status == 200 "Failed to fetch metadata for $ticker"
        meta_data = JSON3.read(String(meta_resp.body))
        endDate = meta_data.endDate
        startDate = isnothing(startDate) ? meta_data.startDate : startDate
    end

    start_date = Dates.format(Date(startDate), "yyyy-mm-dd")
    end_date = Dates.format(Date(endDate), "yyyy-mm-dd")

    url = "$BASE_URL/$ticker/prices"
    query_params = Dict(
        "startDate" => start_date,
        "endDate" => end_date,
        "token" => api_key
    )

    response = HTTP.get(url, query=query_params, headers=headers)
    @assert response.status == 200 "Failed to fetch data for $ticker"

    data = JSON3.read(String(response.body), Vector{Dict})
    @assert !isempty(data) "No data returned for $ticker"

    return DataFrame(data)
end

"""
    download_latest_tickers(tickers_url::String, duckdb_path::String)

Download and process the latest tickers from Tiingo.
"""
function download_latest_tickers(
    tickers_url::String="https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip";
    duckdb_path::String = "tiingo_historical_data.duckdb",
    zip_file_path::String = "supported_tickers.zip"
)
    # Download the zip file
    HTTP.download(tickers_url, zip_file_path)

    # Unzip the file
    r = ZipFile.Reader(zip_file_path)
    for f in r.files
        open(f.name, "w") do io
            write(io, read(f))
        end
    end
    close(r)

    # Connect to the duckdb database
    conn = DBInterface.connect(DuckDB.DB, duckdb_path)
    DBInterface.execute(conn, """
    CREATE OR REPLACE TABLE us_tickers AS
    SELECT * FROM read_csv("supported_tickers.csv")
    """);
    DBInterface.close(conn)

    # Delete the unzipped csv file
    # if isfile("supported_tickers.csv")
    #     rm("supported_tickers.csv")
    #     println("File 'supported_tickers.csv' has been deleted.")
    # else
    #     println("File 'supported_tickers.csv' does not exist.")
    # end    
    
    @info "Downloaded and processed the latest tickers from Tiingo"
end

"""
    generate_filtered_tickers(duckdb_path::String)

Generate a filtered list of US tickers.
"""
function generate_filtered_tickers(;
    duckdb_path::String = "tiingo_historical_data.duckdb"
)
    # Connect to the duckdb database
    conn = DBInterface.connect(DuckDB.DB, duckdb_path)

    # Filter the table to only include US tickers
    DBInterface.execute(conn, """
    CREATE OR REPLACE TABLE 'us_tickers_filtered' AS
    SELECT * FROM us_tickers
    WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
    AND endDate = (SELECT max(endDate) FROM us_tickers)
    AND assetType IN ('Stock', 'ETF')
    AND ticker NOT LIKE '%/%'
    """);

    # Close the connection
    DBInterface.close(conn)

    @info "Generated filtered list of US tickers"
end
