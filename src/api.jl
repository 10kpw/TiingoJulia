const BASE_URL = "https://api.tiingo.com/tiingo/daily"

"""
    fetch_ticker_data(ticker::String; startDate=nothing, endDate=nothing)

Fetch historical data for a given ticker from Tiingo API.
"""
function fetch_ticker_data(
    ticker::String;
    startDate::Union{Date, String}=nothing,
    endDate::Union{Date, String}=nothing
)::DataFrame
    # load tiingo api key
    api_key = ENV["TIINGO_API_KEY"]
    @assert !isnothing(api_key) "TIINGO_API_KEY not found in environment variables"

    headers = Dict("Authorization" => "Token $api_key")

    # IF endDate is nothing, fetch the metadata to get the endDate
    if isnothing(endDate)
        meta = "$BASE_URL/$ticker"
        meta_resp = HTTP.get(meta, headers=headers)
        @assert meta_resp.status == 200 "Failed to fetch metadata for $ticker"
        meta_data = JSON3.read(String(meta_resp.body))
        end_date = meta_data["endDate"]
        start_date = isnothing(startDate) ? meta_data["startDate"] : startDate
    end
    
    start_date = Dates.format.(Date(startDate), "yyyy-mm-dd")
    end_date = Dates.format.(Date(endDate), "yyyy-mm-dd")

    url = "$BASE_URL/$ticker/prices"
    query_params = Dict(
        "startDate" => start_date,
        "endDate" => end_date,
        "token" => api_key
    )
    @info "Fetching data for $ticker from $start_date to $end_date"
    response = HTTP.get(url, queryparams=query_params, headers=headers)
    @assert response.status == 200 "Failed to fetch data for $ticker from $start_date to $end_date"

    # data = JSON3.parse(String(response.body))
    data = JSON3.read(String(response.body), Vector{Dict})
    @assert !isempty(data) "No data found for $ticker from $start_date to $end_date"

    return DataFrame(data)
end

"""
    download_latest_tickers(tickers_url::String, duckdb_path::String)

Download and process the latest tickers from Tiingo.
"""
function download_latest_tickers(
    tickers_url::String="https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip";
    duckdb_path::String = "tiingo_historical_data.duckdb" 
)
    zip_file_path = "supported_tickers.zip"
    HTTP.download(tickers_url, zip_file_path)

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
