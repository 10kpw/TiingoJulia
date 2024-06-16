const BASE_URL = "https://api.tiingo.com/tiingo/daily"

function fetch_ticker_data(
    ticker::String;
    start_date::String=nothing,
    end_date::String=nothing
)::DataFrame
    # load tiingo api key
    cfg = DotEnv.config()
    api_key = cfg["TIINGO_API_KEY"]
    @assert api_key !== nothing

    headers = Dict("Authorization" => "Token $api_key")
    if endDate === nothing
        meta = "$BASE_URL/$ticker"
        meta_resp = HTTP.get(meta, headers=headers)
        @assert meta_resp.status == 200
        meta_data = JSON3.read(String(meta_resp.body))
        end_date = meta_data["endDate"]
        if startDate === nothing
            start_date = meta_data["startDate"]
        end
    end

    url = "$BASE_URL/$ticker/prices"
    query_params = Dict(
        "startDate" => start_date,
        "endDate" => end_date,
        "token" => api_key
    )
    response = HTTP.get(url, queryparams=query_params, headers=headers)
    @assert response.status == 200 

    data = JSON3.parse(String(response.body))
    @assert !isempty(data) 

    return DataFrame(data)
end


function download_latest_tickers(
    tickers_url::String="https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip";
    duckdb_path::String = "tiingo_historical_data.duckdb" 
)::DataFrame
    # Download the zip file
    response = HTTP.get(tickers_url);
    zip_file_path = "supported_tickers.zip"
    open(zip_file_path, "w") do file
        write(file, response.body)
    end
    # Unzip the file
    r = ZipFile.Reader(zip_file_path)
    for f in r.files
        out_path = joinpath(pwd(), f.name)
        open(out_path, "w") do out_file
            write(out_file, read(f, String))
        end
    end
    close(r)

    # Connect to the duckdb database
    conn = DBInterface.connect(DuckDB.DB, duckdb_path)

    # Create a table from the csv file
    DBInterface.execute(conn, """
    CREATE OR REPLACE TABLE us_tickers AS
    SELECT * FROM read_csv("supported_tickers.csv")
    """);

    # Filter the table to only include US tickers
    DBInterface.execute(conn,
    """
    CREATE OR REPLACE TABLE 'us_tickers_filtered' AS
    SELECT * FROM us_tickers
    WHERE exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'AMEX', 'ASX')
    AND endDate = (SELECT max(endDate) FROM us_tickers)
    AND assetType IN ('Stock', 'ETF')
    AND ticker NOT LIKE '%/%'
    """);

    # Load the filtered data into a DataFrame
    df = DBInterface.execute(conn, """
    SELECT ticker, exchange, assetType, startDate, endDate FROM us_tickers_filtered
    """) |> DataFrame
    # Close the connection
    DBInterface.close(conn)

    return df
end


