const BASE_URL = "https://api.tiingo.com/tiingo/daily"

function fetch_ticker_data(
    ticker::String;
    start_date::String="1970-01-01", 
    end_date::String=Dates.today()
)::DataFrame

    cfg = DotEnv.config()
    api_key = cfg["TIINGO_API_KEY"]
    @assert api_key !== nothing

    url = "$BASE_URL/$ticker/prices"
    query_params = Dict(
        "startDate" => start_date,
        "endDate" => end_date,
        "token" => api_key
    )

    response = HTTP.get(url, queryparams=query_params)

    @assert response.status == 200 

    data = JSON3.parse(String(response.body))
    @assert !isempty(data) 

    return DataFrame(data)
end
