using HTTP, JSON3, DataFrames, Dates

function download_from_tiingo(ticker, start_date, end_date)
    api_key = ENV["TIINGO_TOKEN"]

    url = "https://api.tiingo.com/tiingo/daily/$ticker/prices"

    if start_date isnothing
        start_date = "1970-01-01"
    end
    if end_date isnothing
        end_date = Dates.today()
    end

    query_params = Dict(
        "startDate" => start_date,
        "endDate" => end_date,
        "token" => api_key
    )
    response = HTTP.get(url, queryparams=query_params)

    if response.status == 200
        data = JSON3.parse(response.body)
        if isempty(data)
            throw("No data receieved for ticker $ticker")
        else
            return DataFrame(data)
        end
    else
        error("API request failed")
    end
end