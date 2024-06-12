module tiingo_julia

using DataFrames
using JSON3
using HTTP
using LibPQ
using DotEnv
using Logging
using LoggingExtras

export fetch_ticker_data
export store_data, update_splitted_ticker, get_tickers_all, upsert_stock_data, add_historical_data, update_historical

include("api.jl")
include("db.jl")

end # end of module
