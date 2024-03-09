module tiingo_julia

<<<<<<< HEAD
using DataFrames
using JSON3
using HTTP
using LibPQ
using DotEnv

export fetch_and_store_data

include("api.jl")
include("db.jl")

function init()
    DotEnv.config()
end

function fetch_and_store_data(ticker::String, start_date::String, end_date:: String, table_name::String)
    df = download_from_tiingo(ticker, start_date, end_date)
    store_data(df, table_name)
end


end # end of module
=======
# Write your package code here.

end
>>>>>>> fc1e21b (initial commit)
