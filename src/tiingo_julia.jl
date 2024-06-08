module tiingo_julia

using DataFrames
using JSON3
using HTTP
using LibPQ
using DotEnv

export fetch_and_store_data
export download_from_tiingo

include("api.jl")
include("db.jl")

end # end of module
