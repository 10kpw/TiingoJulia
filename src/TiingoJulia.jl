module TiingoJulia

using CSV
using DataFrames
using Dates
using DBInterface
using DotEnv
using DuckDB
using HTTP
using JSON3
using LibPQ
using Tables
using ZipFile
using Logging
using LoggingExtras
using TimeSeries

# Logger configuration
const CONSOLE_LOGGER = ConsoleLogger(stderr, Logging.Info)
const NULL_LOGGER = LoggingExtras.NullLogger()

function __init__()
    logger_type = get(ENV, "TIINGO_LOGGER", "null")
    if logger_type == "console"
        global_logger(CONSOLE_LOGGER)
    elseif logger_type == "tee"
        global_logger(LoggingExtras.TeeLogger(NULL_LOGGER, CONSOLE_LOGGER))
    else
        global_logger(NULL_LOGGER)
    end
end

# Include submodules
include("api.jl")
include("db.jl")
include("fundamental.jl")

# Export functions
export get_api_key
export get_ticker_data,
    download_tickers_duckdb,
    download_latest_tickers,
    process_tickers_csv,
    generate_filtered_tickers
export connect_duckdb,
    close_duckdb, update_us_tickers, upsert_stock_data, upsert_stock_data_bulk
export add_historical_data,
    update_historical,
    update_historical_parallel,
    update_historical_sequential,
    update_split_ticker
export get_tickers_all, get_tickers_etf, get_tickers_stock
export connect_postgres, close_postgres, export_to_postgres
export list_tables
export get_daily_fundamental
export create_or_replace_table, create_tables, create_indexes, optimize_database


end # module TiingoJulia
