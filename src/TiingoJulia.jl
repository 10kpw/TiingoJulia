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

# Global logger configuration (if needed)
console_logger = LoggingExtras.TeeLogger(LoggingExtras.NullLogger(), ConsoleLogger(stderr, Logging.Info))
global_logger(console_logger)

# Include your API and DB modules
include("api.jl")
include("db.jl")

# Export functions
export get_api_key
export fetch_ticker_data, download_latest_tickers, generate_filtered_tickers
export connect_db, close_db, update_us_tickers, upsert_stock_data
export add_historical_data, update_historical, update_splitted_ticker
export get_tickers_all, get_tickers_etf, get_tickers_stock
export connect_postgres, close_postgres, export_to_postgres
export list_tables

end # end of module
