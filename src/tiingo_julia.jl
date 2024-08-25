module tiingo_julia

using DataFrames
using JSON3
using HTTP
using LibPQ
using DotEnv
using CSV
using DuckDB
using DBInterface
using ZipFile
using Dates
using Logging
using LoggingExtras

# load tiingo api key
DotEnv.config()
@info "Loaded .env file"

# Export functions
export fetch_ticker_data, download_latest_tickers, generate_filtered_tickers
export connect_db, close_db, update_us_tickers, upsert_stock_data
export add_historical_data, update_historical, update_splitted_ticker
export get_tickers_all, get_tickers_etf, get_tickers_stock

# Export new PostgreSQL functions
export connect_postgres, close_postgres, export_to_postgres, export_all_to_postgres

# Include your API and DB modules
include("api.jl")
include("db.jl")

# Global logger configuration (if needed)
console_logger = LoggingExtras.TeeLogger(LoggingExtras.NullLogger(), ConsoleLogger(stderr, Logging.Info))
global_logger(console_logger)

end # end of module
