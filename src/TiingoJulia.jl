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

# Include configuration module first
include("config.jl")
using .Config

# Include database modules
module DB
    using DataFrames
    using DBInterface
    using DuckDB
    using LibPQ
    using Logging
    using Base: @kwdef
    
    # Import parent module's Config
    using ..Config
    
    # Include all DB submodules
    include("db/core.jl")
    using .Core
    
    include("db/schema.jl")
    using .Schema
    
    include("db/operations.jl")
    using .Operations
    
    include("db/postgres.jl")
    using .Postgres
    
    # Re-export core database functionality
    export connect_duckdb, close_duckdb, optimize_database
    export create_tables, create_indexes, list_tables
    export upsert_stock_data, upsert_stock_data_bulk
    export get_tickers_all, get_tickers_etf, get_tickers_stock
    export connect_postgres, close_postgres, export_to_postgres
    export create_or_replace_table
    
    
    # Custom error types and aliases are imported from submodules
    
    # Global reference to log file handle for cleanup
    const TIINGO_LOG_FILE_HANDLE = Ref{Union{IO,Nothing}}(nothing)
    const LOG_FILE = Config.DB.LOG_FILE
    
    # Set up logging to file
    function setup_logging()
        io = open(LOG_FILE, "a")
        TIINGO_LOG_FILE_HANDLE[] = io
        logger = SimpleLogger(io)
        global_logger(logger)
    end
    
    # Cleanup logging resources
    function cleanup_logging()
        if TIINGO_LOG_FILE_HANDLE[] !== nothing
            try
                close(TIINGO_LOG_FILE_HANDLE[])
                TIINGO_LOG_FILE_HANDLE[] = nothing
                @info "TiingoJulia log file handle closed successfully"
            catch e
                @warn "Error closing TiingoJulia log file" exception=e
            end
        end
    end
    
    # Register cleanup to run on exit
    atexit(cleanup_logging)
    
    # Wrapper functions that call Schema.create_tables with proper connection
    function connect_duckdb(path::String = Config.DB.DEFAULT_DUCKDB_PATH)
        conn = Core.connect_duckdb(path)
        Schema.create_tables(conn)
        return conn
    end
    
    # Re-export all functions from submodules
    for name in names(Core, all=false)
        name == :Core && continue
        @eval export $name
    end
    for name in names(Schema, all=false)
        name == :Schema && continue
        @eval export $name
    end
    for name in names(Operations, all=false)
        name == :Operations && continue
        @eval export $name
    end
    for name in names(Postgres, all=false)
        name == :Postgres && continue
        @eval export $name
    end
end

using .DB

# Include API module
module API
    using HTTP
    using JSON3
    using DataFrames
    using TimeSeries
    using Dates
    using DotEnv
    
    using ..Config
    
    include("api.jl")
    
    export get_api_key, get_ticker_data, fetch_api_data, load_env_file
end

using .API

# Include Sync module for data synchronization
module Sync
    using Dates
    using DataFrames
    using DBInterface
    using DuckDB
    using Logging
    using ZipFile
    using HTTP
    
    using ..Config
    using ..DB
    using ..API
    
    include("sync.jl")
    
    export download_tickers_duckdb, download_latest_tickers
    export process_tickers_csv, generate_filtered_tickers
    export update_historical, update_historical_parallel, update_historical_sequential
    export update_split_ticker, add_historical_data, update_us_tickers
end

using .Sync

# Include fundamental data module
include("fundamental.jl")

# Export all public functions to maintain backward compatibility
export get_api_key
export get_ticker_data
export download_tickers_duckdb, download_latest_tickers, process_tickers_csv, generate_filtered_tickers
export connect_duckdb, close_duckdb, update_us_tickers
export upsert_stock_data, upsert_stock_data_bulk
export add_historical_data, update_historical, update_historical_parallel, update_historical_sequential, update_split_ticker
export get_tickers_all, get_tickers_etf, get_tickers_stock
export connect_postgres, close_postgres, export_to_postgres
export list_tables
export get_daily_fundamental
export create_or_replace_table, create_tables, create_indexes, optimize_database
# Export types and errors
export DatabaseConnectionError, DatabaseQueryError, DuckDBConnection, PostgreSQLConnection

end # module TiingoJulia

