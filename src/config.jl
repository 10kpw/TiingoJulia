module Config
    using DotEnv
    using JSON3

    const CONFIG_FILE = "config.json"

    function load_config()
        # Handle case where this is run from REPL or included
        # We need to find config.json relative to the package root
        # @__DIR__ is src/
        config_path = joinpath(dirname(@__DIR__), CONFIG_FILE)
        if !isfile(config_path)
            # Fallback for when running from test/ or other locations
            # Assuming standard package structure
            current_dir = pwd()
            if isfile(joinpath(current_dir, CONFIG_FILE))
                config_path = joinpath(current_dir, CONFIG_FILE)
            else
                 # Try to find it relative to the module file if @__DIR__ didn't work as expected
                 # or if we are in a different context.
                 # But for now, let's stick to the original logic but robustified
                 error("Config file not found at: $config_path")
            end
        end
        return JSON3.read(read(config_path, String))
    end

    const CONFIG = load_config()
    
    module API
        import ..CONFIG
        const ENV_FILE = CONFIG.files.env_file
        const API_KEY_NAME = CONFIG.environment.api_key_name
        const BASE_URL = CONFIG.api.base_url
        const TICKERS_URL = CONFIG.api.tickers_url
        const MAX_RETRIES = CONFIG.api.max_retries
        const RETRY_DELAY = CONFIG.api.retry_delay
    end

    module DB
        import ..CONFIG
        const DEFAULT_DUCKDB_PATH = "tiingo_historical_data.duckdb"
        const DEFAULT_CSV_FILE = CONFIG.files.csv_file
        const ZIP_FILE_PATH = CONFIG.files.zip_file
        const LOG_FILE = "stock.log"

        module Tables
            const US_TICKERS = "us_tickers"
            const US_TICKERS_FILTERED = "us_tickers_filtered"
            const HISTORICAL_DATA = "historical_data"
        end
    end
    
    module Filtering
        import ..CONFIG
        const SUPPORTED_EXCHANGES = CONFIG.filtering.supported_exchanges
        const SUPPORTED_ASSET_TYPES = CONFIG.filtering.supported_asset_types
    end
end
