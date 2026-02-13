module Config
    using Logging
    using JSON3

    const TOML_PKG_ID = Base.PkgId(Base.UUID("fa267f1f-6049-4f14-aa54-33bafae1ed76"), "TOML")

    const CONFIG_FILE_TOML = "config.toml"
    const CONFIG_FILE_JSON = "config.json"

    function parse_env_int(name::String, default::Int)::Int
        raw = get(ENV, name, "")
        if isempty(strip(raw))
            return default
        end
        try
            return parse(Int, raw)
        catch
            @warn "Invalid integer environment variable, using default" var=name value=raw default
            return default
        end
    end

    function parse_env_list(name::String, default)::Vector{String}
        raw = get(ENV, name, "")
        if isempty(strip(raw))
            return [String(item) for item in default]
        end
        return [strip(item) for item in split(raw, ",") if !isempty(strip(item))]
    end

    function cfg_get(config, keys::Vector{String})
        current = config
        for key in keys
            if current isa AbstractDict
                if haskey(current, key)
                    current = current[key]
                elseif haskey(current, Symbol(key))
                    current = current[Symbol(key)]
                else
                    error("Missing configuration key: " * join(keys, "."))
                end
            else
                sym = Symbol(key)
                if hasproperty(current, sym)
                    current = getproperty(current, sym)
                else
                    error("Missing configuration key: " * join(keys, "."))
                end
            end
        end
        return current
    end

    function resolve_config_path()::String
        env_path = get(ENV, "TIINGO_CONFIG_PATH", "")
        if !isempty(strip(env_path))
            return abspath(env_path)
        end

        package_toml = joinpath(dirname(@__DIR__), CONFIG_FILE_TOML)
        if isfile(package_toml)
            return package_toml
        end

        pwd_toml = joinpath(pwd(), CONFIG_FILE_TOML)
        if isfile(pwd_toml)
            return pwd_toml
        end

        package_json = joinpath(dirname(@__DIR__), CONFIG_FILE_JSON)
        if isfile(package_json)
            @warn "Using legacy JSON config; migrate to TOML for future releases" path=package_json
            return package_json
        end

        pwd_json = joinpath(pwd(), CONFIG_FILE_JSON)
        if isfile(pwd_json)
            @warn "Using legacy JSON config; migrate to TOML for future releases" path=pwd_json
            return pwd_json
        end

        error(
            "Config file not found. Checked: " *
            joinpath(dirname(@__DIR__), CONFIG_FILE_TOML) * ", " *
            joinpath(pwd(), CONFIG_FILE_TOML) * ", " *
            joinpath(dirname(@__DIR__), CONFIG_FILE_JSON) * ", " *
            joinpath(pwd(), CONFIG_FILE_JSON)
        )
    end

    function load_config()
        config_path = resolve_config_path()
        if !isfile(config_path)
            error("Config file not found at: $config_path")
        end
        if endswith(lowercase(config_path), ".toml")
            toml = Base.require(TOML_PKG_ID)
            return Base.invokelatest(getproperty(toml, :parsefile), config_path)
        end
        return JSON3.read(read(config_path, String))
    end

    const CONFIG = load_config()

    module API
        import ..CONFIG, ..cfg_get, ..parse_env_int
        const ENV_FILE = get(ENV, "TIINGO_ENV_FILE", String(cfg_get(CONFIG, ["files", "env_file"])))
        const API_KEY_NAME = get(ENV, "TIINGO_API_KEY_NAME", String(cfg_get(CONFIG, ["environment", "api_key_name"])))
        const BASE_URL = get(ENV, "TIINGO_API_BASE_URL", String(cfg_get(CONFIG, ["api", "base_url"])))
        const TICKERS_URL = get(ENV, "TIINGO_TICKERS_URL", String(cfg_get(CONFIG, ["api", "tickers_url"])))
        const MAX_RETRIES = parse_env_int("TIINGO_API_MAX_RETRIES", Int(cfg_get(CONFIG, ["api", "max_retries"])))
        const RETRY_DELAY = parse_env_int("TIINGO_API_RETRY_DELAY", Int(cfg_get(CONFIG, ["api", "retry_delay"])))
    end

    module DB
        import ..CONFIG, ..cfg_get
        const DEFAULT_DUCKDB_PATH = get(ENV, "TIINGO_DB_PATH", "tiingo_historical_data.duckdb")
        const DEFAULT_CSV_FILE = get(ENV, "TIINGO_TICKERS_CSV", String(cfg_get(CONFIG, ["files", "csv_file"])))
        const ZIP_FILE_PATH = get(ENV, "TIINGO_TICKERS_ZIP", String(cfg_get(CONFIG, ["files", "zip_file"])))
        const LOG_FILE = get(ENV, "TIINGO_LOG_FILE", "stock.log")

        module Tables
            const US_TICKERS = "us_tickers"
            const US_TICKERS_FILTERED = "us_tickers_filtered"
            const HISTORICAL_DATA = "historical_data"
        end
    end

    module Filtering
        import ..CONFIG, ..cfg_get, ..parse_env_list
        const SUPPORTED_EXCHANGES = parse_env_list("TIINGO_SUPPORTED_EXCHANGES", cfg_get(CONFIG, ["filtering", "supported_exchanges"]))
        const SUPPORTED_ASSET_TYPES = parse_env_list("TIINGO_SUPPORTED_ASSET_TYPES", cfg_get(CONFIG, ["filtering", "supported_asset_types"]))
    end
end
