#!/usr/bin/env julia

const TOML_PKG_ID = Base.PkgId(Base.UUID("fa267f1f-6049-4f14-aa54-33bafae1ed76"), "TOML")

function fail(msg::String)
    println(stderr, "ERROR: " * msg)
    exit(1)
end

function get_nested(config::Dict{String,Any}, keys::Vector{String})
    current = config
    for (i, key) in enumerate(keys)
        if !(current isa Dict{String,Any}) || !haskey(current, key)
            fail("Missing configuration key: " * join(keys[1:i], "."))
        end
        current = current[key]
    end
    return current
end

function validate_config(path::String)
    if !isfile(path)
        fail("Required config file is missing: $path")
    end

    config = try
        toml = Base.require(TOML_PKG_ID)
        Base.invokelatest(getproperty(toml, :parsefile), path)
    catch e
        fail("Failed to parse $path: $e")
    end

    required_string_keys = [
        ["api", "base_url"],
        ["api", "tickers_url"],
        ["files", "env_file"],
        ["files", "zip_file"],
        ["files", "csv_file"],
        ["environment", "api_key_name"],
    ]

    for key_path in required_string_keys
        value = get_nested(config, key_path)
        if !(value isa String) || isempty(strip(value))
            fail("Expected non-empty string for key: " * join(key_path, "."))
        end
    end

    for key_path in ([
        ["filtering", "supported_exchanges"],
        ["filtering", "supported_asset_types"],
    ])
        value = get_nested(config, key_path)
        if !(value isa AbstractVector) || isempty(value)
            fail("Expected non-empty array for key: " * join(key_path, "."))
        end
    end

    max_retries = get_nested(config, ["api", "max_retries"])
    retry_delay = get_nested(config, ["api", "retry_delay"])
    if !(max_retries isa Integer) || max_retries < 1
        fail("api.max_retries must be an integer >= 1")
    end
    if !(retry_delay isa Integer) || retry_delay < 0
        fail("api.retry_delay must be an integer >= 0")
    end
end

function validate_tracked_env_files()
    tracked_files = try
        split(chomp(read(`git ls-files`, String)), '\n')
    catch e
        fail("Unable to list tracked files with git: $e")
    end

    bad_files = String[]
    for file in tracked_files
        isempty(file) && continue
        is_env_like = occursin(r"(^|/)\.env($|\.)", file)
        is_example = endswith(file, ".env.example")
        if is_env_like && !is_example
            push!(bad_files, file)
        end
    end

    if !isempty(bad_files)
        fail("Secret env files are tracked in git: " * join(bad_files, ", "))
    end
end

function main()
    validate_tracked_env_files()
    validate_config("config.toml")
    validate_config("config.example.toml")
    println("Release hygiene checks passed.")
end

main()
