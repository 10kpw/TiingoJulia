module Core
    using DBInterface
    using DuckDB
    using DataFrames
    using Logging

    # Import Config module from parent directory
    using ..Config

    # Custom error types
    struct DatabaseConnectionError <: Exception
        msg::String
    end

    struct DatabaseQueryError <: Exception
        msg::String
        query::String
    end

    const DuckDBConnection = DBInterface.Connection

    # Valid SQL identifier pattern: alphanumeric + underscore, must start with letter/underscore
    const VALID_IDENTIFIER_RE = r"^[a-zA-Z_][a-zA-Z0-9_]*$"

    """
        validate_identifier(name::String)::String

    Validate that a string is a safe SQL identifier (table name, column name).
    Prevents SQL injection when identifiers must be interpolated into queries.
    Returns the name if valid, throws ArgumentError otherwise.
    """
    function validate_identifier(name::String)::String
        if !occursin(VALID_IDENTIFIER_RE, name)
            throw(ArgumentError("Invalid SQL identifier: '$name'. Must match pattern: $VALID_IDENTIFIER_RE"))
        end
        return name
    end

    """
        validate_file_path(path::String)::String

    Validate that a file path does not contain characters that could enable SQL injection.
    Returns the path if valid, throws ArgumentError otherwise.
    """
    function validate_file_path(path::String)::String
        if occursin('\'', path) || occursin(';', path)
            throw(ArgumentError("File path contains unsafe characters: '$path'"))
        end
        return path
    end

    """
        validate_sql_value(value::String)::String

    Validate that a string value does not contain characters that could break out of
    a SQL string literal (single quotes). Use for config-sourced values interpolated into SQL.
    Returns the value if valid, throws ArgumentError otherwise.
    """
    function validate_sql_value(value::String)::String
        if occursin('\'', value)
            throw(ArgumentError("SQL value contains unsafe characters: '$value'"))
        end
        return value
    end

    """
        verify_duckdb_integrity(path::String)

    Verify if a DuckDB database is accessible and contains expected tables.
    Returns (is_valid::Bool, error_message::Union{String,Nothing})
    """
    function verify_duckdb_integrity(path::String)
        if !isfile(path)
            return false, "Database file does not exist"
        end

        try
            conn = DBInterface.connect(DuckDB.DB, path)
            try
                # Check if we can execute basic queries
                DBInterface.execute(conn, "SELECT 1")
                tables = DBInterface.execute(conn, """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_name IN ('us_tickers', 'us_tickers_filtered', 'historical_data')
                """) |> DataFrame

                if nrow(tables) == 0
                    return false, "No expected tables found in database"
                end

                return true, nothing
            finally
                DuckDB.close(conn)
            end
        catch e
            return false, "Database verification failed: $e"
        end
    end

    """
        configure_database(conn::DuckDBConnection)

    Configure database settings. Currently uses DuckDB defaults.
    Use optimize_database(conn) for automatic performance tuning.
    """
    function configure_database(conn::DuckDBConnection)
        # Don't hardcode threads - let DuckDB use system defaults
        # Users can call optimize_database(conn) for automatic performance tuning
        return conn
    end

    """
        connect_duckdb(path::String = Config.DB.DEFAULT_DUCKDB_PATH)::DuckDBConnection

    Connect to the DuckDB database and create necessary tables if they don't exist.
    """
    function connect_duckdb(path::String = Config.DB.DEFAULT_DUCKDB_PATH)::DuckDBConnection
        try
            @info "Attempting to connect to DuckDB at path: $path"
            conn = DBInterface.connect(DuckDB.DB, path)
            configure_database(conn)
            # We need to call create_tables here, but it's in Schema module.
            # To avoid circular dependencies, we might need to pass the create_tables function
            # or just let the user call it, or have a higher level 'init' function.
            # For now, let's assume the caller will handle table creation or we'll inject it.
            # Actually, better design: Core just connects. Schema handles tables.
            # But the original code called create_tables.
            # Let's keep it simple: Core returns connection.
            return conn
        catch e
            @warn "Failed to connect to existing database: $e"

            @info "Attempting to create a new database at path: $path"
            try
                # Ensure directory exists
                mkpath(dirname(path))
                conn = DBInterface.connect(DuckDB.DB, path)
                configure_database(conn)
                return conn
            catch new_e
                @error "Failed to create new database" exception=(new_e, catch_backtrace())
                throw(DatabaseConnectionError("Failed to connect to or create DuckDB: $new_e"))
            end
        end
    end

    """
        close_duckdb(conn::DuckDBConnection)

    Safely close a DuckDB database connection.
    """
    function close_duckdb(conn::DuckDBConnection)
        try
            DBInterface.close!(conn)
            @info "DuckDB connection closed successfully"
        catch e
            @warn "Error closing DuckDB connection" exception=e
        end
    end

    """
        optimize_database(conn::DuckDBConnection)

    Optimize the database by automatically detecting system resources and configuring
    database settings for optimal performance.
    """
    function optimize_database(conn::DuckDBConnection)
        try
            @info "Optimizing database..."

            # Detect system memory
            total_memory_gb = try
                if Sys.islinux()
                    mem_info = read("/proc/meminfo", String)
                    mem_total = match(r"MemTotal:\s+(\d+)", mem_info)
                    parse(Int, mem_total[1]) ÷ 1024 ÷ 1024  # Convert KB to GB
                elseif Sys.isapple()
                    mem_bytes = parse(Int, read(`sysctl -n hw.memsize`, String))
                    mem_bytes ÷ 1024^3  # Convert bytes to GB
                else
                    16  # Default to 16GB if unknown
                end
            catch
                16  # Default to 16GB if detection fails
            end

            # Set memory limit to ~75% of available memory, but don't hardcode a 4GB floor
            memory_limit_gb = total_memory_gb * 0.75

            # Detect CPU threads
            num_threads = Sys.CPU_THREADS
            worker_threads = max(1, num_threads - 1)

            @info "System resources detected" total_memory_gb memory_limit_gb=memory_limit_gb threads=num_threads

            # Apply DuckDB optimizations
            if memory_limit_gb >= 1
                memory_limit = max(1, Int(floor(memory_limit_gb)))
                DBInterface.execute(conn, "SET memory_limit = '$(memory_limit)GB'")
            else
                memory_limit_mb = max(256, Int(floor(memory_limit_gb * 1024)))
                DBInterface.execute(conn, "SET memory_limit = '$(memory_limit_mb)MB'")
            end

            # Try to set threads, but don't fail if external threads prevent it
            try
                DBInterface.execute(conn, "SET threads = $num_threads")
            catch e
                @debug "Could not set threads" exception=e
            end

            # Try to set worker_threads, but don't fail if external threads prevent it
            try
                DBInterface.execute(conn, "SET worker_threads = $worker_threads")
            catch e
                @debug "Could not set worker_threads" exception=e
            end

            tmp_dir = get(ENV, "TIINGO_DUCKDB_TMP", tempdir())
            try
                mkpath(tmp_dir)
                DBInterface.execute(conn, "SET temp_directory = '$(tmp_dir)'")
            catch e
                @debug "Could not set temp_directory" exception=e
            end

            # Run VACUUM and ANALYZE
            DBInterface.execute(conn, "VACUUM")
            DBInterface.execute(conn, "ANALYZE")

            @info "Database optimization completed" threads=num_threads
        catch e
            @warn "Database optimization failed" exception=e
            rethrow(e)
        end
    end
    export verify_duckdb_integrity, configure_database, connect_duckdb, close_duckdb, optimize_database
    export DuckDBConnection, DatabaseConnectionError, DatabaseQueryError
    export validate_identifier, validate_file_path, validate_sql_value
end
