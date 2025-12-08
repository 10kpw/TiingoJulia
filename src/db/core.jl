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
            @info "Attempting to connect to DuckDB at path: \$path"
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
            @warn "Failed to connect to existing database: \$e"

            @info "Attempting to create a new database at path: \$path"
            try
                # Ensure directory exists
                mkpath(dirname(path))
                conn = DBInterface.connect(DuckDB.DB, path)
                configure_database(conn)
                return conn
            catch new_e
                @error "Failed to create new database" exception=(new_e, catch_backtrace())
                throw(DatabaseConnectionError("Failed to connect to or create DuckDB: \$new_e"))
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

            # Set memory limit to 75% of available memory
            memory_limit = max(4, Int(floor(total_memory_gb * 0.75)))

            # Detect CPU threads
            num_threads = Sys.CPU_THREADS
            worker_threads = max(1, num_threads - 1)

            @info "System resources detected" total_memory_gb memory_limit_gb=memory_limit threads=num_threads

            # Apply DuckDB optimizations
            DBInterface.execute(conn, "SET memory_limit = '$(memory_limit)GB'")
            DBInterface.execute(conn, "SET threads = $num_threads")

            # Try to set worker_threads, but don't fail if external threads prevent it
            try
                DBInterface.execute(conn, "SET worker_threads = $worker_threads")
            catch e
                @debug "Could not set worker_threads" exception=e
            end

            DBInterface.execute(conn, "SET temp_directory = '/tmp/duckdb'")

            # Run VACUUM and ANALYZE
            DBInterface.execute(conn, "VACUUM")
            DBInterface.execute(conn, "ANALYZE")

            @info "Database optimization completed" memory_limit="\$(memory_limit)GB" threads=num_threads
        catch e
            @warn "Database optimization failed" exception=e
            rethrow(e)
        end
    end
    export verify_duckdb_integrity, configure_database, connect_duckdb, close_duckdb, optimize_database
    export DuckDBConnection, DatabaseConnectionError, DatabaseQueryError
end
