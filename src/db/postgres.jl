module Postgres
    using LibPQ
    using DuckDB
    using DBInterface
    using DataFrames
    using Logging

    using ..Config
    using ..Core: DuckDBConnection, validate_identifier, validate_file_path
    using ..Schema: create_or_replace_table, generate_create_table_query

    const PostgreSQLConnection = LibPQ.Connection

    """
        connect_postgres(connection_string::String; timeout_seconds::Int=30, max_retries::Int=3, retry_delay::Int=5)

    Connect to the PostgreSQL database with retry logic and timeout.
    """
    function connect_postgres(connection_string::String;
                             timeout_seconds::Int=30,
                             max_retries::Int=3,
                             retry_delay::Int=5)::PostgreSQLConnection
        last_error = nothing

        for attempt in 1:max_retries
            try
                # Add timeout to connection string if not already present
                conn_str = if !contains(connection_string, "connect_timeout")
                    sep = contains(connection_string, "?") ? "&" : "?"
                    "$connection_string$(sep)connect_timeout=$timeout_seconds"
                else
                    connection_string
                end

                conn = LibPQ.Connection(conn_str)

                # Test connection with simple query
                result = execute(conn, "SELECT 1")
                close(result)

                @info "Connected to PostgreSQL successfully" attempt=attempt
                return conn
            catch e
                last_error = e
                @warn "PostgreSQL connection attempt $attempt/$max_retries failed" exception=(e, catch_backtrace())

                if attempt < max_retries
                    @info "Retrying in $retry_delay seconds..."
                    sleep(retry_delay)
                end
            end
        end

        @error "Failed to connect to PostgreSQL after $max_retries attempts"
        throw(last_error)
    end

    """
        close_postgres(conn::PostgreSQLConnection)

    Close the PostgreSQL database connection.
    """
    close_postgres(conn::PostgreSQLConnection) = LibPQ.close(conn)

    """
        export_to_postgres(duckdb_conn::DuckDBConnection, pg_conn::PostgreSQLConnection, tables::Vector{String}; pg_host::String="127.0.0.1", pg_user::String="otwn", pg_dbname::String="tiingo")

    Export tables from DuckDB to PostgreSQL.
    """
    function export_to_postgres(
        duckdb_conn::DuckDBConnection,
        pg_conn::PostgreSQLConnection,
        tables::Vector{String};
        parquet_file::String="historical_data.parquet",
        pg_host::String="127.0.0.1",
        pg_user::String="otwn",
        pg_dbname::String="tiingo",
        max_retries::Int=3,
        retry_delay::Int=5,
        use_dataframe::Union{Bool, Nothing}=nothing,
        max_rows_for_dataframe::Int = 1_000_000
    )
        for table_name in tables
            retry_with_exponential_backoff(max_retries, retry_delay) do
                export_table_to_postgres(
                    duckdb_conn, pg_conn, table_name, parquet_file, pg_host, pg_user, pg_dbname,
                    use_dataframe=use_dataframe, max_rows_for_dataframe=max_rows_for_dataframe
                )
                @info "Successfully exported $table_name from DuckDB to PostgreSQL"
            end
        end
    end

    # Helper function for retrying with exponential backoff
    function retry_with_exponential_backoff(f::Function, max_retries::Int, initial_delay::Int)
        for attempt in 1:max_retries
            try
                return f()
            catch e
                if attempt == max_retries
                    @error "Failed after $max_retries attempts" exception=(e, catch_backtrace())
                    rethrow(e)
                end
                delay = initial_delay * 2^(attempt - 1)
                @warn "Attempt $attempt failed. Retrying in $delay seconds..." exception=(e, catch_backtrace())
                sleep(delay)
            end
        end
    end

    """
        export_table_to_postgres(duckdb_conn::DuckDBConnection, pg_conn::PostgreSQLConnection, table_name::String, pg_host::String, pg_user::String, pg_dbname::String)

    Export a single table from DuckDB to PostgreSQL.
    """
    function export_table_to_postgres(
        duckdb_conn::DuckDBConnection,
        pg_conn::PostgreSQLConnection,
        table_name::String,
        parquet_file::String,
        pg_host::String,
        pg_user::String,
        pg_dbname::String;
        use_dataframe::Union{Bool, Nothing}=nothing,
        max_rows_for_dataframe::Int = 1_000_000
    )
        safe_name = validate_identifier(table_name)
        @info "Exporting table $safe_name to PostgreSQL"

        # Check if the table exists in DuckDB
        table_exists = DBInterface.execute(
            duckdb_conn,
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?;
            """,
            [safe_name]
        ) |> DataFrame

        if isempty(table_exists)
            error("Table $safe_name does not exist in DuckDB")
        end

        # Get row count
        row_count = DBInterface.execute(duckdb_conn, "SELECT COUNT(*) FROM $safe_name") |> DataFrame
        row_count = row_count[1, 1]

        # Determine whether to use DataFrame or Parquet
        use_df = if isnothing(use_dataframe)
            row_count <= max_rows_for_dataframe
        else
            use_dataframe
        end

        if use_df
            export_table_to_postgres_dataframe(duckdb_conn, pg_conn, table_name, pg_host, pg_user, pg_dbname)
        else
            export_table_to_postgres_parquet(duckdb_conn, pg_conn, table_name, parquet_file, pg_host, pg_user, pg_dbname)
        end
    end

    function export_table_to_postgres_dataframe(
        duckdb_conn::DuckDBConnection,
        pg_conn::PostgreSQLConnection,
        table_name::String,
        pg_host::String,
        pg_user::String,
        pg_dbname::String
    )
        safe_name = validate_identifier(table_name)
        @info "Exporting table $safe_name to PostgreSQL using DataFrames"

        try
            # Read the entire table into a DataFrame
            df = DBInterface.execute(duckdb_conn, "SELECT * FROM $safe_name") |> DataFrame
            @info "Loaded $safe_name into DataFrame with $(nrow(df)) rows"

            # Get the schema and create table
            schema = DBInterface.execute(duckdb_conn, "DESCRIBE $safe_name") |> DataFrame
            create_table_query = generate_create_table_query(safe_name, schema)
            create_or_replace_table(pg_conn, safe_name, create_table_query)

            # Insert data into PostgreSQL
            columns = join(lowercase.(names(df)), ", ")
            placeholders = join([string('$', i) for i in 1:ncol(df)], ", ")
            insert_query = "INSERT INTO $safe_name ($columns) VALUES ($placeholders)"

            LibPQ.load!(
                (col => df[!, col] for col in names(df)),
                pg_conn,
                insert_query
            )

            @info "Inserted $(nrow(df)) rows into PostgreSQL table $safe_name"

        catch e
            @error "Error exporting table $safe_name using DataFrames" exception=(e, catch_backtrace())
            rethrow(e)
        end
    end

    function export_table_to_postgres_parquet(
        duckdb_conn::DuckDBConnection,
        pg_conn::PostgreSQLConnection,
        table_name::String,
        parquet_file::String,
        pg_host::String,
        pg_user::String,
        pg_dbname::String
    )
        safe_name = validate_identifier(table_name)
        safe_parquet = validate_file_path(parquet_file)
        @info "Exporting table $safe_name to PostgreSQL using Parquet"

        try
            # Export to parquet
            DBInterface.execute(duckdb_conn, """COPY $safe_name TO '$safe_parquet';""")
            @info "Exported $safe_name to parquet file"

            # Get the schema and create table
            safe_name_lower = lowercase(safe_name)
            schema = DBInterface.execute(duckdb_conn, "DESCRIBE $safe_name_lower") |> DataFrame
            create_table_query = generate_create_table_query(safe_name_lower, schema)
            create_or_replace_table(pg_conn, safe_name_lower, create_table_query)

            # Copy data from parquet to PostgreSQL
            setup_postgres_connection(duckdb_conn, pg_host, pg_user, pg_dbname)
            DBInterface.execute(
                duckdb_conn,
                """COPY postgres_db.$safe_name FROM '$safe_parquet';"""
            )
            DBInterface.execute(duckdb_conn, "DETACH postgres_db;")
            @info "Copied data from parquet file to PostgreSQL table $safe_name"
        catch e
            @error "Error exporting table $safe_name using Parquet" exception=(e, catch_backtrace())
            rethrow(e)
        finally
            # if isfile(parquet_file)
            #     rm(parquet_file)
            #     @info "Removed temporary parquet file for $table_name"
            # end
            try
                DBInterface.execute(duckdb_conn, "DETACH postgres_db;")
            catch
            end
        end
    end

    """
        setup_postgres_connection(duckdb_conn::DuckDBConnection, pg_host::String, pg_user::String, pg_dbname::String)

    Set up a PostgreSQL connection in DuckDB.
    """
    function setup_postgres_connection(
        duckdb_conn::DuckDBConnection,
        pg_host::String,
        pg_user::String,
        pg_dbname::String
    )
        try
            DBInterface.execute(duckdb_conn, "INSTALL postgres;")
            DBInterface.execute(duckdb_conn, "LOAD postgres;")
            DBInterface.execute(duckdb_conn, """
                ATTACH 'dbname=$pg_dbname user=$pg_user host=$pg_host' AS postgres_db (TYPE postgres);
            """)
            @info "Successfully set up PostgreSQL connection in DuckDB"
        catch e
            @error "Failed to set up PostgreSQL connection in DuckDB" exception=(e, catch_backtrace())
            rethrow(e)
        end
    end

    export PostgreSQLConnection
    export connect_postgres, close_postgres, export_to_postgres
    export export_table_to_postgres, export_table_to_postgres_dataframe, export_table_to_postgres_parquet
    export setup_postgres_connection
end
