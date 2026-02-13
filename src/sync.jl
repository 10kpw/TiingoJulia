using ..DB.Core: DuckDBConnection, validate_identifier, validate_file_path, validate_sql_value
using ..DB.Operations
using ..API: get_ticker_data, get_api_key

"""
        download_tickers_duckdb(conn::DuckDBConnection; tickers_url, zip_file_path, csv_file)

    Download and process the latest tickers from Tiingo.
    """
    function download_tickers_duckdb(
        conn::DuckDBConnection;
        tickers_url::String = Config.API.TICKERS_URL,
        zip_file_path::String = Config.DB.ZIP_FILE_PATH,
        csv_file::String = Config.DB.DEFAULT_CSV_FILE
    )
        try
            # Step 1: Download and unzip
            @info "Starting ticker download and processing..."
            download_latest_tickers(tickers_url, zip_file_path, csv_file)
            process_tickers_csv(conn, csv_file)

            # Step 2: Create filtered table and verify
            @info "Generating filtered tickers table..."
            create_filtered_tickers(conn)

            # Step 3: Verify the tables were created and have rows
            us_tickers_count = Operations.get_table_count(conn, Config.DB.Tables.US_TICKERS)
            filtered_count = Operations.get_table_count(conn, Config.DB.Tables.US_TICKERS_FILTERED)

            @info "Ticker processing completed" original_count=us_tickers_count filtered_count=filtered_count

            if filtered_count == 0
                @warn "us_tickers_filtered table was created but contains no rows"
            end
        catch e
            @error "Error in download_tickers_duckdb" exception=(e, catch_backtrace())
            rethrow(e)
        finally
            cleanup_files(zip_file_path, csv_file)
        end
    end

    """
        download_latest_tickers(url::String, zip_file_path::String, csv_file::String)

    Helper function to download and unzip a file.
    """
    function download_latest_tickers(
        url::String = Config.API.TICKERS_URL,
        zip_file_path::String = Config.DB.ZIP_FILE_PATH,
        csv_file::String = Config.DB.DEFAULT_CSV_FILE
    )
        HTTP.download(url, zip_file_path)

        target_path = csv_file
        target_basename = basename(target_path)
        target_dir = dirname(target_path)
        if target_dir != "." && !isempty(target_dir)
            mkpath(target_dir)
        end

        found = false
        reader = ZipFile.Reader(zip_file_path)
        try
            for f in reader.files
                if basename(f.name) == target_basename
                    open(target_path, "w") do io
                        write(io, read(f))
                    end
                    found = true
                    break
                end
            end
        finally
            close(reader)
        end

        if !found
            error("CSV file '$target_basename' not found in zip archive: $zip_file_path")
        end

        @info "Downloaded and unzipped: $target_basename"
    end

    """
        process_tickers_csv(conn::DuckDBConnection, csv_file::String)

    Helper function to process the tickers CSV file and insert into DuckDB.
    """
    function process_tickers_csv(
        conn::DuckDBConnection,
        csv_file::String
    )
        safe_path = validate_file_path(csv_file)
        DBInterface.execute(conn, """
        CREATE OR REPLACE TABLE us_tickers AS
        SELECT * FROM read_csv('$safe_path')
        """)
        @info "Update us_tickers in DuckDB with the CSV"
    end

    """
        create_filtered_tickers(conn::DuckDBConnection)

    Create filtered US tickers table.
    """
    function create_filtered_tickers(conn::DuckDBConnection)
        @info "Generating filtered tickers table..."
        exchanges = join(["'" * validate_sql_value(ex) * "'" for ex in Config.Filtering.SUPPORTED_EXCHANGES], ", ")
        asset_types = join(["'" * validate_sql_value(at) * "'" for at in Config.Filtering.SUPPORTED_ASSET_TYPES], ", ")

        DBInterface.execute(conn, """
            CREATE OR REPLACE TABLE us_tickers_filtered AS
            SELECT * FROM us_tickers
            WHERE exchange IN ($exchanges)
              AND endDate >= (SELECT max(endDate) FROM us_tickers WHERE assetType = 'Stock' and exchange = 'NYSE')
              AND assetType IN ($asset_types)
              AND ticker NOT LIKE '%/%'
        """)
    end

    """
        cleanup_files(zip_file_path::String)

    Helper function to clean up temporary files.
    """
    function cleanup_files(zip_file_path::String, csv_file::String)
        for file in [zip_file_path, csv_file]
            if isfile(file)
                rm(file)
                @info "Cleaned up temporary file: $file"
            end
        end
    end

    """
        generate_filtered_tickers(conn::DuckDBConnection)

    Generate a filtered list of US tickers.
    """
    function generate_filtered_tickers(
        conn::DuckDBConnection
    )
        try
            # Check if us_tickers table exists and has data
            us_tickers_count = Operations.get_table_count(conn, Config.DB.Tables.US_TICKERS)

            if us_tickers_count == 0
                error("us_tickers table is empty or does not exist")
            end

            # Create and populate the filtered table
            create_filtered_tickers(conn)

            # Verify the table was created and has rows
            filtered_count = Operations.get_table_count(conn, Config.DB.Tables.US_TICKERS_FILTERED)

            @info "Original us_tickers count: $us_tickers_count"
            @info "Filtered us_tickers_filtered count: $filtered_count"

            if filtered_count == 0
                @warn "us_tickers_filtered table was created but contains no rows"
            else
                @info "Generated filtered list of US tickers with $filtered_count rows"
            end

        catch e
            @error "Error in generate_filtered_tickers: $(e)"
            rethrow(e)
        end
    end

    """
        update_us_tickers(conn::DuckDBConnection, csv_file::String)

    Update the us_tickers table in the database from a CSV file.
    """
    function update_us_tickers(conn::DuckDBConnection, csv_file::String = Config.DB.DEFAULT_CSV_FILE)
        safe_table = validate_identifier(Config.DB.Tables.US_TICKERS)
        safe_path = validate_file_path(csv_file)
        query = """
        CREATE OR REPLACE TABLE $safe_table AS
        SELECT * FROM read_csv('$safe_path')
        """
        try
            DBInterface.execute(conn, query)
            @info "Updated us_tickers table from file: $csv_file"
        catch e
            @error "Failed to update us_tickers table" exception=(e, catch_backtrace())
            # throw(DatabaseQueryError("Failed to update us_tickers: $e", query)) # Error type not available here
            rethrow(e)
        end
    end

    """
        update_historical(conn::DuckDBConnection, tickers::DataFrame, api_key::String; use_parallel::Bool=false, batch_size::Int=50, max_concurrent::Int=10, add_missing::Bool=true)

    Update historical data for multiple tickers.
    """
    function update_historical(
        conn::DuckDBConnection,
        tickers::DataFrame,
        api_key::String = get_api_key();
        use_parallel::Bool = false,
        batch_size::Int = 50,
        max_concurrent::Int = 10,
        add_missing::Bool = true,
        latest_dates_df::Union{DataFrame,Nothing} = nothing,
        reference_ticker::String = "SPY"
    )
        # Dispatch to parallel or sequential version
        if use_parallel
            return update_historical_parallel(
                conn, tickers, api_key;
                batch_size=batch_size,
                max_concurrent=max_concurrent,
                add_missing=add_missing
            )
        else
            return update_historical_sequential_impl(
                conn, tickers, api_key;
                add_missing=add_missing,
                latest_dates_df=latest_dates_df
            )
        end
    end

    function update_historical_sequential_impl(
        conn::DuckDBConnection,
        tickers::DataFrame,
        api_key::String;
        add_missing::Bool = true,
        latest_dates_df::Union{DataFrame,Nothing} = nothing
    )
        # Only compute latest dates if not provided (optimization for batch processing)
        if latest_dates_df === nothing
            latest_dates_df = Operations.get_latest_dates(conn)
        end

        updated_tickers = String[]
        missing_tickers = String[]
        error_tickers = String[]

        for (i, row) in enumerate(eachrow(tickers))
            symbol = row.ticker
            # Use ticker's own end_date from the row (this comes from us_tickers_filtered query)
            ticker_end_date = haskey(row, :end_date) ? row.end_date : (haskey(row, :endDate) ? row.endDate : Date(now()) - Day(1))
            ticker_latest = filter(r -> r.ticker == symbol, latest_dates_df)

            if isempty(ticker_latest)
                if add_missing
                    handle_missing_ticker(conn, row, api_key, missing_tickers, updated_tickers)
                else
                    push!(missing_tickers, symbol)
                    @info "$i : $symbol is missing and add_missing=false"
                end
            else
                latest_date = ticker_latest[1, :latest_date]
                if latest_date < ticker_end_date
                    @info "$i : $symbol : $(latest_date + Day(1)) ~ $ticker_end_date"
                    try
                        ticker_data = get_ticker_data(
                            row,
                            start_date = latest_date + Day(1),
                            end_date = ticker_end_date,
                            api_key = api_key
                        )
                        if !isempty(ticker_data)
                            Operations.upsert_stock_data(conn, ticker_data, symbol)
                            push!(updated_tickers, symbol)
                        end
                    catch e
                        if (isa(e, ErrorException) && occursin("No data returned", e.msg)) ||
                           (isa(e, AssertionError) && occursin("No data returned", e.msg))
                            @info "$i : $symbol has no new data"
                        else
                            @warn "Failed to update $symbol: $e"
                            push!(error_tickers, symbol)
                        end
                    end
                else
                    @info "$i : $symbol is up to date"
                end
            end
        end

        log_update_results(missing_tickers, updated_tickers, error_tickers, add_missing)
        return (updated_tickers, missing_tickers)
    end

    function update_historical_parallel(
        conn::DuckDBConnection,
        tickers::DataFrame,
        api_key::String = get_api_key();
        batch_size::Int = 50,
        max_concurrent::Int = 10,
        add_missing::Bool = true
    )
        @info "Starting parallel historical data update" total_tickers=nrow(tickers) batch_size max_concurrent

        # Pre-compute latest dates once for all batches
        latest_dates_df = Operations.get_latest_dates(conn)

        all_updated = String[]
        all_missing = String[]

        # Process tickers in batches
        num_batches = Int(ceil(nrow(tickers) / batch_size))

        for batch_idx in 1:num_batches
            start_idx = (batch_idx - 1) * batch_size + 1
            end_idx = min(batch_idx * batch_size, nrow(tickers))
            batch = tickers[start_idx:end_idx, :]

            @info "Processing batch $batch_idx/$num_batches" tickers_in_batch=nrow(batch)

            # Use Channel for job queue to limit concurrency
            jobs = Channel{Int}(max_concurrent)
            results = Channel{Tuple{String, Bool, Union{Exception, Nothing}}}(nrow(batch))
            write_queue = Channel{Tuple{String, Union{DataFrame, Nothing}, Bool, Union{Exception, Nothing}}}(nrow(batch))

            # Spawn workers
            @sync begin
                # Single writer to avoid concurrent DB writes on one connection
                @async begin
                    for (ticker, data, success, error) in write_queue
                        if data !== nothing
                            try
                                Operations.upsert_stock_data_bulk(conn, data, ticker)
                                put!(results, (ticker, true, nothing))
                            catch e
                                put!(results, (ticker, false, e))
                            end
                        else
                            put!(results, (ticker, success, error))
                        end
                    end
                    close(results)
                end

                # Producer: add jobs to queue
                @async begin
                    for i in 1:nrow(batch)
                        put!(jobs, i)
                    end
                    close(jobs)
                end

                # Consumers: process jobs with limited concurrency
                worker_tasks = Task[]
                for _ in 1:max_concurrent
                    t = @async begin
                        for job_idx in jobs
                            row = batch[job_idx, :]
                            ticker = row.ticker

                            try
                                ticker_end_date = haskey(row, :end_date) ? row.end_date :
                                                 (haskey(row, :endDate) ? row.endDate : Date(now()) - Day(1))
                                ticker_latest = filter(r -> r.ticker == ticker, latest_dates_df)

                                if isempty(ticker_latest)
                                    # Missing ticker
                                    if add_missing
                                        ticker_data = get_ticker_data(row; api_key=api_key)
                                        if !isempty(ticker_data)
                                            put!(write_queue, (ticker, ticker_data, true, nothing))
                                        else
                                            put!(write_queue, (ticker, nothing, false, ErrorException("No data retrieved")))
                                        end
                                    else
                                        put!(write_queue, (ticker, nothing, false, nothing))
                                    end
                                else
                                    latest_date = ticker_latest[1, :latest_date]
                                    if latest_date < ticker_end_date
                                        ticker_data = get_ticker_data(
                                            row,
                                            start_date = latest_date + Day(1),
                                            end_date = ticker_end_date,
                                            api_key = api_key
                                        )
                                        if !isempty(ticker_data)
                                            put!(write_queue, (ticker, ticker_data, true, nothing))
                                        else
                                            put!(write_queue, (ticker, nothing, false, ErrorException("No new data")))
                                        end
                                    else
                                        put!(write_queue, (ticker, nothing, true, nothing))
                                    end
                                end
                            catch e
                                put!(write_queue, (ticker, nothing, false, e))
                            end
                        end
                    end
                    push!(worker_tasks, t)
                end

                # Close write queue when all workers are done
                @async begin
                    for t in worker_tasks
                        wait(t)
                    end
                    close(write_queue)
                end
            end

            # Collect results
            batch_updated = String[]
            batch_missing = String[]

            for (ticker, success, error) in results
                if success
                    push!(batch_updated, ticker)
                else
                    push!(batch_missing, ticker)
                    if !isnothing(error)
                        @warn "Failed to update ticker: $ticker" exception=error
                    end
                end
            end

            append!(all_updated, batch_updated)
            append!(all_missing, batch_missing)

            @info "Batch $batch_idx complete" updated=length(batch_updated) missing=length(batch_missing)
        end

        @info "Parallel update completed" total_updated=length(all_updated) total_missing=length(all_missing)
        return (all_updated, all_missing)
    end

    function update_historical_sequential(
        conn::DuckDBConnection,
        tickers::DataFrame,
        api_key::String = get_api_key();
        add_missing::Bool = true
    )
        @info "Starting sequential historical data update" total_tickers=nrow(tickers)
        return update_historical_sequential_impl(conn, tickers, api_key; add_missing=add_missing)
    end

    function handle_missing_ticker(
        conn::DuckDBConnection,
        ticker_info::DataFrameRow,
        api_key::String,
        missing_tickers::Vector{String},
        updated_tickers::Vector{String}
    )
        symbol = ticker_info.ticker
        push!(missing_tickers, symbol)
        @info "Adding missing ticker: $symbol"
        try
            ticker_data = get_ticker_data(ticker_info; api_key=api_key)
            if !isempty(ticker_data)
                Operations.upsert_stock_data(conn, ticker_data, symbol)
                push!(updated_tickers, symbol)
            else
                @warn "No data retrieved for $symbol"
            end
        catch e
            @warn "Failed to add historical data for $symbol: $e"
        end
    end

    function log_update_results(missing_tickers::Vector{String}, updated_tickers::Vector{String}, error_tickers::Vector{String}, add_missing::Bool)
        if !isempty(missing_tickers)
            if add_missing
                @info "Attempted to add $(length(missing_tickers)) missing tickers to historical_data"
            else
                @warn "The following tickers are not in historical_data: $missing_tickers"
            end
        end

        if !isempty(error_tickers)
            @warn "The following tickers encountered errors during processing: $error_tickers"
        end

        @info "Historical data update completed" updated_count=length(updated_tickers) missing_count=length(missing_tickers) error_count=length(error_tickers)
    end

    """
        update_split_ticker(conn::DuckDBConnection, tickers::DataFrame, api_key::String)

    Update data for tickers that have undergone a split.
    """
    function update_split_ticker(
        conn::DuckDBConnection,
        tickers::DataFrame, # all tickers is best
        api_key::String = get_api_key()
    )
        # Handle empty tickers DataFrame
        if nrow(tickers) == 0
            @info "No tickers to process for split updates"
            return
        end

        end_date = maximum(skipmissing(tickers.end_date))

        split_tickers = DBInterface.execute(conn, """
        SELECT ticker, splitFactor, date
          FROM historical_data
         WHERE date = ?
           AND splitFactor <> 1.0
        """, [end_date]) |> DataFrame

        for (i, row) in enumerate(eachrow(split_tickers))
            symbol = row.ticker
            if ismissing(symbol) || symbol === nothing
                continue  # Skip this row if ticker is missing or null
            end
            ticker_info = tickers[tickers.ticker .== symbol, :]
            if isempty(ticker_info)
                @warn "No ticker info found for $symbol"
                continue
            end
            start_date = ticker_info[1, :start_date]
            @info "$i: Updating split ticker $symbol from $start_date to $end_date"
            ticker_data = get_ticker_data(ticker_info[1, :]; api_key=api_key)
            Operations.upsert_stock_data(conn, ticker_data, symbol)
        end
        @info "Updated split tickers"
    end

    """
        add_historical_data(conn::DuckDBConnection, ticker::String, api_key::String)

    Add historical data for a single ticker.
    """
    function add_historical_data(
        conn::DuckDBConnection,
        ticker::String,
        api_key::String = get_api_key()
    )
        # Look up ticker info from the database to get a DataFrameRow for get_ticker_data
        ticker_df = DBInterface.execute(conn, """
            SELECT ticker, exchange, assettype as asset_type,
                   startdate as start_date, enddate as end_date
            FROM us_tickers_filtered
            WHERE ticker = ?
        """, [ticker]) |> DataFrame

        if isempty(ticker_df)
            @warn "Ticker $ticker not found in us_tickers_filtered"
            return
        end

        ticker_info = ticker_df[1, :]
        data = get_ticker_data(ticker_info; api_key=api_key)
        if isempty(data)
            @warn "No data retrieved for $ticker"
            return
        end
        Operations.upsert_stock_data(conn, data, ticker)
        @info "Added historical data for $ticker"
    end
