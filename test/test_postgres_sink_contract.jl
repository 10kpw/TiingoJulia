using Test
using DataFrames
using Dates
using DBInterface
using DuckDB
using LibPQ
using QuansiftMarketData

@testset "PostgreSQL sink dispatch" begin
    postgres_module = QuansiftMarketData.DB.Postgres
    schema_module = QuansiftMarketData.DB.Schema

    @test PostgreSQLConnection === LibPQ.Connection

    persistence_methods = [
        (
            upsert_stock_data,
            Tuple{LibPQ.Connection,DataFrame,String},
        ),
        (
            upsert_stock_data_bulk,
            Tuple{LibPQ.Connection,DataFrame,String},
        ),
        (
            upsert_security_observations,
            Tuple{LibPQ.Connection,DataFrame},
        ),
        (
            upsert_fundamental_daily_metrics,
            Tuple{LibPQ.Connection,DataFrame},
        ),
        (
            postgres_module.replace_ticker_universe,
            Tuple{LibPQ.Connection,DataFrame,DataFrame},
        ),
    ]

    for (persistence_function, signature) in persistence_methods
        @test hasmethod(persistence_function, signature)
        @test which(persistence_function, signature).module === postgres_module
    end

    replacement_method = which(
        postgres_module.replace_ticker_universe,
        Tuple{LibPQ.Connection,DataFrame,DataFrame},
    )
    @test :statement_timeout_seconds in Base.kwarg_decl(replacement_method)
    @test :lock_timeout_seconds in Base.kwarg_decl(replacement_method)
    @test !isdefined(postgres_module, :export_to_postgres)

    schema_signature = Tuple{LibPQ.Connection}
    @test hasmethod(create_tables, schema_signature)
    @test which(create_tables, schema_signature).module === schema_module
end

@testset "PostgreSQL upsert guards accept only typed columns" begin
    postgres_module = QuansiftMarketData.DB.Postgres
    method = which(
        postgres_module.transactional_upsert!,
        Tuple{
            LibPQ.Connection,
            String,
            DataFrame,
            Vector{Symbol},
            Vector{Symbol},
        },
    )
    keyword_arguments = Base.kwarg_decl(method)

    @test :update_guard_column in keyword_arguments
    @test :update_where ∉ keyword_arguments
end

@testset "PostgreSQL observation upsert validates canonical keys before SQL" begin
    postgres_source = read(
        joinpath(@__DIR__, "..", "src", "db", "postgres.jl"),
        String,
    )

    @test occursin(r"using \.\.Operations:.*validate_security_observation_keys"s,
                   postgres_source)
    observation_method = match(
        r"function upsert_security_observations\(.*?\n    end"s,
        postgres_source,
    )
    @test !isnothing(observation_method)
    @test occursin(
        "validate_security_observation_keys(data; require_columns=true)",
        isnothing(observation_method) ? "" : observation_method.match,
    )
end

@testset "PostgreSQL temporary environment is serialized and restored" begin
    postgres_module = QuansiftMarketData.DB.Postgres
    variable = "TIINGO_TEST_TEMPORARY_ENV_LOCK"
    original = get(ENV, variable, nothing)
    pop!(ENV, variable, nothing)

    first_entered = Channel{Nothing}(1)
    release_first = Channel{Nothing}(1)
    second_entered = Ref(false)
    first = @async postgres_module.with_temporary_env(Dict(variable => "first")) do
        put!(first_entered, nothing)
        take!(release_first)
        @test ENV[variable] == "first"
    end
    take!(first_entered)
    second = @async postgres_module.with_temporary_env(Dict(variable => "second")) do
        second_entered[] = true
        @test ENV[variable] == "second"
    end

    yield()
    @test !second_entered[]
    @test ENV[variable] == "first"
    put!(release_first, nothing)
    wait(first)
    wait(second)
    @test second_entered[]
    @test !haskey(ENV, variable)

    if isnothing(original)
        pop!(ENV, variable, nothing)
    else
        ENV[variable] = original
    end
end

@testset "DuckDB PostgreSQL extension setup is load-only and fail-closed" begin
    postgres_module = QuansiftMarketData.DB.Postgres
    postgres_source = read(
        joinpath(@__DIR__, "..", "src", "db", "postgres.jl"),
        String,
    )
    @test !occursin(
        r"DBInterface\.execute\(duckdb_conn,\s*\"INSTALL postgres;?\"\)"s,
        postgres_source,
    )

    load_calls = Tuple{Any,String}[]
    @test isnothing(postgres_module.load_postgres_extension!(
        :test_connection;
        execute=(conn, sql) -> push!(load_calls, (conn, sql)),
    ))
    @test load_calls == [(:test_connection, "LOAD postgres")]

    secret = "postgresql://alice:load-secret@example.invalid/app"
    load_error = try
        postgres_module.load_postgres_extension!(
            :test_connection;
            execute=(_, _) -> error("extension failure for $secret"),
        )
        nothing
    catch error
        error
    end
    @test load_error isa ErrorException
    load_message = sprint(showerror, load_error)
    @test occursin("build time", lowercase(load_message))
    @test occursin("runtime downloads are disabled", lowercase(load_message))
    @test !occursin("alice", load_message)
    @test !occursin("load-secret", load_message)

    duckdb_conn = DBInterface.connect(DuckDB.DB)
    pg_conn = LibPQ.Connection(
        "host=127.0.0.1 port=1 dbname=unavailable connect_timeout=1";
        throw_error=false,
    )
    setup_calls = String[]
    try
        @test isnothing(postgres_module.setup_postgres_connection(
            duckdb_conn,
            pg_conn;
            connection_string="host=example.invalid dbname=app user=reader",
            execute=(_, sql) -> push!(setup_calls, sql),
        ))
        @test setup_calls == [
            "LOAD postgres",
            "ATTACH '' AS postgres_db (TYPE postgres);",
        ]
        @test all(sql -> !occursin("INSTALL", uppercase(sql)), setup_calls)
    finally
        LibPQ.close(pg_conn)
        DBInterface.close!(duckdb_conn)
    end
end

@testset "PostgreSQL upsert column mappings" begin
    postgres_module = QuansiftMarketData.DB.Postgres
    @test last(postgres_module.STOCK_COLUMN_MAPPINGS) ==
          (:fetched_at => :fetched_at)
    mapping_contracts = [
        postgres_module.STOCK_COLUMN_MAPPINGS,
        postgres_module.SECURITY_OBSERVATION_COLUMN_MAPPINGS,
        postgres_module.FUNDAMENTAL_METRIC_COLUMN_MAPPINGS,
        postgres_module.TICKER_UNIVERSE_COLUMN_MAPPINGS,
    ]

    for mappings in mapping_contracts
        source_values = Dict(
            source => index
            for (index, (source, _)) in enumerate(mappings)
        )
        input = DataFrame()
        for (source, _) in reverse(mappings)
            input[!, source] = [source_values[source]]
        end
        input[!, :ignored_extra_column] = [-1]

        mapped = postgres_module.select_upsert_columns(input, mappings)

        @test propertynames(mapped) == last.(mappings)
        @test nrow(mapped) == 1
        @test :ignored_extra_column ∉ propertynames(mapped)
        for (source, target) in mappings
            @test mapped[1, target] == source_values[source]
        end

        missing_source = first(first(mappings))
        incomplete = select(input, Not(missing_source))
        error = try
            postgres_module.select_upsert_columns(incomplete, mappings)
            nothing
        catch caught
            caught
        end
        @test error isa ArgumentError
        if error isa ArgumentError
            @test occursin(string(missing_source), sprint(showerror, error))
        end
    end
end

@testset "PostgreSQL upsert columns map JSON nothing to SQL NULL" begin
    # Tiingo `null` decodes to `nothing`, which LibPQ would send as the
    # literal text "nothing" and PostgreSQL rejects for BIGINT volume.
    postgres_module = QuansiftMarketData.DB.Postgres
    mappings = [:volume => :volume, :close => :close]
    input = DataFrame(volume = [nothing, 10], close = [1.5, nothing])

    mapped = postgres_module.select_upsert_columns(input, mappings)

    @test mapped.volume[1] === missing
    @test mapped.volume[2] == 10
    @test mapped.close[2] === missing
    @test !any(x -> x === nothing, mapped.volume)
    @test !any(x -> x === nothing, mapped.close)
    @test all(LibPQ.string_parameters(mapped.volume) .!== "nothing")
end

@testset "Attached PostgreSQL keeps its environment for the whole scope" begin
    # The libpq environment must cover the ATTACHMENT'S LIFETIME, not just the
    # ATTACH statement. DuckDB's postgres scanner opens further connections
    # lazily as later queries run, and each reads the environment when it is
    # opened. A narrower scope left those connections resolving against ambient
    # defaults — hydration failed on 2026-08-15 naming a unix socket and a
    # database named after the OS user, neither of which appeared anywhere in
    # the connection string it was given.
    postgres_module = QuansiftMarketData.DB.Postgres
    connection_string = "postgresql://scope-user@scope-host:5432/scope-db"

    statements = String[]
    seen_inside = Dict{String,Union{Nothing,String}}()
    before = Dict(
        name => get(ENV, name, nothing)
        for name in ("PGHOST", "PGUSER", "PGDATABASE")
    )

    result = postgres_module.with_attached_postgres(
        nothing,
        connection_string;
        alias = "pg_scope",
        read_only = true,
        execute = (_, sql) -> (push!(statements, String(sql)); nothing),
    ) do
        for name in ("PGHOST", "PGUSER", "PGDATABASE")
            seen_inside[name] = get(ENV, name, nothing)
        end
        return :body_ran
    end

    @test result == :body_ran
    # The body — where every scanner query runs — sees the real target.
    @test seen_inside["PGHOST"] == "scope-host"
    @test seen_inside["PGUSER"] == "scope-user"
    @test seen_inside["PGDATABASE"] == "scope-db"
    # And the environment is handed back exactly as it was found.
    for (name, original) in before
        @test get(ENV, name, nothing) == original
    end

    @test any(sql -> occursin("LOAD postgres", sql), statements)
    attach_sql = only(filter(sql -> occursin("ATTACH", sql), statements))
    @test occursin("AS pg_scope", attach_sql)
    @test occursin("READ_ONLY", attach_sql)
    # The password never reaches SQL text, which is the whole reason the
    # environment mechanism exists rather than ATTACH '<conninfo>'.
    @test all(sql -> !occursin("scope-user", sql), statements)
    @test occursin("DETACH pg_scope", last(statements))
end

@testset "Attached PostgreSQL detaches without burying the real error" begin
    postgres_module = QuansiftMarketData.DB.Postgres
    connection_string = "postgresql://scope-user@scope-host:5432/scope-db"

    # Detach must run even when the body throws.
    statements = String[]
    caught = try
        postgres_module.with_attached_postgres(
            nothing,
            connection_string;
            alias = "pg_scope",
            execute = (_, sql) -> (push!(statements, String(sql)); nothing),
        ) do
            error("body failure")
        end
        nothing
    catch error
        error
    end
    @test caught isa ErrorException
    @test occursin("body failure", sprint(showerror, caught))
    @test occursin("DETACH pg_scope", last(statements))

    # And a failing detach must not replace the error being propagated: an
    # exception raised in `finally` silently wins, which is how a cleanup
    # problem ends up masquerading as the root cause.
    hostile_execute = function (_, sql)
        occursin("DETACH", String(sql)) && error("detach exploded")
        return nothing
    end
    masked = try
        postgres_module.with_attached_postgres(
            nothing,
            connection_string;
            alias = "pg_scope",
            execute = hostile_execute,
        ) do
            error("the real failure")
        end
        nothing
    catch error
        error
    end
    @test occursin("the real failure", sprint(showerror, masked))
    @test !occursin("detach exploded", sprint(showerror, masked))
end
