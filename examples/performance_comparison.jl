#!/usr/bin/env julia

"""
Performance Comparison Script for TiingoJulia Historical Data Updates

This script demonstrates the performance improvements achieved with the optimized
update_historical_parallel function compared to the original sequential version.
"""

using TiingoJulia
using DataFrames
using Dates
using BenchmarkTools

function setup_test_environment()
    """Set up a test database with sample data"""
    println("Setting up test environment...")

    # Connect to test database
    conn = connect_duckdb("performance_test.duckdb")

    # Optimize database settings
    optimize_database(conn)
    create_indexes(conn)

    # Add some sample tickers for testing
    sample_tickers = DataFrame(
        ticker = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX", "CRM", "ADBE"],
        exchange = fill("NASDAQ", 10),
        asset_type = fill("Stock", 10),
        start_date = fill(Date("2023-01-01"), 10),
        end_date = fill(Date("2023-12-31"), 10)
    )

    return conn, sample_tickers
end

function benchmark_sequential_update(conn, tickers, api_key)
    """Benchmark the original sequential update method"""
    println("\n🐌 Testing Sequential Update (Original)...")

    result = @timed begin
        updated, missing = update_historical_sequential(conn, tickers, api_key; add_missing=true)
    end

    println("Sequential Update Results:")
    println("  ⏱️  Time: $(round(result.time, digits=2)) seconds")
    println("  💾 Memory: $(round(result.bytes / 1024^2, digits=2)) MB")
    println("  📊 Updated: $(length(result.value[1])) tickers")
    println("  ❓ Missing: $(length(result.value[2])) tickers")

    return result
end

function benchmark_parallel_update(conn, tickers, api_key; batch_size=5, max_concurrent=3)
    """Benchmark the optimized parallel update method"""
    println("\n🚀 Testing Parallel Update (Optimized)...")

    result = @timed begin
        updated, missing = update_historical_parallel(
            conn, tickers, api_key;
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            add_missing=true
        )
    end

    println("Parallel Update Results:")
    println("  ⏱️  Time: $(round(result.time, digits=2)) seconds")
    println("  💾 Memory: $(round(result.bytes / 1024^2, digits=2)) MB")
    println("  📊 Updated: $(length(result.value[1])) tickers")
    println("  ❓ Missing: $(length(result.value[2])) tickers")
    println("  🔄 Batch size: $batch_size")
    println("  ⚡ Max concurrent: $max_concurrent")

    return result
end

function compare_performance(sequential_result, parallel_result)
    """Compare and display performance improvements"""
    println("\n📈 Performance Comparison:")
    println("=" ^ 50)

    time_improvement = (sequential_result.time - parallel_result.time) / sequential_result.time * 100
    memory_improvement = (sequential_result.bytes - parallel_result.bytes) / sequential_result.bytes * 100

    println("⏱️  Time Improvement: $(round(time_improvement, digits=1))%")
    println("💾 Memory Improvement: $(round(memory_improvement, digits=1))%")
    println("🚀 Speedup Factor: $(round(sequential_result.time / parallel_result.time, digits=2))x")

    if time_improvement > 0
        println("✅ Parallel version is FASTER!")
    else
        println("⚠️  Sequential version was faster (possibly due to small dataset)")
    end
end

function demonstrate_bulk_upsert_performance(conn)
    """Demonstrate the performance difference between row-by-row and bulk upsert"""
    println("\n🔄 Testing Bulk Upsert Performance...")

    # Generate sample data
    sample_data = DataFrame(
        date = [Date("2023-01-01") + Day(i) for i in 1:100],
        close = rand(100:200, 100),
        high = rand(100:200, 100),
        low = rand(100:200, 100),
        open = rand(100:200, 100),
        volume = rand(1000000:5000000, 100),
        adjClose = rand(100:200, 100),
        adjHigh = rand(100:200, 100),
        adjLow = rand(100:200, 100),
        adjOpen = rand(100:200, 100),
        adjVolume = rand(1000000:5000000, 100),
        divCash = zeros(100),
        splitFactor = ones(100)
    )

    # Test original upsert
    original_time = @elapsed begin
        upsert_stock_data(conn, sample_data, "TEST_ORIGINAL")
    end

    # Test bulk upsert
    bulk_time = @elapsed begin
        upsert_stock_data_bulk(conn, sample_data, "TEST_BULK")
    end

    println("Upsert Performance Comparison:")
    println("  🐌 Original method: $(round(original_time, digits=3)) seconds")
    println("  🚀 Bulk method: $(round(bulk_time, digits=3)) seconds")
    println("  📈 Improvement: $(round((original_time - bulk_time) / original_time * 100, digits=1))%")
    println("  ⚡ Speedup: $(round(original_time / bulk_time, digits=2))x")
end

function main()
    """Main function to run the performance comparison"""
    println("🚀 TiingoJulia Performance Comparison")
    println("=" ^ 50)

    try
        # Get API key
        api_key = get_api_key()
        println("✅ API key loaded successfully")

        # Setup test environment
        conn, sample_tickers = setup_test_environment()
        println("✅ Test environment ready")

        # Use a smaller subset for testing to avoid API rate limits
        test_tickers = sample_tickers[1:3, :]  # Test with 3 tickers

        # Benchmark bulk upsert performance
        demonstrate_bulk_upsert_performance(conn)

        # Note: Uncomment the following lines to test API performance
        # WARNING: This will make actual API calls and may hit rate limits

        # println("\n⚠️  The following tests will make actual API calls...")
        # println("Press Enter to continue or Ctrl+C to cancel...")
        # readline()

        # # Benchmark sequential update
        # sequential_result = benchmark_sequential_update(conn, test_tickers, api_key)

        # # Clear data for fair comparison
        # DBInterface.execute(conn, "DELETE FROM historical_data WHERE ticker IN ('AAPL', 'GOOGL', 'MSFT')")

        # # Benchmark parallel update
        # parallel_result = benchmark_parallel_update(conn, test_tickers, api_key; batch_size=2, max_concurrent=2)

        # # Compare results
        # compare_performance(sequential_result, parallel_result)

        println("\n✅ Performance comparison completed!")

        # Cleanup
        close_duckdb(conn)

    catch e
        if isa(e, InterruptException)
            println("\n⚠️  Test cancelled by user")
        else
            println("\n❌ Error during performance test: $e")
            rethrow(e)
        end
    end
end

# Performance Tips and Recommendations
function print_performance_tips()
    println("\n💡 Performance Optimization Tips:")
    println("=" ^ 50)
    println("1. 🚀 Use update_historical_parallel() instead of update_historical_sequential()")
    println("2. 📦 Adjust batch_size (50-100) based on your API rate limits")
    println("3. ⚡ Set max_concurrent (5-20) based on your network and API limits")
    println("4. 🗃️  Call create_indexes() once after initial data load")
    println("5. ⚙️  Call optimize_database() to tune DuckDB settings")
    println("6. 💾 Use upsert_stock_data_bulk() for large datasets")
    println("7. 🔄 Process tickers in smaller batches to avoid memory issues")
    println("8. 📊 Monitor API rate limits to avoid throttling")
    println("\nExample usage:")
    println("```julia")
    println("conn = connect_duckdb()")
    println("optimize_database(conn)")
    println("create_indexes(conn)")
    println("tickers = get_tickers_stock(conn)")
    println("update_historical(conn, tickers; use_parallel=true, batch_size=50, max_concurrent=10)")
    println("```")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
    print_performance_tips()
end
