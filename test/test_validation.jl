using Test
using TiingoJulia
using TiingoJulia.DB.Core: validate_identifier, validate_file_path, validate_sql_value

@testset "SQL Validation Functions" begin
    @testset "validate_identifier" begin
        @test validate_identifier("us_tickers") == "us_tickers"
        @test validate_identifier("historical_data") == "historical_data"
        @test validate_identifier("_private") == "_private"
        @test validate_identifier("Table123") == "Table123"

        @test_throws ArgumentError validate_identifier("bad table")
        @test_throws ArgumentError validate_identifier("table;DROP")
        @test_throws ArgumentError validate_identifier("123start")
        @test_throws ArgumentError validate_identifier("")
    end

    @testset "validate_file_path" begin
        @test validate_file_path("/path/to/file.csv") == "/path/to/file.csv"
        @test validate_file_path("relative/path.csv") == "relative/path.csv"

        @test_throws ArgumentError validate_file_path("file'; DROP TABLE x;--")
        @test_throws ArgumentError validate_file_path("path;malicious")
    end

    @testset "validate_sql_value" begin
        @test validate_sql_value("NYSE") == "NYSE"
        @test validate_sql_value("NYSE ARCA") == "NYSE ARCA"
        @test validate_sql_value("Stock") == "Stock"
        @test validate_sql_value("ETF") == "ETF"

        @test_throws ArgumentError validate_sql_value("value' OR 1=1--")
    end
end
