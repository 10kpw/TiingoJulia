using LibPQ, DataFrames

function store_data(df::DataFrame, table_name:String)
    conn = LibPQ.Connection("postgresql://user:password@host:port/dbname")
    LibPQ.load!(df, table_name, conn)
end
