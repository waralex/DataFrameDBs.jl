using DataFrameDBs: insert, create_table, materialize
using DataFrames
import Tables
using CSV
@testset "from DataFrame" begin
rm("test_data", force = true, recursive = true) 
df = DataFrame((
    a = [1,2,3,4,5],
    b = ["1", "2", "3", "4", "5"]
))

tb = DataFrameDBs.create_table("test_data", from = df)

@test DataFrameDBs.materialize(tb) == df

@test size(tb) == size(df)
@test size(tb,1) == size(df,1)
@test size(tb,2) == size(df,2)
rm("test_data", force = true, recursive = true) 



end

@testset "from rows" begin
    rm("test_data", force = true, recursive = true) 
    data = CSV.Rows("test.csv")
    tb = DataFrameDBs.create_table("test_data", from=data, block_size = 10)
    insert(tb, data)
    insert(tb, data)
    insert(tb, data)
    
    df = DataFrame(data)

    dft = tb |> materialize

    for n in names(df)
        @test dft[:, n] == repeat(df[:, n], 4)
    end

    rm("test_data", force = true, recursive = true) 
end