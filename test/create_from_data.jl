using DataFrames
import Tables
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