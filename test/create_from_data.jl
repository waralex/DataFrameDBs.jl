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


rm("test_data", force = true, recursive = true) 

end