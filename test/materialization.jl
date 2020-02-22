using DataFrameDBs: meta_by_condition, open_files
using DataFrames
@testset "openfiles" begin
rm("test_data", force = true, recursive = true) 
size = 500000
df = DataFrame((
    a = rand(Int32, size),
    b = rand(Int8, size),
    c = string.(rand(Int16, size))
))

tb = DataFrameDBs.create_table("test_data", from = df)

@test meta_by_condition(tb, :) == tb.meta.columns
@test meta_by_condition(tb, [1,3]) == tb.meta.columns[[1,3]]
@test meta_by_condition(tb, [3,1]) == tb.meta.columns[[3,1]]
@test_throws BoundsError meta_by_condition(tb, [1,4])
@test_throws ArgumentError meta_by_condition(tb, [1,1,3])

@test meta_by_condition(tb, [:a, :c]) == tb.meta.columns[[1,3]]
@test meta_by_condition(tb, [:c, :b]) == tb.meta.columns[[3,2]]
@test_throws ArgumentError meta_by_condition(tb, [:a, :a, :c])
@test_throws ArgumentError meta_by_condition(tb, [:e])


ios = open_files(tb, :)
@test length(ios) == 3
close.(ios)
ios = open_files(tb, [1,2])
@test length(ios) == 2
close.(ios)
ios = open_files(tb, [:b])
@test length(ios) == 1
close.(ios)

rm("test_data", force = true, recursive = true)
end