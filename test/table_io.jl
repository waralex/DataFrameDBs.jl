


@testset "meta_io" begin
    test_meta = DataFrameDBs.DFTableMeta([:a, :b, :c], [Int32, String, Int64], 1223, 1)
    io = PipeBuffer()
    DataFrameDBs.write_table_meta(io, test_meta)
    @test DataFrameDBs.read_table_meta(io) == test_meta
end

#=@testset "open table io" begin
    rm("test_tb", force = true, recursive = true) 

    @test_throws ErrorException DataFrameDBs.open_table("test_tb")
    test_table = DataFrameDBs.create_table("test_tb", [:a, :b], [Int32, Int64])
    @test isdir("test_tb")

    @test_throws ErrorException DataFrameDBs.create_table("test_tb", [:a], [Int32])


    res_tb = DataFrameDBs.open_table("test_tb")
    @test res_tb.meta == test_table.meta
    @test isfile(joinpath(res_tb.path, "1.bin"))
    @test isfile(joinpath(res_tb.path, "2.bin"))
    
    rm("test_tb", force = true, recursive = true) 
    
end=#