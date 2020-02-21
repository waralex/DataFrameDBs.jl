struct tst_1
    i::Int32
end
@testset "meta" begin
    @test_throws DataFrameDBs.ColumnTypes.UnsupportedType DataFrameDBs.ColumnMeta(:t, tst_1)
    @test_throws DataFrameDBs.ColumnTypes.UnsupportedType DataFrameDBs.ColumnMeta(1, :t, tst_1)
    
    test_meta = DataFrameDBs.DFTableMeta([DataFrameDBs.ColumnMeta(1, :test, Int64), DataFrameDBs.ColumnMeta(2, :test_2, String)])
    @test length(test_meta.columns) == 2
    test_meta2 = DataFrameDBs.DFTableMeta(
        [:test, :test_2], [Int64, String]
    )
    @test test_meta == test_meta2
end 

@testset "materialization" begin
    meta1 = DataFrameDBs.ColumnMeta(:a, Int64)
    meta2 = DataFrameDBs.ColumnMeta(:a, String)
    meta3 = DataFrameDBs.ColumnMeta(:a, Union{String, Missing})
    @test typeof(DataFrameDBs.make_materialization(meta1)) == Vector{Int64}
    @test typeof(DataFrameDBs.make_materialization(meta2)) == DataFrameDBs.FlatStringsVector{String}
    @test typeof(DataFrameDBs.make_materialization(meta3)) == DataFrameDBs.FlatStringsVector{Union{String, Missing}}
end 

@testset "open table filesystem" begin
    rm("test_tb", force = true, recursive = true) 

    @test_throws ErrorException  DataFrameDBs.open_table("test_tb")
    
    
    rm("test_tb", force = true, recursive = true) 
    test_meta = DataFrameDBs.DFTableMeta([:c,:a,:b], [Int32, Int64, String])
    test_table = DataFrameDBs.create_table("test_tb", [:c,:a,:b], [Int32, Int64, String])
    @test test_table.is_opened
    @test isdir("test_tb")
    @test isfile(joinpath("test_tb","meta.bin"))
    @test isfile(joinpath("test_tb","1.bin"))
    @test isfile(joinpath("test_tb","2.bin"))
    @test isfile(joinpath("test_tb","3.bin"))
    
    test_table2 = DataFrameDBs.open_table("test_tb")
    @test test_table2.is_opened
    @test test_table2.meta == test_meta

    open(joinpath("test_tb","3.bin"), "w") do io
        Base.write(io, Int64(10))
        DataFrameDBs.write_column_type(io, String)
    end
    
    @test_throws ErrorException DataFrameDBs.open_table("test_tb")
    
    open(joinpath("test_tb","3.bin"), "w") do io
        Base.write(io, Int64(DataFrameDBs.DEFAULT_BLOCK_SIZE))
        DataFrameDBs.write_column_type(io, Int64)
    end
    
    @test_throws ErrorException DataFrameDBs.open_table("test_tb")

    open(joinpath("test_tb","3.bin"), "w") do io
        Base.write(io, Int64(DataFrameDBs.DEFAULT_BLOCK_SIZE))
        DataFrameDBs.write_string(io, "Union")
        Base.write(io, Int16(2))
        DataFrameDBs.write_string(io, "Int32")
        DataFrameDBs.write_string(io, "Int64")
    end

    @test_throws DataFrameDBs.ColumnTypes.UndefinedType DataFrameDBs.open_table("test_tb")
    
    rm("test_tb", force = true, recursive = true) 
    
end