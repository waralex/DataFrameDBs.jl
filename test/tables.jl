
@testset "base" begin
    test_tb = DataFrameDBs.DFTable("test_tb", DataFrameDBs.DFTableMeta([:col_a, :col_b]))
    @test test_tb.path == "test_tb"
    @test test_tb.meta.rows == 0
    @test test_tb.meta.columns == [:col_a=>1, :col_b=>2]
end 