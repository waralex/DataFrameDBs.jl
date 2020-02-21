@testset "missings" begin
    
    io = PipeBuffer()
    test = [1, missing, 2, 3, missing, 5, 6, missing, 10, 11, missing]
    DataFrameDBs.write_block_body(io, test)
    res = empty(test)
    
    DataFrameDBs.read_block_body!(io, res, length(test))
    @test all(res .=== test)
    @test eof(io)
end