@testset "number block" begin
    test_a = rand(Int32, 100)
    size = 100
    test_b = DateTime.(rand(2010:2020, size), rand(1:12, size), rand(1:20, size),
    rand(0:23, size), rand(0:59, size), rand(0:59, size))
    io = PipeBuffer()
    DataFrameDBs.write_block(io, test_a)
    DataFrameDBs.write_block(io, test_b)
    res_a = Int32[]
    res_b = DateTime[]
    DataFrameDBs.read_block!(io, res_a)
    DataFrameDBs.read_block!(io, res_b)
    @test length(res_a) == length(test_a)
    @test length(res_b) == length(test_b)
    @test res_a == test_a
    @test res_b == test_b
    @test eof(io)
end

@testset "string block" begin
    test_a = string.(rand(Int32, 100))
    io = PipeBuffer()
    DataFrameDBs.write_block(io, test_a)

    res_a = DataFrameDBs.FlatStringsVector()
    DataFrameDBs.read_block!(io, res_a)
    @test length(res_a) == length(test_a)
    @test res_a == test_a
    @test eof(io)
end


@testset "number data" begin
    size = 300
    block_size = 100
    test_a = rand(Int32, size)
    
    test_b = DateTime.(rand(2010:2020, size), rand(1:12, size), rand(1:20, size),
    rand(0:23, size), rand(0:59, size), rand(0:59, size))
    io = PipeBuffer()

    DataFrameDBs.write_column_data(io, test_a, block_size)
    res_a = Int32[]
    DataFrameDBs.read_column_data!(io, res_a, block_size)
    @test length(res_a) == length(test_a)
    @test res_a == test_a
    @test eof(io)

    DataFrameDBs.write_column_data(io, test_b, block_size)
    
    res_b = DateTime[]
    
    DataFrameDBs.read_column_data!(io, res_b, block_size)
    
    @test length(res_b) == length(test_b)
    
    @test res_b == test_b
    @test eof(io)
end