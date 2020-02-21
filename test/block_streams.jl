@testset "write stream" begin
    io = PipeBuffer()
    wio = DataFrameDBs.BlockStream(io)
    test = 1000
    DataFrameDBs.write(wio, test)

    @test DataFrameDBs.read(wio, typeof(test)) == test
    @test eof(wio)    
end

@testset "write compressed" begin

    io = PipeBuffer()
    wio = DataFrameDBs.BlockStream(io)
    test = 1000
    DataFrameDBs.write(wio, test)
    test_a = rand(1:100000, 64000)
    DataFrameDBs.prepare_block_write!(wio, length(test_a)) do io
        write(io, test_a)
    end
    cz = DataFrameDBs.commit_block_write!(wio)        
    

    @test DataFrameDBs.read(wio, typeof(test)) == test
    
    cz = DataFrameDBs.read_block(wio) do rows, io
        res = similar(test_a)
        @test rows == length(test_a)
        read!(io, res)
        @test res == test_a
    end
    @test eof(wio)

end