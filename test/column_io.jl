
@testset "number block" begin
    test_a = rand(Int32, 100)
    size = 100
    test_b = DateTime.(rand(2010:2020, size), rand(1:12, size), rand(1:20, size),
    rand(0:23, size), rand(0:59, size), rand(0:59, size))
    io = PipeBuffer()
    DataFrameDBs.write_block_body(io, test_a)
    DataFrameDBs.write_block_body(io, test_b)
    res_a = Int32[]
    res_b = DateTime[]
    DataFrameDBs.read_block_body!(io, res_a, length(test_a))
    DataFrameDBs.read_block_body!(io, res_b, length(test_b))
    @test length(res_a) == length(test_a)
    @test length(res_b) == length(test_b)
    @test res_a == test_a
    @test res_b == test_b
    @test eof(io)
end

@testset "string block" begin
    test_a = string.(rand(Int32, 100))
    test_b = DataFrameDBs.FlatStringsVector{String}(test_a)    
    
    io = PipeBuffer()
    DataFrameDBs.write_block_body(io, test_a)

    res_a = DataFrameDBs.FlatStringsVector{String}()
    DataFrameDBs.read_block_body!(io, res_a, length(test_a))
    @test length(res_a) == length(test_a)
    @test res_a.sizes == test_b.sizes
    @test res_a == test_a
    @test eof(io)
end

@testset "single column" begin
    
    test_a = string.(rand(Int32, 1000))
    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400; close_on_done = false)
    
    res = DataFrameDBs.FlatStringsVector{String}()
    i = 0
    
    for block in DataFrameDBs.eachblock([io], [DataFrameDBs.ColumnMeta(:a, String)])
        
        append!(res, block[:a])
        
    end
    
    @test res == test_a
    @test eof(io)

    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400)
    
    #@test_throws ErrorException DataFrameDBs.eachblock([io], [DataFrameDBs.ColumnMeta(:a, Int32)])

end

@testset "single column sizes" begin
    
    test_a = rand(Int32, 1000)
    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400; close_on_done = false)
    
    res = DataFrameDBs.FlatStringsVector{String}()
    i = 0
    res_rows = 0
    res_uncomp = 0
    stats = DataFrameDBs.SizeStats()
    for block in DataFrameDBs.eachsize([io], [DataFrameDBs.ColumnMeta(:a, Int32)])
        
        stats += block[:a]
        
        
    end

    @test stats.rows == length(test_a)
    @test stats.uncompressed == sizeof(test_a)
    
    @test eof(io)

    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400)
    
    #@test_throws ErrorException DataFrameDBs.eachblock([io], [DataFrameDBs.ColumnMeta(:a, Int32)])

end

@testset "missing column" begin
    
    size = 1000
    test_a_unmiss = rand(Int32, size)
    bits = BitArray(rand(Bool, size))
    test_a = [bits[i] ? missing : test_a_unmiss[i] for i in 1:size]
    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400; close_on_done = false)
    

    meta = DataFrameDBs.ColumnMeta(:a, Union{Missing, Int32})

    res = DataFrameDBs.make_materialization(meta)
    i = 0
    
    for block in DataFrameDBs.eachblock([io], [meta])
        
        append!(res, block[:a])
        
    end
    
    @test all(res .=== test_a)
    @test eof(io)

    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400)
    
    #@test_throws ErrorException DataFrameDBs.eachblock([io], [DataFrameDBs.ColumnMeta(:a, Int32)])

end

@testset "missing string column" begin
    
    size = 1000
    test_a_unmiss = string.(rand(Int32, size))
    bits = BitArray(rand(Bool, size))
    test_a = [bits[i] ? missing : test_a_unmiss[i] for i in 1:size]
    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400; close_on_done = false)
    

    meta = DataFrameDBs.ColumnMeta(:a, Union{Missing, String})

    res = DataFrameDBs.make_materialization(meta)
    
    i = 0
    
    for block in DataFrameDBs.eachblock([io], [meta])
        
        append!(res, block[:a])
        
    end
    
    @test length(res) == length(test_a)
    for i in 1:length(test_a)
        @test ismissing(res[i]) == ismissing(test_a[i])
        if !ismissing(res[i])
            @test res[i] == test_a[i]
        end
    end

    @test eof(io)

    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400)
    
    #@test_throws ErrorException DataFrameDBs.eachblock([io], [DataFrameDBs.ColumnMeta(:a, Int32)])

end


@testset "missing column custom struct" begin
    struct test_struct
        a ::Int64
        b ::Float32
    end
    DataFrameDBs.ColumnTypes.serialize(::Type{test_struct}) = DataFrameDBs.ColumnTypes.Ast(Symbol(test_struct))
    DataFrameDBs.ColumnTypes.deserialize(::Val{Symbol(test_struct)}) = test_struct
    
    size = 1000
    test_a_unmiss = test_struct.(rand(Int64, size), rand(Float32, size))
    bits = BitArray(rand(Bool, size))
    test_a = [bits[i] ? missing : test_a_unmiss[i] for i in 1:size]
    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400; close_on_done = false)
    

    meta = DataFrameDBs.ColumnMeta(:a, Union{Missing, test_struct})

    res = DataFrameDBs.make_materialization(meta)
    i = 0
    
    for block in DataFrameDBs.eachblock([io], [meta])
        
        append!(res, block[:a])
        
    end
    
    @test all(res .=== test_a)
    @test eof(io)

end

@testset "tuple column" begin
    
    size = 1000
    test_a = tuple.(rand(Int64, size), rand(Float32, size))
    
    
    io = PipeBuffer()
    DataFrameDBs.write_columns([io], [test_a], 400; close_on_done = false)
    

    meta = DataFrameDBs.ColumnMeta(:a, eltype(test_a))

    res = DataFrameDBs.make_materialization(meta)
    i = 0
    
    for block in DataFrameDBs.eachblock([io], [meta])
        
        append!(res, block[:a])
        
    end
    
    @test all(res .=== test_a)
    @test eof(io)

end

@testset "multi column" begin
    size = 1000
    test = [
        string.(rand(Int32, size)),
        rand(Int32, size),
        rand(UInt64, size)
    ]

    meta = [
        DataFrameDBs.ColumnMeta(:a, String),
        DataFrameDBs.ColumnMeta(:b, Int32),
        DataFrameDBs.ColumnMeta(:c, UInt64),
    ]
    
    io = [PipeBuffer(), PipeBuffer(), PipeBuffer()]
    DataFrameDBs.write_columns(io, test, 400; close_on_done = false)
    
    res = DataFrameDBs.make_materialization.(meta)
    i = 0

    for block in DataFrameDBs.eachblock(io, meta)
        
        for (i, b) in enumerate(block)
            @test meta[i].name == b[1]
        end
        
        append!(res[1], block[:a])
        append!(res[2], block[:b])
        append!(res[3], block[:c])
        
    end
    
    for i in 1:length(test)
        @test test[i] == res[i]
    end
    
    for i in io
        @test eof(i)
    end

end