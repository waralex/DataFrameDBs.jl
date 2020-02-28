using DataFrameDBs: BlockBroadcasting, BroadcastExecutor, eval_on_range
@testset "broadcast" begin
    test_func(x) = x*2
    
    data = (
        a = collect(1:100),
        b = string.(collect(1:100)),
        c = collect(0.5:0.5:50)
    )
    

    test_b = BlockBroadcasting([Vector{Int64}], (:a,), test_func)
    @test eltype(test_b) == Int64

    exec = BroadcastExecutor(test_b)
    @test typeof(exec.buffer) == Vector{Int64}

    res = eval_on_range(data, exec, 1:100)
    @test res == data[:a] .* 2

    res = eval_on_range(data, exec, 1:5:100)
    @test res == data[:a][1:5:100] .* 2
    

    test_func2(a, b) = a/2 == b
    test_b = BlockBroadcasting([Vector{Int64}, Vector{Float64}], (:a,:c), test_func2)
    @test eltype(test_b) == Bool

    exec = BroadcastExecutor(test_b)
    @test typeof(exec.buffer) <: BitArray

    res = eval_on_range(data, exec, 1:100)
    @test res == (@. data[:a]/2 == data[:c])

end