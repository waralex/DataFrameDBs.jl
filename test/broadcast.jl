using DataFrameDBs: BlockBroadcasting, ColRef, required_columns,
     columns_buffers, BroadcastExecutor, eval_on_range
using InteractiveUtils
@testset "broadcast" begin
    test_func(x) = x*2
    test_func2(x, y) = x + y
    
    data = (
        a = collect(1:100),
        b = string.(collect(1:100)),
        c = collect(0.5:0.5:50)
    )
    

    test_b = BlockBroadcasting(
        test_func,
        (ColRef{Int64}(:a), )
        )
    @test eltype(test_b) == Int64

    test_b = BlockBroadcasting(
        test_func2,
        (ColRef{Int64}(:a), ColRef{Float64}(:c))
        )


    @test eltype(test_b) == Float64

    test_c = BlockBroadcasting(
        test_func2,
        (ColRef{Int64}(:a), test_b)
        )

        
    @test eltype(test_c) == Float64

    @test required_columns(test_c) == (:a, :c)
    
    

    @test columns_buffers(test_c) == (a = Int64[], c=Float64[])


    
    
    exec = BroadcastExecutor(test_c)
    res = eval_on_range(data, exec, 1:10:100)
    test_res = data.a[1:10:100] .*2 .+ data.c[1:10:100]
    @test res == test_res
    #println(@code_typed optimize=true eval_on_range(data, exec, 1:10:100))
    #println("===========")
    #@code_warntype eval_on_range(data, exec, 1:10:100)
    #=exec = BroadcastExecutor(test_b)
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


    test_c = BlockBroadcasting(
        [Vector{Int64}, typeof(test_b)], (:a, :e), 
        (a, e) -> a + e
    )

    exec = BroadcastExecutor(test_c)
    @test typeof(exec.buffer) == Vector{Int64}

    res = eval_on_range(data, exec, 1:100)
    println(res)=#
end