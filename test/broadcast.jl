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

    test_e = BlockBroadcasting(
        test_func2,
        (ColRef{Int64}(:a), 20)
        )

    exec = BroadcastExecutor(test_e)
    res = eval_on_range(data, exec, 1:10:100)
    @test res == data.a[1:10:100] .+ 20

    test_e = BlockBroadcasting(
        in,
        (ColRef{Int64}(:a), Ref([1,11,21]))
        )

    exec = BroadcastExecutor(test_e)
    res = eval_on_range(data, exec, 1:10:100)
    
    @test res == in.(data.a[1:10:100], Ref([1,11,21]))

    @test_throws ArgumentError test_e = BlockBroadcasting(
        in,
        (ColRef{Int64}(:a), [1,11,21])
        )

    @test_throws ArgumentError test_e = BlockBroadcasting(
        in,
        (ColRef{Int64}(:a), zip([1,11,21], [1,11,21]))
        )
    
    
end