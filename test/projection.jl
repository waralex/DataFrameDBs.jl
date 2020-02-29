using DataFrameDBs: BlockBroadcasting, ColRef, required_columns,
     columns_buffers, BroadcastExecutor, eval_on_range, Projection,
     add, ProjectionExecutor, eval_on_range
using InteractiveUtils

@testset "projection" begin
    test_func(x) = x*2    

    test_p = Projection((a = ColRef{Int64}(:a), ))

    test_p = Projection(
        (
            a = ColRef{Int64}(:a), 
            b = BlockBroadcasting(
                test_func,
                (ColRef{Int64}(:a), )
            )
        )
        )
    @test keys(test_p) == (:a, :b)

    @test_throws ArgumentError add(test_p, (a=ColRef{Int64}(:a),))

    @test_throws ArgumentError add(
        test_p, 
        (a=ColRef{Int64}(:a), b = ColRef{Float64}(:a))
        )
    test_p = Projection((a = ColRef{Int64}(:a), ))
    
    test_p2 = add(test_p, (c = ColRef{Float64}(:b), ))
    @test keys(test_p2) == (:a, :c)

    test_p2 = add(test_p, 
        (c = ColRef{Float64}(:a), e = ColRef{Float64}(:e))
        )
    @test keys(test_p2) == (:a, :c, :e)

    l = test_p2[2]
    @test keys(l) == (:c,)

    l = test_p2[1:2]
    @test keys(l) == (:a, :c,)

    l = test_p2[[1,3]]
    @test keys(l) == (:a, :e,)
    

    l = test_p2[:c]
    

    l = test_p2[[:a, :e]]
    


    data = (
        a = collect(1:100),
        b = string.(collect(1:100)),
        c = collect(0.5:0.5:50)
    )
    test_p = Projection((a = ColRef{Int64}(:a), ))
    exec_p = ProjectionExecutor(test_p)
    
    res = eval_on_range(data, exec_p, 1:10:100)
    @test res.a == data.a[1:10:100]

    test_p = add(
        test_p,
         (
             b = BlockBroadcasting(test_func, (ColRef{Int64}(:a),))
         ,)
        )
    exec_p = ProjectionExecutor(test_p)
    res = eval_on_range(data, exec_p, 1:10:100)
    
    @test res.a == data.a[1:10:100]
    @test res.b == (data.a[1:10:100] .* 2)

   
end