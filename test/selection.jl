using DataFrameDBs: SelectionQueue, BlockBroadcasting, BroadcastExecutor, add, SelectionExecutor, apply, is_finished, ColRef
using InteractiveUtils
@testset "selection" begin
    @test true
    sel = SelectionQueue()
    @test isempty(sel)
    sel2 = SelectionQueue((1:30, 1:2:30))
    @test typeof(sel2) == SelectionQueue{Tuple{UnitRange{Int64}, StepRange{Int64, Int64}}}

    test_b = BlockBroadcasting((a)->a==1, (ColRef{Int64}(:a),))
    sel3 = SelectionQueue((1:30, test_b, 1:2:30))
    @test typeof(sel3) == SelectionQueue{Tuple{UnitRange{Int64}, typeof(test_b), StepRange{Int64, Int64}}}

    sel2 = add(sel, :)
    @test isempty(sel2)
    sel2 = add(sel, 5:20)
    @test length(sel2) == 1
    @test typeof(sel2) == SelectionQueue{Tuple{UnitRange{Int64}}}
    sel2 = add(sel2, 1:5)
    @test length(sel2) == 1
    @test typeof(sel2) == SelectionQueue{Tuple{UnitRange{Int64}}}
    @test first(sel2.queue) == 5:9
    sel2 = add(sel2, test_b)
    @test typeof(sel2) == SelectionQueue{Tuple{UnitRange{Int64}, typeof(test_b)}}

    sel3 = add(sel, test_b)
    @test length(sel3) == 1
    @test typeof(sel3) == SelectionQueue{Tuple{typeof(test_b)}}
    sel3 = add(sel3, test_b)
    @test length(sel3) == 1
    
    sel3 = add(sel3, 1:3)
    @test length(sel3) == 2
    

    test_c = BlockBroadcasting((a)->a*3, (ColRef{Int64}(:a),))
    @test_throws ArgumentError sel3 = add(sel3, test_c)

    
    exe_sel1 = SelectionQueue()
    exe_sel1 = add(exe_sel1, 5:20)
    exe_sel1 = add(exe_sel1, 3:4)
    
    exe = SelectionExecutor(exe_sel1)

    test_data1 = (a = collect(1:100),)

    r = apply(exe, 50, nothing)    
    @test r == 7:8

    exe_sel1 = SelectionQueue()
    exe_sel1 = add(exe_sel1, 10:60)
    exe_sel1 = add(exe_sel1, BlockBroadcasting((a)->65>a>34, (ColRef{Int64}(:a),)))
    exe_sel1 = add(exe_sel1, 15:18)
    
    exe = SelectionExecutor(exe_sel1)
    r = apply(exe, 100, test_data1)
    @test r == 49:52
    @test test_data1.a[r] == 49:52
    @test is_finished(exe)

    exe = SelectionExecutor(exe_sel1)
    res = Int64[]
    range = 1:50
    for i in 1:2
        part = (a = test_data1.a[range],)
        @test !is_finished(exe)
        append!(res, part.a[apply(exe, 50, part)])
        range = range.+50
    end
    @test res == 49:52
    @test is_finished(exe)

    test_data2 = (
        a = collect(1:100),
        b = collect((1:100) .* 5),
        )
    
    exe_sel1 = SelectionQueue()
    exe_sel1 = add(
        exe_sel1, 
        BlockBroadcasting((a)->65>a>34, (ColRef{Int64}(:a),))
        )

    exe_sel1 = add(
        exe_sel1, 
        BlockBroadcasting((b)->b % 10 == 0, (ColRef{Int64}(:b),))
        )
    @test length(exe_sel1) == 1
    exe = SelectionExecutor(exe_sel1)
    res = (a = Int64[], b = Int64[])
    range = 1:50
    for i in 1:2
        part = (a = test_data2.a[range], b = test_data2.b[range])
        @test !is_finished(exe)
        r = apply(exe, 50, part)
        append!(res.a, part.a[r])
        append!(res.b, part.b[r])

        range = range.+50
    end
    
    test_range = @. (65 > test_data2.a > 34) & (test_data2.b % 10 == 0)

    @test res.a == test_data2.a[test_range]
    @test res.b == test_data2.b[test_range]
    

end