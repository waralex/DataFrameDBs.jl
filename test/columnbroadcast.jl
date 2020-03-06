using DataFrameDBs: DFTable, create_table, 
selection, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, DFColumn, DFColumnStyle
using DataFrames
using InteractiveUtils
using Base.Broadcast: BroadcastStyle
@testset "column broadcast" begin
    rm("test_data", force = true, recursive = true) 
    sz = 1000
    df = DataFrame((
        a = collect(1:sz),        
        b = string.(collect(1:sz)),
        c = collect(1:sz)
    ))

    tb = create_table("test_data", from = df; block_size = 100)

    @test BroadcastStyle(BroadcastStyle(typeof(tb.a)), BroadcastStyle(Int64)) == DFColumnStyle()
    
    @test BroadcastStyle(BroadcastStyle(typeof(tb.a)), BroadcastStyle(typeof((1,2,3)))) == DFColumnStyle()

    @test BroadcastStyle(BroadcastStyle(typeof(tb.a)), BroadcastStyle(typeof([1,2,3]))) == Base.Broadcast.DefaultArrayStyle{1}()

    @test BroadcastStyle(BroadcastStyle(typeof(tb.a)), BroadcastStyle(typeof(Ref([1,2,3])))) == DFColumnStyle()

    @test_throws ArgumentError tb.a[1:20] .+ tb.c[11:30]

    r = tb.a[1:20] .+ 20
    @test materialize(r) == df.a[1:20] .+ 20
    @test materialize(tb.a[1:20] .* tb.a[1:20]) == df.a[1:20] .* df.a[1:20]
    @test materialize(tb.a[1:20] .* tb.a[1:20] .- 20) == df.a[1:20] .* df.a[1:20] .- 20
    @test materialize(tb.a .* tb.c) == df.a .* df.c
    @test materialize(tb.a .== 10) == (df.a .== 10)

    #@test (tb.a .* df.a) == (df.a .* df.a)

    test_t = Vector{Int64}(undef, sz)

    test_t .= tb.a .* tb.c
    @test test_t == (df.a .* df.c)

    test_t .= tb.a
    @test test_t == df.a

    test2 = 300 .>= tb.a .>= 10
    tb2 = tb[test2, :]
    df2 = df[300 .>= df.a .>= 10, :]
    @test materialize(tb2) == df2

    tb3 = tb2[startswith.(tb2.b, "1"), :]
    df3 = df2[startswith.(df2.b, "1"), :]
    
    @test materialize(tb3) == df3

    v = DFView((a = tb.a .* 3, g = tb.a.*tb.c))
    
    @test materialize(v) == DataFrame((a = df.a .* 3, g = df.a.*df.c))

    v = DFView(a = tb.a .* 3, g = tb.a.*tb.c)
    @test materialize(v) == DataFrame((a = df.a .* 3, g = df.a.*df.c))

    rm("test_data", force = true, recursive = true)
end 