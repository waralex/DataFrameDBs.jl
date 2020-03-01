using DataFrameDBs: TableView, DFTable, create_table, 
read_range, eachprojection, selection, eachsize, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize
using DataFrames
using InteractiveUtils
@testset "view" begin
    rm("test_data", force = true, recursive = true) 
    size = 1000
    df = DataFrame((
        a = collect(1:size),        
        b = string.(collect(1:size)),
        c = collect(1:size)
    ))

    tb = create_table("test_data", from = df; block_size = 100)

    @test names(tb) == names(df)

    v1 = DFView(tb)
    v2 = selection(v1, 1:1000)
    v3 = selection(v2, (:a,) => (a)->a % 50 == 0)
    v4 = selection(v3, :c => (a)->a < 930)
    v4 = projection(v4, (a=(:a,)=> (a)->a / 50, ))
    
    it = BlocksIterator(v4)
    #println(it)
    #println(@code_typed optimize=true iterate(it))
    
    while true
        @time r = iterate(it)
        println(r)
        isnothing(r) && break
    end
    
    materialize(v4)

    pv1 = projection(v1, (e=:a,))
    #println(pv1)
    pv2 = projection(v1, (e = (:a,)=>(a)->a*2, a=:c))
    @test required_columns(pv2) == (:a, :c)
    
    #println(pv2)
    pv3 = selection(pv2, :e => (e)-> e < 10)
    #println("*** ", pv3)
    pv4 = projection(pv2, (k=:e,))
    #println("vv ", pv4)
    pv6 = projection(pv4, (с = (:k,)=>(k)->k+5,))
    #println("vv+ ", pv6)

    #println("====")
    #println(proj_elem(v1, :a))
    #println("====")
    #println(proj_elem(v1, (:a,) => (a)->a*2 ))

    rm("test_data", force = true, recursive = true) 

end