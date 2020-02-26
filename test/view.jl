using DataFrameDBs: TableView, DFTable, create_table, read_range, eachprojection, selection, eachsize
using DataFrames
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

    view = tb[:, :]
    
    @test typeof(view) <: TableView
    @test names(view) == names(df)
    
    view2 = view[:, :]
    @test typeof(view2) <: TableView
    @test names(view2) == names(df)

    view2 = view[:, 1]
    @test typeof(view2) <: TableView
    @test names(view2) == [:a]

    view2 = view[:, [1,2]]
    @test typeof(view2) <: TableView
    @test names(view2) == [:a, :b]

    view2 = view[:, [:a]]
    @test typeof(view2) <: TableView
    @test names(view2) == [:a]
    view2 = view[:, [:a, :b]]
    @test typeof(view2) <: TableView
    @test names(view2) == [:a, :b]
    view2 = view[:, 2:3]
    @test typeof(view2) <: TableView
    @test names(view2) == [:b, :c]
    
    view3 = tb[1:10, :]
    @test typeof(view3) <: TableView
    @test read_range(view3.filter) == 1:10
    @test read_range(view3[3:4, :].filter) == 3:4
    @test read_range(view3[:, :].filter) == read_range(view3.filter)
    @test view3.filter === view3.filter
    @test view3[:, :].filter !== view3.filter

    view_iter = tb[101:105, [1]]
    
    iter = eachprojection(view_iter)
    
    for block in iter
        @test block[:a] == df[101:105,:a]
    end

    view_iter2 = selection(tb, (:a,) => (a) -> a < 10)
    view_iter2 = selection(view_iter2, (:c,) => (c) -> c > 5)

    iter = eachprojection(view_iter2)
    
    for block in iter
        
        @test block[:a] == Int64[6, 7, 8, 9]
    end


    rm("test_data", force = true, recursive = true) 

end