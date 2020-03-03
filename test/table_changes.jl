using DataFrameDBs: DFTable, create_table, 
selection, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, issameselection,
table_exists, drop_table!, truncate_table!, insert, rename_column!, open_table, drop_column!, add_column!
using DataFrames
using InteractiveUtils
@testset "drop truncate rename" begin
    rm("test_data", force = true, recursive = true) 
    sz = 1000
    df = DataFrame((
        a = collect(1:sz),        
        b = string.(collect(1:sz)),
        c = collect(Int16, 1:sz)
    ))

    tb = create_table("test_data", from = df; block_size = 100)
    @test table_exists(tb.path)
    
    drop_table!(tb)
    @test !table_exists(tb.path)

    tb = create_table("test_data", from = df; block_size = 100)
    @test size(tb) == size(df)
    truncate_table!(tb)
    @test size(tb) == (0, 3)
    @test names(tb) == [:a, :b, :c]
    insert(tb, df)
    @test size(tb) == size(df)

    @test_throws ArgumentError rename_column!(tb, :a, :c)

    rename_column!(tb, :a, :f)
    @test materialize(tb) == DataFrame((f=df[:,:a], b=df[:,:b], c=df[:,:c]))

    tb = open_table("test_data")
    @test materialize(tb) == DataFrame((f=df[:,:a], b=df[:,:b], c=df[:,:c]))
    drop_table!(tb)
    @test !table_exists(tb.path)
    tb = create_table("test_data", from = df; block_size = 100)
    @test_throws KeyError drop_column!(tb, :f)

    @test ispath("test_data/3.bin")

    drop_column!(tb, :c)
    @test !ispath("test_data/3.bin")
    
    @test names(tb) == [:a,:b]
    tb = open_table("test_data")
    @test names(tb) == [:a,:b]
    @test materialize(tb) == df[:,[:a,:b]]
    


    rm("test_data", force = true, recursive = true) 
end

@testset "add_column" begin
    rm("test_data", force = true, recursive = true) 
    sz = 1000
    df = DataFrame((
        a = collect(1:sz),        
        b = string.(collect(1:sz)),
        c = collect(Int16, 1:sz)
    ))
    tb = create_table("test_data", from = df; block_size = 100)

    @test_throws ArgumentError add_column!(tb, :c, [])
    @test_throws ArgumentError add_column!(tb, :e, [])
    add_column!(tb, :e, (1:1000).+2000)
    @test materialize(tb) == DataFrame(
        (a = df.a, b = df.b, c= df.c, e = (1:1000).+2000)
    )
    drop_table!(tb)

    tb = create_table("test_data", from = df; block_size = 100)
    add_column!(tb, :e, (1:1000).+2000, before = :a)
    @test materialize(tb) == DataFrame(
        (e = (1:1000).+2000, a = df.a, b = df.b, c= df.c)
    )

    drop_table!(tb)
    
    tb = create_table("test_data", from = df; block_size = 100)
    add_column!(tb, :e, (1:1000).+2000, before = :c)
    @test materialize(tb) == DataFrame(
        (a = df.a, b = df.b, e = (1:1000).+2000, c= df.c)
    )
    tb = open_table("test_data")
    @test materialize(tb) == DataFrame(
        (a = df.a, b = df.b, e = (1:1000).+2000, c= df.c)
    )

    drop_table!(tb)
    
    tb = create_table("test_data", from = df; block_size = 100)
    add_column!(tb, :e, zip(df.a, df.c))
    @test materialize(tb[:,:e]) == collect(zip(df.a, df.c))

    drop_table!(tb)

    tb = create_table("test_data", from = df; block_size = 100)
    tb2 = create_table("test_data2", from = df; block_size = 100)
    v1 = tb[:, (c = :c=> (c)->c*3,)][:,:c]
    add_column!(tb, :e, v1)
    @test materialize(tb[:,:e]) == df.c .* 3

    v2 = tb2[:, (c = :c=> (c)->c*3,)][:,:c]
    add_column!(tb, :f, v2)
    @test materialize(tb[:,:f]) == df.c .* 3

    drop_table!(tb)
    drop_table!(tb2)

    rm("test_data", force = true, recursive = true) 
end