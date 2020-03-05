using DataFrameDBs: DFTable, create_table, 
selection, projection, rows,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, DFColumn, map_to_column
using DataFrames
using InteractiveUtils
@testset "columns" begin
    rm("test_data", force = true, recursive = true) 
    sz = 1000
    df = DataFrame((
        a = collect(1:sz),        
        b = string.(collect(1:sz)),
        c = collect(1:sz)
    ))

    tb = create_table("test_data", from = df; block_size = 100)

    col = tb[:, 1] 
    
    
    @test typeof(col) <: DFColumn
    @test typeof(tb[:,[1]]) <: DFView
    @test typeof(tb[:,[:a]]) <: DFView
    @test typeof(tb[:,:a]) <: DFColumn
    @test typeof(tb[1:5:end,:a]) <: DFColumn


    #@test typeof(col) <: AbstractVector{Int64}

    @test length(col) == sz

    @test materialize(col) == df[!,:a]
    
    @test eltype(col) == Int64
    @test collect(col) == materialize(col)    
    @test unique(col) == collect(col)    
    col2 = col[90:110]
    @test collect(col2) == df[90:110, :a]

    @test col2[1] == 90
    @test col2[12] == 101
    
    col3 = tb[:, (:a,:c)=>(a,c)->a+c*2]
    @test col3 |>materialize == df.a .+ df.c .* 2

    col3 = tb[:, :a=>(a)->a*4]
    @test col3 |>materialize == df.a .* 4

    @test tb.a == tb[:, :a]
    @test tb[1:20, :].a == tb[1:20, :a]

    r = map_to_column(tb[:, [:a, :c]]) do a, c        
        2a - c
    end

    r2 = map_to_column(tb) do a, b, c        
        2a - c
    end

    @test r == r2 
    @test materialize(r2) == @. 2*df.a - df.c

    r3 = map_to_column(r2) do a
        a * 3
    end

    @test materialize(r3) == @. (2*df.a - df.c) * 3

    test = tb[:,:]
    test.e = map_to_column(test) do a, b, c
        a+c
    end
    
    df.e = df.a .+ df.c
    @test materialize(test) == df


    rm("test_data", force = true, recursive = true)
end