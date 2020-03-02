using DataFrameDBs: DFTable, create_table, 
selection, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, DFColumn
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
    

    rm("test_data", force = true, recursive = true)
end