using DataFrameDBs: DFTable, create_table, 
selection, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, DFColumn, rows
using DataFrames
using InteractiveUtils

@testset "rows" begin
    #rm("test_data", force = true, recursive = true) 
    sz = 200
    df = DataFrame((
        a = collect(1:sz),        
        b = string.(collect(1:sz)),
        c = collect(1:sz)
    ))

    tb = create_table("test_data", from = df; block_size = 100)
    
    r = rows(tb)    
    i = 0
    for r in rows(tb[:,[:a,:c]])
        i += 1
        @test r == (a=i, c=i)        
    end
    @test i == sz
    
    @test tb[3,:] == (a=3, b="3", c=3)
    @test_throws BoundsError tb[202, :]
    @test tb[3,[:c,:a]] == (c=3, a=3)
    rm("test_data", force = true, recursive = true)
end