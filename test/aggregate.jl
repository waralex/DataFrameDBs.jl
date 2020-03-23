using DataFrameDBs: DFTable, create_table, 
selection, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, issameselection
using DataFrames
using OnlineStats
using InteractiveUtils
@testset "aggr" begin
    rm("test_data", force = true, recursive = true)
    sz = 1000
    df = DataFrame((
        a = rand(1:5, sz),        
        b = string.(collect(1:sz)),
        c = collect(1:sz)
    ))

    tb = create_table("test_data", from = df; block_size = 100)
    
    v = tb[:, [:a,:b]]

    groupreduce(tb[:,:], (:a,), c = :c => Mean())
    
    

    rm("test_data", force = true, recursive = true)
end