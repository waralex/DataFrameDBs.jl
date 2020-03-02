using DataFrameDBs: DFTable, create_table, 
selection, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, issameselection
using DataFrames
using InteractiveUtils
@testset "view" begin
    rm("test_data", force = true, recursive = true) 
    sz = 1000
    df = DataFrame((
        a = collect(1:sz),        
        b = string.(collect(1:sz)),
        c = collect(1:sz)
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
    

    dft = materialize(v1)
    @test df == dft
    @test DataFrameDBs.nrow(v1) == nrow(df)
    dft = materialize(v2)
    @test df == dft
    @test DataFrameDBs.nrow(v2) == nrow(df)
    
    @test size(v2) == size(df)
    @test size(v2,1) == size(df,1)
    @test size(v2,2) == size(df,2)

    dft = materialize(v3)
    @test df[df[!,:a] .% 50 .== 0, :] == dft
    @test DataFrameDBs.nrow(v3) == nrow(dft)
    

    dft = materialize(v4)
    ind = @. (df[!,:a] % 50 == 0) & (df[!, :c] < 930)
    @test DataFrame((a = df[ind, :a] ./ 50)) == dft
    @test DataFrameDBs.nrow(v4) == nrow(dft)
    @test size(v4) == size(dft)

    
    
    @test_throws ArgumentError v5 = projection(v4, (c=:c,))

    tv = projection(v1, [:a,:c])
    @test size(tv, 2) == 2
    @test materialize(tv) == df[:,[:a,:c]]

    tv = projection(v1, [:a=>:a,:c=>:c=>(c)->c*2])
    @test size(tv, 2) == 2
    @test materialize(tv) == DataFrame((a=df[:,:a], c=df[:,:c].*2))

    tv = projection(v1, [1,3])
    @test size(tv, 2) == 2
    @test materialize(tv) == df[:,[:a,:c]]

    tv = projection(v1, 1:2)
    @test size(tv, 2) == 2
    @test materialize(tv) == df[:,[:a,:b]]

    tv = selproj(v1, :a=>(a)->a%50==0, [:c])
    @test size(tv, 2) == 1
    @test materialize(tv) == df[df[!,:a] .% 50 .== 0, [:c]]

    tv = selproj(v1, 1, [:c])
    @test size(tv, 2) == 1
    @test materialize(tv) == df[[1], [:c]]

    tv = selproj(v1, [1,200], [:c])
    @test size(tv, 2) == 1
    @test materialize(tv) == df[[1,200], [:c]]


    #tv = selproj(v1, :a=>(a)->a%50==0, [:c])
    tv = v1[:a=>(a)->a%50==0, [:c]]
    
    @test size(tv, 2) == 1
    @test materialize(tv) == df[df[!,:a] .% 50 .== 0, [:c]]

    tv = v1[[1,200], [:c]]
    @test size(tv, 2) == 1
    @test materialize(tv) == df[[1,200], [:c]]

    tv = v1[1:200, :]
    @test size(tv, 2) == 3
    @test size(tv, 1) == 200
    @test materialize(tv) == df[1:200, :]

    tv = v1[:, (e=:a,)]
    @test size(tv, 2) == 1
    @test size(tv, 1) == 1000
    @test materialize(tv) == DataFrame((e = df[!,:a],))

    tv = v1[:, :]
    @test tv === v1

    tv = tb[:, (e=:a,)]
    @test size(tv, 2) == 1
    @test size(tv, 1) == 1000
    @test materialize(tv) == DataFrame((e = df[!,:a],))

    tv = tb[end-10:end, (e=:a,)]
    @test size(tv, 2) == 1
    @test size(tv, 1) == 11
    @test materialize(tv) == DataFrame((e = df[end-10:end,:a],))


    tv = tb[end-10:end, end-1:end]
    @test size(tv, 2) == 2
    @test size(tv, 1) == 11
    @test materialize(tv) == df[end-10:end, end-1:end]


    @test tb[1:20, [:a, :b]] == tb[1:20, [:a, :b]]

    @test tb[1:30, [:a, :b]] != tb[1:20, [:a, :b]]
    @test !issameselection(tb[1:30, [:a, :b]], tb[1:20, [:a, :b]])
    @test tb[1:20, [:a, :b]] != tb[1:20, [:b, :a]]
    tff(a) = a%50==0
    tv = selproj(v1, :a=>tff, [:c])
    tv2 = tb[:a=>tff,:]
    @test tv != tv2
    @test issameselection(tv, tv2)
    @test tv == tv2[:,[:c]]

    rm("test_data", force = true, recursive = true) 

end