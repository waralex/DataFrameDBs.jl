using DataFrameDBs: create_table, table_stats, materialize, nrows
using DataFrames
@testset "range indexing" begin
    rm("test_data", force = true, recursive = true) 
    size = 1000
    df = DataFrame((
        a = collect(1:size),
        c = collect(1:size),
        b = string.(collect(1:size))
    ))

    tb = create_table("test_data", from = df; block_size = 100)
    
    
    @test df[:, [:a]] == materialize(tb[:, :a])    
    @test tb[:, [1,3]] == tb[:, [1, 3]]
    @test tb[:, [1,3]] != tb[:, [1, 2]]
    @test tb[:, 1:end] == tb[:, 1:3]
    @test tb[:, 2:3] == tb[:,[2,3]]
    @test tb[:, 2:3] != tb[:,[1,3]]

    t1 = tb[1:1:5, :]
    @test tb[1:1:5, :].row_index == 1:1:5
    @test tb[1:5, :].row_index == 1:5
    @test tb[[1,5,6], :].row_index == [1,5,6]
    @test tb[6, :].row_index == 6

    @test tb[1:1:5, :][:, :].row_index == 1:1:5

    @test tb[1:1:5, :][:, :] == tb[1:1:5, :]
    
    t = tb[5:100, :]
    @test t[3:5, :].row_index == 7:9
    @test t[3:5, :][2, :].row_index == 8 
    
    @test materialize(tb[5:60, :]) == df[5:60,:]
    @test materialize(tb[5:300, :]) == df[5:300,:]
    @test materialize(tb[5:300:1000, :]) == df[5:300:1000,:]
    @test materialize(tb[[1,200,20], :]) == df[[1,20,200],:]
    
    @test nrows(tb[5:60, :]) == length(5:60)
    @test nrows(tb[5:2:60, :]) == length(5:2:60)

    @test materialize(tb[end-20:end, :])== df[end-20:end, :]
    #println(t1.row_index)
    #println(tb[1:1:5, :])
    #println(typeof(tb[1:5, :]))
    #=tb[1:5,:a]
    println("====")
    tb[1:2:10,:a]
    println("====")
    tb[1,:a]
    println("====")
    tb[[1,2,3,4],:a]=#
    rm("test_data", force = true, recursive = true) 
end