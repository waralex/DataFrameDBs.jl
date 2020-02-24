using DataFrameDBs: create_table, table_stats,
         materialize, nrows, FuncFilter, FilterQueue, add, read_range,
        required_columns, extract_args, eval_on_range, apply, iscompleted
@testset "filter construction" begin
    @test_throws ArgumentError FuncFilter([Vector{Int32}, Vector{Int64}], (:a, :b), (a, b, c)->true)
    @test_throws ArgumentError FuncFilter([Vector{Int32}, Vector{Int64}], (:a, :b), (a::String, b)->true)
    @test_throws ArgumentError FuncFilter([Vector{Int32}, Vector{Int64}],(:a, :b), (a, b)->1)

    @test_throws ArgumentError FuncFilter([Vector{Union{Int32, Missing}}, Vector{Int64}],(:a, :b), (a, b)->a==1)
    
    FuncFilter([Vector{Union{Int32, Missing}}, Vector{Int64}],(:a, :b), (a, b)->ismissing(a))
    @test true

    @test_throws ArgumentError FuncFilter([Union{Int32, Missing}, Vector{Int64}],(:a, :b), (a, b)->ismissing(a))

    

    test_f = FuncFilter([Vector{Int64}, Vector{Int64}], (:a, :c), (a,c)->a==c)

    test_data = Dict(
        :a=>[1,2,3,4,5,6,7,8],
        :b=>[3,2,2,3,5,7,7,10],
        :c=>[4,2,3,4,5,7,7,8],
                    )

    tt = extract_args(test_data, test_f)                    
    @test length(tt) == 2
    @test tt[1] == test_data[:a]
    @test tt[2] == test_data[:c]
    
    r = eval_on_range(test_data, test_f, 1:length(test_data[:a]))
    
    @test length(r) == sum(test_data[:a].==test_data[:c])
    @test test_data[:a][r] == test_data[:a][test_data[:a].==test_data[:c]]

    test_f2 = FuncFilter([Vector{Int64}, Vector{Int64}], (:a, :b), (a,c)->a!=c)
    
    r2 = eval_on_range(test_data, test_f2, r)
    
    @test test_data[:a][r2] == test_data[:a][(test_data[:a] .== test_data[:c]).&(test_data[:a] .!= test_data[:b])]

    queue = FilterQueue()
    nq = add(queue, 2:5)
    nq = add(nq, test_f)
    @test isnothing(read_range(queue))
    @test read_range(nq) == 2:5
    @test required_columns(queue) == Symbol[]
    @test required_columns(nq) == Symbol[:a,:c]
    nq2 = add(nq, test_f2)
    @test required_columns(nq2) == Symbol[:a,:c,:b]

    res = apply(nq, test_data)
    
    @test test_data[:a][res] == test_data[:a][2:5][test_data[:a][2:5] .== test_data[:c][2:5]]
    @test test_data[:b][res] == test_data[:b][2:5][test_data[:a][2:5] .== test_data[:c][2:5]]

    res = apply(nq2, test_data)
    cond = (test_data[:a][2:5] .== test_data[:c][2:5]) .& (test_data[:a][2:5] .!= test_data[:b][2:5])
    @test test_data[:a][res] == test_data[:a][2:5][cond]
    @test test_data[:b][res] == test_data[:b][2:5][cond]
    

    nq3 = add(nq2, 1)
    
    res = apply(nq3, test_data)
    println(res)
    @test test_data[:a][res] == test_data[:a][2:5][cond][[1]]
    @test test_data[:b][res] == test_data[:b][2:5][cond][[1]]

end

@testset "next chunk" begin
    test_f = FuncFilter([Vector{Int64}, Vector{Int64}], (:a, :c), (a,c)->a==c)

    test_data = Dict(
        :a=>[1,2,3,4,5,6,7,8],
        :b=>[3,2,2,3,5,7,7,10],
        :c=>[4,2,3,4,5,7,7,8],
                    )
    test_data_1 = Dict(
        :a => test_data[:a][1:4],
        :b => test_data[:b][1:4],
        :c => test_data[:c][1:4],
    )
    test_data_2 = Dict(
        :a => test_data[:a][5:end],
        :b => test_data[:b][5:end],
        :c => test_data[:c][5:end],
    )
    
    q = FilterQueue()
    q = add(q, test_f)
    q = add(q, 5:8)
    
    res = test_data_1[:a][apply(q, test_data_1)]
    
    append!(res, test_data_2[:a][apply(q, test_data_2)])
    
    cond = test_data[:a] .== test_data[:c]
    @test res == test_data[:a][cond][5:end]

    

end