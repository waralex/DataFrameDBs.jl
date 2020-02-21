using DataFrames
import Tables, CSV

struct YY{S} end

macro tst(args...)
    @show args
    dump(args)
    funcs = Expr[]
    for arg in args
        @show arg
        push!(funcs, quote des(::Type{YY{Symbol($arg)}}) = $arg end )
    end
    return esc(:($(funcs...),))
end

@testset "devel" begin
#@show @macroexpand @tst(Int32)
#@tst Int32
@tst begin 
    Int64
    Int8
    Int32
end
@test 1 == 1
println(des(YY{Symbol("Int32")}))
println(des(YY{Symbol("Int64")}))
println(des(YY{Symbol("Int8")}))
#csv = CSV.read("test.csv")
#@show csv
#@show Tables.columnnames(csv)
end