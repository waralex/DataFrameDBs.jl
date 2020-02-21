using DataFrameDBs.ColumnTypes: Ast, human_typestring, parse_typestring, deserialize, typestring
@testset "columntypes" begin
    test_ast = Ast(Symbol("Missing"))
    @test human_typestring(test_ast) == "Missing"
    push!(test_ast, Ast(Symbol("Int32")))
    @test human_typestring(test_ast) == "Missing(Int32)"
    push!(test_ast, Ast(Symbol("Int16")))
    @test human_typestring(test_ast) == "Missing(Int32, Int16)"
    push!(test_ast, test_ast)
    @test human_typestring(test_ast) == "Missing(Int32, Int16, Missing(Int32, Int16))"    
end

@testset "parse" begin
    test = "Missing(Int32, Int16, Tst(Int32, Int16), String)"
    ast = parse_typestring(test)
    
    @test human_typestring(ast) == test
    test2 = "Int32"
    ast2 = parse_typestring(test2)
    @test human_typestring(ast2) == test2

    test3 = "Missing(Int32, Int16, Tst(Int32, Int16, String)"
    @test_throws ErrorException ast3 = parse_typestring(test3)
   
end

@testset "deserialize" begin
    test = "Int32"
    type = deserialize(test)
    @test type == Int32

    test2 = "Missing(Int32)"
    type2 = deserialize(test2)
    @test type2 == Union{Missing, Int32}
   
end


@testset "tuples" begin
    @test typestring(Tuple{Int32, UInt64}) == "Tuple(Int32, UInt64)"
    @test typestring(NTuple{3,Int32}) == "Tuple(Int32, Int32, Int32)"
    @test deserialize("Tuple(Int32, Int32, Int32)") == NTuple{3, Int32}
   
end