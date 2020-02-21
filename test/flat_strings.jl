using DataFrameDBs.FlatStringsVectors
Base.show(io, a::T) where {T<:Unsigned} = print(io, a) 
@testset "base" begin
    
    test = ["sfsdf", "ggggg", "ll", "", "kk"]
    a = FlatStringsVector{String}(test)
    b = FlatStringsVector{String}(test)
    @test a == b
    FlatStringsVectors.unsafe_remake_offsets!(b)
    @test reinterpret(Int32,a.offsets) == reinterpret(Int32, b.offsets)
    @test a == b
    
    @test typeof(a[1]) <: AbstractString
    @test length(a) == length(test)
    @test a[1] == test[1]
    @test a[2] == test[2]
    @test a[end - 1] == test[end - 1]
    @test a[end] == test[end]
    
    for (i, s) in enumerate(a)
        @test s == test[i]
    end
    
    
    push!(a, "tyuu")
    @test a != b
    @test a[end] == "tyuu"
    
    push!(a, "iooo")
    @test a[end] == "iooo"
    @test length(a) == length(test) + 2

    s = "123456789"
    push!(a, SubString(s, 3:5))
    @test a[end] == "345"
    
    
    

    test_2 = ["1", "2", "", "fff"]
    a = FlatStringsVector{String}(test)
    b = FlatStringsVector{String}(test_2)
    append!(a, b)
    @test length(a) == length(test) + length(test_2)

    for (i, s) in enumerate(a)
        (i <= length(test)) && @test s == test[i]
        (i > length(test)) && @test s == test_2[i - length(test)]
    end
    
end

@testset "missing" begin
test = ["sfsdf", "ggggg", missing, "", "kk", missing]
a = FlatStringsVector{Union{String, Missing}}(test)
b = FlatStringsVector{Union{String, Missing}}(test)
@test a == b
@test ismissing.(a) == ismissing.(test)
end