using DataFrameDBs.FlatStringsVectors
Base.show(io, a::T) where {T<:Unsigned} = print(io, a) 
@testset "base" begin
    
    test = ["sfsdf", "ggggg", "ll", "", "kk"]
    a = FlatStringsVector(test)
    b = FlatStringsVector(test)
    @test a == b
    
    
    @test typeof(a[1]) <: SubString
    @test length(a) == length(test)
    @test a[1] == test[1]
    @test a[2] == test[2]
    @test a[end - 1] == test[end - 1]
    @test a[end] == test[end]
    
    for (i, s) in enumerate(a)
        @test s == test[i]
    end
    
    @test pointer(a.data) == pointer(a.string_data)
    push!(a, "tyuu")
    @test a != b
    @test a[end] == "tyuu"
    @test pointer(a.data) == pointer(a.string_data)
    push!(a, "iooo")
    @test a[end] == "iooo"
    @test length(a) == length(test) + 2

    s = "123456789"
    push!(a, SubString(s, 3:5))
    @test a[end] == "345"
    @test pointer(a.data) == pointer(a.string_data)
    
    

    test_2 = ["1", "2", "", "fff"]
    a = FlatStringsVector(test)
    b = FlatStringsVector(test_2)
    append!(a, b)
    @test length(a) == length(test) + length(test_2)

    for (i, s) in enumerate(a)
        (i <= length(test)) && @test s == test[i]
        (i > length(test)) && @test s == test_2[i - length(test)]
    end
    
end