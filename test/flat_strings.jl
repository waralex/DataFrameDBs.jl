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

cmp_missing(a, b) = (ismissing(a) && ismissing(b)) || (!ismissing(a==b) && (a == b)) 

@testset "range index" begin
    test = ["1", "222", "32", "44", "335", "11116", "312313127", "444", "assadf", "bvxvbx"]
    a = FlatStringsVector{String}(test)
    @test length(a[3:5]) == length(3:5)
    @test all(cmp_missing.(a[3:5],test[3:5]))
    @test typeof(a[3:6]) <: FlatStringsVector
    
    @test length(a[1:2:10]) == length(1:2:10)
    @test all(a[1:2:10].==test[1:2:10])
    @test typeof(a[1:2:10]) <: FlatStringsVector
    @test typeof(a[:]) <: FlatStringsVector
    @test a[:] == a

    @test all( a[startswith.(a, "3")] == test[startswith.(test, "3")])
    @test typeof(a[startswith.(a, "3")]) <: FlatStringsVector

    test = ["1", "222", missing, "44", "335", "11116", missing, "444", "assadf", "bvxvbx"]
    a = FlatStringsVector{Union{String, Missing}}(test)
    
    @test length(a[3:5]) == length(3:5)
    @test all(cmp_missing.(a[3:5],test[3:5]))
    @test typeof(a[3:6]) <: FlatStringsVector
    
    @test length(a[1:2:10]) == length(1:2:10)
    @test length(a[[]]) == 0
    @test typeof(a[[]]) <: FlatStringsVector
    
    @test all(cmp_missing.(a[1:2:10],test[1:2:10]))
    @test typeof(a[1:2:10]) <: FlatStringsVector
    @test typeof(a[ismissing.(a)]) <: FlatStringsVector
    @test length(ismissing.(a)) == length(ismissing.(test))
    @test all(a[@. !ismissing(a)] .== test[@. !ismissing(test)])
    
    #println(a[3:5].data)
end