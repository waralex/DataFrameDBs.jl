module FlatStringsVectors
export FlatStringsVector, sizeofdata, RangeVector
const RangeVector = Vector{Pair{UInt32, UInt32}} 
mutable struct FlatStringsVector <: AbstractVector{String}
    ranges ::RangeVector
    data ::Vector{UInt8}
    string_data ::String
    data_size ::Int
    function FlatStringsVector(;sizehint = 1024)
        string_data = Base._string_n(sizehint)
        data = unsafe_wrap(Vector{UInt8}, string_data)
        new(
            RangeVector(undef, 0),
            data,
            string_data,
            0        
        )
    end
    function FlatStringsVector(ranges::RangeVector, d::Vector{UInt8})
        total_sizeof = sizeof(d)        
        string_data = String(d)
        data = unsafe_wrap(Vector{UInt8}, string_data)
        res = new(
            ranges,
            data,
            string_data,
            total_sizeof
        )
    end
    function FlatStringsVector(source::AbstractVector{String})
        total_sizeof = sum(sizeof.(source))
        string_data = Base._string_n(total_sizeof)
        data = unsafe_wrap(Vector{UInt8}, string_data)

        res = new(
            RangeVector(undef, length(source)),
            data,
            string_data,
            total_sizeof
        )
        
        offset::UInt32 = 0
        for (i,s) in enumerate(source)            
            res.ranges[i] = offset=>UInt32(sizeof(s))            
            GC.@preserve s unsafe_copyto!(pointer(res.data) + offset, pointer(s), sizeof(s))
            offset += sizeof(s)
        end 
        return res
    end
end

Base.:(==)(a::FlatStringsVector, b::FlatStringsVector) = length(a) == length(b) &&
                                                        sizeofdata(a) == sizeofdata(b) &&
                                                        a.ranges == b.ranges &&
                                                        view(a.data, 1:sizeofdata(a)) == view(b.data, 1:sizeofdata(b))
                                                         


@inline getstring(a::FlatStringsVector, r::Pair{UInt32, UInt32}) = SubString(a.string_data, r[1] + 1, r[1] + r[2])#unsafe_string(pointer(a.data) + r[1], r[2])

@inline sizeofdata(a::FlatStringsVector) = a.data_size

function resize_data!(a::FlatStringsVector, new_size::Number)
    if new_size > sizeof(a.string_data)
        new_string = Base._string_n(sizeof(a.string_data) * 2)
        new_data = unsafe_wrap(Vector{UInt8}, new_string)
        Base.copyto!(new_data, a.data)
        a.data = new_data
        a.string_data = new_string
    end    
    a.data_size = new_size
end

@inline add_datasize!(a::FlatStringsVector, added_size::Number) = resize_data!(a, sizeofdata(a) + added_size)

function Base.empty!(a::FlatStringsVector)
    empty!(a.ranges)
    a.data_size = 0
end

Base.size(a::FlatStringsVector) = Base.size(a.ranges)

@inline datasizeof(a) = a.data_size

Base.@propagate_inbounds Base.getindex(a::FlatStringsVector, i::Int)::SubString = getstring(a, a.ranges[i])

Base.sizehint!(a::FlatStringsVector, s::Int) = sizehint!(a.ranges, s)

Base.firstindex(a::FlatStringsVector) = 1
Base.lastindex(a::FlatStringsVector) = Base.lastindex(a.ranges)
Base.IndexStyle(::Type{<:FlatStringsVector}) = IndexLinear()


function Base.push!(a::FlatStringsVector, s::AbstractString)        
    Base.push!(a, String(s))    
end

function Base.push!(a::FlatStringsVector, s::String)
    offset = a.data_size
    push!(a.ranges, UInt32(offset)=>UInt32(sizeof(s)))    
    add_datasize!(a, sizeof(s))
    GC.@preserve s unsafe_copyto!(pointer(a.data) + offset, pointer(s), sizeof(s))
end

function Base.append!(a::FlatStringsVector, appended::FlatStringsVector)
    offset = datasizeof(a)
    add_datasize!(a, sizeofdata(appended))
    copyto!(a.data, offset + 1, appended.data, 1, datasizeof(appended))
    append!(
        a.ranges,
        map(
            p->(p[1] + offset)=>(p[2]),
            appended.ranges
        )
    )    
end

function Base.iterate(a::FlatStringsVector)
    r = Base.iterate(a.ranges)
    isnothing(r) && return r
    return (getstring(a, r[1]), r[2])
end

function Base.iterate(a::FlatStringsVector, state)
    r = Base.iterate(a.ranges, state)
    isnothing(r) && return r
    return  (getstring(a, r[1]), r[2])
end

end