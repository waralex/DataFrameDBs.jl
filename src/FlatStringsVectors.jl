module FlatStringsVectors
export FlatStringsVector, sizeofdata, OffSzVector
const OffVector = Vector{Int64}
const SzVector = Vector{Int32}
mutable struct FlatStringsVector{T} <: AbstractVector{T}    
    offsets ::OffVector
    sizes ::SzVector
    data ::T
    datasize ::Int   
    function FlatStringsVector{T}(;sizehint = 1024) where {T <: Union{String, Missing}}
        new{T}(
            OffVector[],
            SzVector[],
            Base._string_n(sizehint),
            0
        )
    end
    function FlatStringsVector{T}(offsets::OffVector, sizes::SzVector, data::String) where {T <: Union{String, Missing}}
        total_sizeof = sizeof(data)        
        @assert(length(offsets) == length(sizes))
        @assert total_sizeof == offsets[end] + sizes[end]        
        res = new{T}(
            offsets,
            sizes,
            data,
            total_sizeof       
        )
    end
    function FlatStringsVector{T}(source::AbstractVector{T}) where {T <: Union{String, Missing}}
        total_sizeof = sum(sizeof.(source))
        data = Base._string_n(total_sizeof)    
        res = new{T}(
            OffVector(undef, length(source)),
            SzVector(undef, length(source)),
            data,
            total_sizeof        
        )        
        offset::Int64 = 0
        for (i,s) in enumerate(source)            
            res.offsets[i] = offset
            if ismissing(s)
                res.sizes[i] = Int32(-1)
            else
                res.sizes[i] = Int32(sizeof(s))
                GC.@preserve s unsafe_copyto!(pointer(res.data) + offset, pointer(s), sizeof(s))
                offset += sizeof(s)
            end
            
        end 
        return res
    end
end
    
function FlatStringsVector{T}(data::String; sizes::Vector{Int32})  where {T <: Union{String, Missing}}
    offsets = OffVector(undef, length(sizes))
    res = FlatStringsVector(offsets, sizes, data)
    unsafe_remake_offsets!(res)
    return res
end

function unsafe_remake_offsets!(a::FlatStringsVector{T})  where {T <: Union{String, Missing}}
    resize!(a.offsets, length(a.sizes))    
    if !isempty(a.sizes)
        a.offsets[1] = 0
        for i in 2:length(a.sizes)
            a.offsets[i] = a.offsets[i - 1] + (a.sizes[i - 1] >=0 ? a.sizes[i - 1] : 0)
        end        
    end
    a.datasize = _elsizes(a, a.sizes)
end

Base.:(==)(a::FlatStringsVector{T}, b::FlatStringsVector{T}) where {T} =
                                            length(a) == length(b) &&
                                            sizeofdata(a) == sizeofdata(b) &&
                                            a.offsets == b.offsets &&
                                            a.sizes == b.sizes &&
                                            SubString(a.data, 1, sizeofdata(a)) == SubString(b.data, 1, sizeofdata(b))
                                                         

Base.@propagate_inbounds eloffset(a::FlatStringsVector, i::Number) = a.offsets[i]                                                 
Base.@propagate_inbounds elsize(a::FlatStringsVector, i::Number) =  a.sizes[i]                                                      

@inline getstring(a::FlatStringsVector{String}, offset, size) = unsafe_string(pointer(a.data) + offset, size)

@inline getstring(a::FlatStringsVector{Union{String, Missing}}, offset, size) = size < 0 ? missing : unsafe_string(pointer(a.data) + offset, size)


sizes(a::FlatStringsVector) = a.sizes
offsets(a::FlatStringsVector) = a.offsets

@inline sizeofdata(a::FlatStringsVector) = a.datasize

function resize_data!(a::FlatStringsVector, new_size::Number)
    if new_size > sizeof(a.data)
        new_data_size = sizeof(a.data) * 2
        while new_size > new_data_size
            new_data_size*=2
        end        
        new_data = Base._string_n(new_data_size)
        GC.@preserve new_data a unsafe_copyto!(pointer(new_data), pointer(a.data), sizeofdata(a))
        a.data = new_data
    end
    a.datasize = new_size
end

@inline add_datasize!(a::FlatStringsVector, added_size::Number) = resize_data!(a, sizeofdata(a) + (added_size >= 0 ? added_size : 0))

function Base.empty!(a::FlatStringsVector)
    empty!(a.offsets)
    a.datasize = 0
end

Base.size(a::FlatStringsVector) = Base.size(a.offsets)


Base.@propagate_inbounds function Base.getindex(a::FlatStringsVector, i::Integer)     
    offset = eloffset(a, i)
    size = elsize(a, i)
    getstring(a, offset, size)
end

_elsizes(::FlatStringsVector{String}, sizes::SzVector) = sum(sizes)
function _elsizes(::FlatStringsVector{Union{String, Missing}}, sizes::SzVector)
    result = 0
    for i in sizes
        if i > 0
            result += i
        end
    end
    return result
end


const PossibleRanges = Union{AbstractRange{<:Integer}, BitArray, AbstractVector{Bool}}
Base.@propagate_inbounds function Base.getindex(a::FlatStringsVector{T}, r::Any) where {T}
    #TODO May be separate method for Continuous range ([a:b]) of data for copy data as single chunk 
    new_sizes = sizes(a)[r]
    data_size = _elsizes(a, new_sizes)
    new_offsets = offsets(a)[r]    
    new_data = Base._string_n(data_size)
    position = 0
    for (size, offset) in zip(new_sizes, new_offsets)
        if size > 0
            GC.@preserve new_data a begin
                unsafe_copyto!(pointer(new_data) + position, pointer(a.data) + offset, size)
            end
            position += size
        end
        
    end
    result = FlatStringsVector{T}()
    result.data = new_data 
    result.sizes = new_sizes
    unsafe_remake_offsets!(result)
    return result        
end

Base.sizehint!(a::FlatStringsVector, s::Int) = sizehint!(a.offsets, s)

Base.firstindex(a::FlatStringsVector) = 1
Base.lastindex(a::FlatStringsVector) = Base.lastindex(a.offsets)
Base.IndexStyle(::Type{<:FlatStringsVector}) = IndexLinear()


function Base.push!(a::FlatStringsVector, s::AbstractString)        
    Base.push!(a, String(s))    
end

function Base.push!(a::FlatStringsVector, s::String)
    offset = sizeofdata(a)
    push!(a.offsets, Int32(offset))
    push!(a.sizes, Int32(sizeof(s)))    
    add_datasize!(a, sizeof(s))
    GC.@preserve s unsafe_copyto!(pointer(a.data) + offset, pointer(s), sizeof(s))
end



function Base.append!(a::FlatStringsVector, appended::FlatStringsVector)
    offset = sizeofdata(a)    
    add_datasize!(a, sizeofdata(appended))    
    GC.@preserve a appended unsafe_copyto!(pointer(a.data) + offset, pointer(appended.data), sizeofdata(appended))     
    append!(
        a.offsets,
        map(
            p->(p + offset),
            appended.offsets
        )
    )
    append!(a.sizes, appended.sizes)
end

function Base.iterate(a::FlatStringsVector, state = 1)
    (state > length(a.offsets)) && return nothing
    offset = eloffset(a, state)
    size = elsize(a, state)
    state+=1
    return (getstring(a, offset, size), state)
end

end