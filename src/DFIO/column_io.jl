
write_length(io, v::AbstractArray) = Base.write(io, Int32(length(v)))

read_length(io) = Base.read(io, Int32)

function write_block(io::IO, v::AbstractArray{T,1}) where {T}
    write_block(io, collect(T,v))
end

function write_block(io::IO, v::Union{Array{T,1}, SubArray{T, 1, Array{T,1}}}) where {T}
    if !isbitstype(T)
        error("`write_block` is not supported on this array")
    end
    isempty(v) && return
    write_length(io, v) #rows in block 
    Base.write(io, v)
end

function read_block!(io::IO, v::Array{T,1}) where {T}
    if !isbitstype(T)
        error("`read_block` is not supported on this array")
    end
    length = read_length(io)
    resize!(v, length)
    Base.read!(io, v)
    return length
end

function write_string_block(io::IO, v::AbstractArray{String,1})
    isempty(v) && return 
    write_length(io, v) #rows in block
    flat_block = FlatStringsVector(v)
    Base.write(io, Int32(sizeofdata(flat_block)))
    Base.write(io, flat_block.ranges)
    GC.@preserve flat_block Base.unsafe_write(io, pointer(flat_block.data), sizeofdata(flat_block))
end
write_block(io::IO, v::AbstractArray{String,1}) = write_string_block(io, v)
write_block(io::IO, v::Union{Array{String,1}, SubArray{String, 1, Array{String,1}}}) = write_string_block(io, v)

function read_block!(io::IO, v::FlatStringsVector)
    length = read_length(io)
    datasize = read_length(io)
    println("", length)    
    resize!(v.ranges, length)
    Base.read!(io, v.ranges)    
    FlatStringsVectors.resize_data!(v, datasize)
    println(datasize)
    Base.unsafe_read(io, pointer(v.data), datasize)    
    return length
end

write_column_type(io::IO, ::Type{T}) where {T} = write_string(io, "$T")
write_column_type(io::IO, ::T) where {T} = write_string(io, "$T")
read_column_type(io::IO) = eval(Meta.parse(read_string(io)))

struct BlockIterator{T}
    io::IO
    buffer::T
    BlockIterator{T}(io, buffer::T) where {T} = new(io, buffer)
    BlockIterator{T}(io, block_size::Int) where {T} = new(io, T(undef, block_size))
end
BlockIterator(io, ::Type{T}, block_size) where {T} = BlockIterator{T}(io, block_size)
BlockIterator(io, ::Type{FlatStringsVector}, block_size) where {T} = BlockIterator{FlatStringsVector}(io, FlatStringsVector())

function Base.iterate(iter::BlockIterator, state = nothing)
    eof(iter.io) && return nothing
    read_block!(iter.io, iter.buffer)
    return (iter.buffer, nothing)
end

function write_column_data(io::IO, v::AbstractArray{T,1}, block_size::Int64) where {T}
    write_column_type(io, T)
    len = Base.length(v)
    offset = 1
    while offset + block_size  <= len
        r = offset:(offset + block_size - 1)
        
        write_block(io, view(v, r))
        offset += block_size
    end
    if offset <= len
        r = offset:len
        
        write_block(io, view(v, r))
    end    
end

function read_column_data!(io::IO, v::T, block_size::Int64 = 0;sizehint::Int64 = 100000) where {T <: AbstractVector}
    read_string(io)
    empty!(v)
        
    i = 0
    for block in BlockIterator(io, T, DEFAULT_BLOCK_SIZE)                                
        i+=1
        #append!(v, block)
    end
    
end

function read_column_data(io::IO, ::Type{T}, block_size::Int64 = 0) where {T}
    result = T[]
    read_column_data!(io, result, block_size)
    return result
end

function read_column_data(io::IO, ::Type{String}, block_size::Int64 = 0) where {T}
    result = FlatStringsVector()
    read_column_data!(io, result, block_size)
    return result
end