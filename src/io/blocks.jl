
function write_block_body(io::IO, v::AbstractVector{T}) where {T}
    if !isbitstype(T)
        error("write_block is not supported for array of type $(T)")
    end    
    Base.write(io, v)
end

function write_block_body(io::IO, v::AbstractVector{Union{T, Missing}}) where {T}
    if !isbitstype(T)
        error("write_block is not supported for array of type $(T)")
    end
    fill_value = missing_fill_value(T)
    missing_bits = ismissing.(v)
    Base.write(io, missing_bits)
    unmissing_data = [ismissing(a) ? fill_value : a for a in v]
    Base.write(io, unmissing_data)
end


function write_block_body(io::IO, v::AbstractVector{String})
    flat_block = FlatStringsVector{String}(v)
    Base.write(io, Int32(sizeofdata(flat_block)))    
    Base.write(io, flat_block.sizes)
    GC.@preserve flat_block Base.unsafe_write(io, pointer(flat_block.data), sizeofdata(flat_block))
end

function write_block_body(io::IO, v::AbstractVector{Union{String, Missing}})
    flat_block = FlatStringsVector{Union{String, Missing}}(v)
    Base.write(io, Int32(sizeofdata(flat_block)))    
    Base.write(io, flat_block.sizes)
    GC.@preserve flat_block Base.unsafe_write(io, pointer(flat_block.data), sizeofdata(flat_block))
end

prepare_block_write!(s::BlockStream, v::AbstractVector) = prepare_block_write!(io->write_block_body(io, v), s, length(v))

function read_block_body!(io::IO, v::Vector{T}, rows::Number) where {T}
    if !isbitstype(T)
        error("read_block is not supported for array of type $(T)")
    end    
    resize!(v, rows)

    Base.read!(io, v)
end

function read_block_body!(io::IO, v::Vector{Union{T, Missing}}, rows::Number) where {T}    
    if !isbitstype(T)
        error("read_block is not supported for array of type $(T)")
    end    
    
    resize!(v, rows)
    bits = BitArray(undef, rows)
    unmissing = Vector{T}(undef, rows)
        
    Base.read!(io, bits)
    Base.read!(io, unmissing)
    v[bits] .= missing
    v[@. !bits] .= unmissing[@. !bits]
    
end

function read_block_body!(io::IO, v::FlatStringsVector, rows::Number)
    datasize = Base.read(io, Int32)
        
    resize!(v.sizes, rows)

    Base.read!(io, v.sizes)    
    FlatStringsVectors.resize_data!(v, datasize)        
    GC.@preserve io v Base.unsafe_read(io, pointer(v.data), datasize)
    FlatStringsVectors.unsafe_remake_offsets!(v)
end

function read_block!(s::BlockStream, v::AbstractArray)
    read_block(s) do rows, io
        read_block_body!(io, v, rows)
    end
end
