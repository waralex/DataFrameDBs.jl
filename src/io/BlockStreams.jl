import CodecLz4
const DEFAULT_BUFFER = 1024*1024
const COMPRESSION_LEVEL = 2

CompBuffer() =  IOBuffer(read = true,  append = true, truncate = true, sizehint = DEFAULT_BUFFER)

const _BlockHeaderSizeAddon = sizeof((Int32, Int64, Int64))

mutable struct BlockStream
    io ::IO
    uncomp_buffer ::IOBuffer
    comp_buffer ::IOBuffer
    rows ::Int32
    BlockStream(io::IO) = new(io, CompBuffer(), CompBuffer())
end

Base.eof(s::BlockStream) = Base.eof(s.io) && s.uncomp_buffer.size == 0

Base.close(s::BlockStream) = Base.close(s.io)

Base.flush(s::BlockStream) = Base.flush(s.io)

stats_from_block(;rows, in_block_compressed, uncompressed) = SizeStats(rows, in_block_compressed + _BlockHeaderSizeAddon, uncompressed)

function write(s::BlockStream, v::AbstractString) 
    write(s, Int32(length(v)))
    Base.write(s.io, v)
end
write(s::BlockStream, v::T)  where {T} = Base.write(s.io, v)

function prepare_block_write!(f::Function, s::BlockStream, rows::Number)
    s.rows = rows
    f(s.uncomp_buffer)    
end

function commit_block_write!(s::BlockStream)
    size_to_compress = s.uncomp_buffer.size
    size_to_compress == 0 && return (0,0)
    compressed_bound = Int64(CodecLz4.LZ4_compressBound(size_to_compress))    
    Base.ensureroom(s.comp_buffer, compressed_bound)
    
    compressed_size = Int64(CodecLz4.LZ4_compress_fast(
        s.uncomp_buffer.data,
        s.comp_buffer.data,
        size_to_compress,
        compressed_bound,
        COMPRESSION_LEVEL
    ))    
    s.comp_buffer.size += compressed_size
    Base.write(s.io, Int32(s.rows))
    Base.write(s.io, Int64(size_to_compress))
    Base.write(s.io, Int64(compressed_size))
    Base.write(s.io, s.comp_buffer)
    flush(s.io)
    truncate(s.comp_buffer, 0)
    truncate(s.uncomp_buffer, 0)
    rows = s.rows
    s.rows = 0    
    return stats_from_block(rows = rows, in_block_compressed = compressed_size, uncompressed = size_to_compress)
end

read(s::BlockStream, v::Type{T})  where {T} = Base.read(s.io, v)
function read(s::BlockStream, v::Type{String}) 
    length = read(s, Int32)
    return String(Base.read(s.io, length))
end

read_sizes(s::BlockStream) = (
    rows = Base.read(s.io, Int32), 
    origin = Base.read(s.io, Int64), 
    compressed = Base.read(s.io, Int64)
    )

function skip_block(s::BlockStream)    
    sizes = read_sizes(s)
    Base.skip(s.io, sizes.compressed)
    return stats_from_block(rows = sizes.rows, in_block_compressed = sizes.compressed, uncompressed = sizes.origin)
end

function seek_to_lastblock(s::BlockStream, block_size::Integer)    
    rows = block_size
    while !eof(s.io)
        mark(s.io)
        rows = skip_block(s).rows        
    end
    
    if rows < block_size 
        reset(s.io)
        return rows
    end
    return 0
end

function read_block_and_reset(f::Function, s::BlockStream)
    mark(s.io)
    res = read_block(f, s)
    reset(s.io)
    return res
end

function read_block(f::Function, s::BlockStream)
    
    sizes = read_sizes(s)
    
    Base.ensureroom(s.comp_buffer, sizes.compressed)
    Base.ensureroom(s.uncomp_buffer, sizes.origin)
    Base.unsafe_read(s.io, pointer(s.comp_buffer.data), sizes.compressed)
    size = CodecLz4.LZ4_decompress_safe(pointer(s.comp_buffer.data), pointer(s.uncomp_buffer.data),
    sizes.compressed, sizes.origin)
    @assert size == sizes.origin "decompression error"
    s.uncomp_buffer.size = size
    f(sizes.rows, s.uncomp_buffer)
    truncate(s.comp_buffer, 0)
    truncate(s.uncomp_buffer, 0)
    
    return stats_from_block(rows = sizes.rows, in_block_compressed = sizes.compressed, uncompressed = sizes.origin)
end