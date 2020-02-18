function write_table_meta(io::IO, meta::DFTableMeta)
    Base.write(io, meta.protocol_version)
    Base.write(io, meta.rows)
    Base.write(io, meta.block_size)
    Base.write(io, Int32(length(meta.columns)))
    for col in meta.columns
        write_string(io, string(col[1]))
        Base.write(io, col[2])
    end
end

function read_table_meta(io::IO)
    protocol_version = Base.read(io, UInt16)
    rows = Base.read(io, Int64)
    block_size = Base.read(io, Int64)
    columns_length = Base.read(io, Int32)
    columns = Vector{Pair{Symbol, Int32}}(undef, columns_length)
    for i in 1:columns_length
        columns[i] = Symbol(read_string(io)) => Base.read(io, Int32)
    end
    return DFTableMeta(columns, block_size, rows, protocol_version)
end

