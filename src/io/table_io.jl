function write_column_type(io::IO, t::Type)
    data = ColumnTypes.typestring(t)
    write_string(io, data)    
end
function read_column_type(io::IO)
    data = read_string(io)
    return ColumnTypes.deserialize(data)    
end
function write_table_meta(io::IO, meta::DFTableMeta)
    Base.write(io, Int64(meta.format_version))
    Base.write(io, Int64(meta.block_size))
    Base.write(io, Int64(length(meta.columns)))
    for col_meta in meta.columns
        Base.write(io, Int64(col_meta.id))
        write_symbol(io, col_meta.name)
        write_column_type(io, col_meta.type)        
    end
    
end

function read_table_meta(io::IO)
    format_version = Base.read(io, Int64)
    block_size = Base.read(io, Int64)
    col_length = Base.read(io, Int64)
    columns = Vector{ColumnMeta}(undef, 0)
    for i in 1:col_length
        id = Base.read(io, Int64)
        name = read_symbol(io)
        type= read_column_type(io)
        push!(columns, ColumnMeta(id, name, type))
    end
    return DFTableMeta(columns, block_size, format_version)
end