function check_schema(table, data)
    schema = Tables.schema(data)
    isnothing(schema) && ArgumentError("Tables.schema undefinded for $(data)")
    (length(columns_meta(table)) != length(schema.names)) && throw(ArgumentError("column count in $data don't match table columns count"))
    (collect(schema.types) != getproperty.(columns_meta(table), :type)) && throw(ArgumentError("column type in $data don't match table columns types"))
end

function write_columns(ios::Vector{<:IO}, data::Vector{<:AbstractVector}, block_size; close_on_done = true)
    @assert length(ios) == length(data) "ios and data have must equal sizes"
    total_rows = minimum(length.(data))
    @assert total_rows == maximum(length.(data)) "data columns must has equal lengths"

    streams = BlockStream.(ios)
    
    offset = 1
    while offset + block_size  <= total_rows
        r = offset:(offset + block_size - 1)
        prepare_block_write!.(streams, getindex.(data, Ref(r)))  
        commit_block_write!.(streams)        
        offset += block_size
    end
    if offset <= total_rows
        r = offset:total_rows
        prepare_block_write!.(streams, getindex.(data, Ref(r)))
        commit_block_write!.(streams)                
    end    
    close_on_done && close.(streams)
end

function write_column(table::DFTable, name::Symbol, data::AbstractVector; close_on_done = true, show_progress = false)
    
    total_rows = length(data)
    io = open_file(table, name, mode = :rewrite)
    
    stream = BlockStream(io)    
    offset = 1
    progress= show_progress ? write_progress_channel(1) : nothing
    block_size = blocksize(table)
    while offset + block_size  <= total_rows
        r = offset:(offset + block_size - 1)
        prepare_block_write!(stream, data[r])  
        sz = commit_block_write!(stream)
        !isnothing(progress) && put!(progress, [sz])
        
        offset += block_size
    end
    if offset <= total_rows
        r = offset:total_rows
        prepare_block_write!(stream, data[r])
        sz = commit_block_write!(stream)               
        !isnothing(progress) && put!(progress, [sz]) 
    end    
    if !isnothing(progress)
        put!(progress, nothing)
        take!(progress)        
        
    end
    close_on_done && close(stream)
end

write_column(table::DFTable, name::Symbol, data; close_on_done = true, show_progress = false) =
                     write_column_from_iterator(table, name, data; close_on_done = close_on_done, show_progress = show_progress)

function write_column(table::DFTable, name::Symbol, data::DFColumn; close_on_done = true, show_progress = false)
    if data.view.table != table || !isempty(data.view.selection)
        return write_column_from_iterator(table::DFTable, name::Symbol, data; close_on_done = close_on_done, show_progress = show_progress)
    end
    
    progress= show_progress ? write_progress_channel(1) : nothing
    io = open_file(table, name, mode = :rewrite)    
    stream = BlockStream(io)    

    for block in BlocksIterator(data.view)
        prepare_block_write!(stream, block[1])
        sz = commit_block_write!(stream)
        !isnothing(progress) && put!(progress, [sz])
    end
    if !isnothing(progress)
        put!(progress, nothing)
        take!(progress)                
    end
    close_on_done && close(stream)
end

function write_column_from_iterator(table::DFTable, name::Symbol, data; close_on_done = true, show_progress = false)
    
    total_rows = length(data)
    (total_rows == 0) && return
    io = open_file(table, name, mode = :rewrite)
    buffer = make_write_buffer(getmeta(table, name))
    stream = BlockStream(io)    
    offset = 1
    progress= show_progress ? write_progress_channel(1) : nothing
    resize!(buffer, blocksize(table))
    offset = 1
    size = 0
    for value in data
        buffer[offset] = value
        size += 1
        offset += 1
        if offset == blocksize(table) + 1
            prepare_block_write!(stream, buffer)
            sz = commit_block_write!(stream)
            !isnothing(progress) && put!(progress, [sz])            
            offset = 1
            size = 0
        end        
    end
    if size > 0
        resize!(buffer, size)        
        prepare_block_write!(stream, buffer)
        sz = commit_block_write!(stream)
        !isnothing(progress) && put!(progress, [sz])
    end
    if !isnothing(progress)
        put!(progress, nothing)
        take!(progress)        
        
    end
    close_on_done && close(stream)
end


"""
    insert(table::DFTable, rows; show_progress = false)

Insert rows to table. `rows` must support Tables.schema and Tables.rows interfaces
"""
function insert(table::DFTable, rows; show_progress = false)
    check_schema(table, rows)
    ios = open_files(table, mode = :rewrite)
    streams = BlockStream.(ios)
    
    progress= show_progress ? write_progress_channel(length(streams)) : nothing

    buffers = make_write_buffer.(columns_meta(table))
    lb_rows = first(seek_to_lastblock.(streams, blocksize(table)))
    if lb_rows > 0
        last_block_cols = make_buffer.(columns_meta(table))
        read_block_and_reset!.(streams, last_block_cols)
        buffers .= collect.(last_block_cols)
    end
    
    resize!.(buffers, blocksize(table))
    offset = lb_rows + 1
    size = lb_rows
    for row in Tables.rows(rows)
        
        for i in 1:length(buffers)
            buffers[i][offset] = Tables.getcolumn(row, i)
        end
        
        size += 1
        offset += 1
        if offset == blocksize(table) + 1
            prepare_block_write!.(streams, buffers)
            sz = commit_block_write!.(streams)
            !isnothing(progress) && put!(progress, sz)
            
            offset = 1
            size = 0
        end
    end
    
    if size > 0
        resize!.(buffers, size)        
        prepare_block_write!.(streams, buffers)
        sz = commit_block_write!.(streams)
        !isnothing(progress) && put!(progress, sz)
    end

    if !isnothing(progress)
        put!(progress, nothing)
        take!(progress)        
        
    end
    
    close.(streams)
    return table
end

make_column_storage(::Type{T}) where {T} = Vector{T}(undef, 0)
make_column_storage(::Type{String}) = FlatStringsVector()


make_write_buffer(meta ::ColumnMeta) where {T} = Vector{meta.type}(undef, 0)