write_table_meta(tb::DFTable) = open(f->write_table_meta(f, tb.meta), metapath(tb), "w")

read_table_meta(path::String) = open(f->read_table_meta(f), metapath(path), "r")
    
make_table_dir(table::DFTable) = mkdir(table.path)


metapath(path::String) = joinpath(path, "meta.bin")
metapath(table::DFTable) = metapath(table.path)

columnpath(base_path::String, id::Number) = joinpath(base_path, string(id)) * ".bin"
columnpath(table::DFTable, id::Number) = columnpath(table.path, id)

function make_column_file(table::DFTable, meta::ColumnMeta, keep = false) where {T}
    path = columnpath(table, meta.id)
    ispath(path) && error("Column file with id $(meta.id) already exists")

    io = open(path, "w")
    Base.write(io, Int64(blocksize(table)))
    write_column_type(io, meta.type)
    !keep && close(io)
    return io
end
function remove_column_file(table::DFTable, meta::ColumnMeta) where {T}
    path = columnpath(table, meta.id)

    rm(path, force = true)
end

function make_table_files(table::DFTable)
    table_exists(table.path) && error("Table $(table.path) already exists")    
    make_table_dir(table)
    write_table_meta(table)
    make_columns_files(table)    
    return table
end

table_exists(path::AbstractString) = isdir(path)
meta_exists(path::AbstractString) = isfile(metapath(path))

function make_columns_files(table::DFTable) 
    for meta in table.meta.columns
        make_column_file(table, meta)        
    end
end

function check_column_head(io, table_meta::DFTableMeta, meta::ColumnMeta)
    block_size = Base.read(io, Int64)
    type = read_column_type(io)
    block_size != table_meta.block_size && error(
        "column $(meta.name) has blocksize $(block_size), but table has blocksize $(table_meta.block_size)"
        )
    type != meta.type && error("column $(meta.name) stored type is $(type), but $(meta.type) expected")
end

function check_column_file(path::String, table_meta::DFTableMeta, id::Number, meta::ColumnMeta)
    !isfile(columnpath(path, id)) && error("column file '$(columnpath(path, id))' for column $(meta.name) don't exists")
    open(columnpath(path, id), "r") do io
        check_column_head(io, table_meta, meta)
    end
end
function check_column_files(path::String, meta::DFTableMeta)
    for col_meta in meta.columns
        check_column_file(path, meta, col_meta.id, col_meta)
    end
end

function open_file(table::DFTable, column::Symbol;mode = :read)
    
    !isopen(table) && error("table not opened")
    meta = getmeta(table, column)
    io = open(columnpath(table,meta.id), mode == :read ? "r" : "a+")        
    if mode == :rewrite
        seekstart(io)
    end
    check_column_head(io, table.meta, meta)
    return io
    
end

function open_files(table::DFTable, columns::Tuple{Vararg{Symbol}};mode = :read)
    
    !isopen(table) && error("table not opened")
    metas = getmeta.(Ref(table), columns)
    
    return map(metas) do col_meta
        io = open(columnpath(table,col_meta.id), mode == :read ? "r" : "a+")        
        if mode == :rewrite
            seekstart(io)
        end
        check_column_head(io, table.meta, col_meta)
        return io
    end
end
open_files(table::DFTable; mode = :read) = open_files(table, (names(table)...,), mode = mode)

"""
    drop_table!(table::DFTable)
Drop table with all data and remove table dir
"""
function drop_table!(table::DFTable) 
    table_exists(table.path) && rm(table.path, force = true, recursive = true)
    table.is_opened = false
    return table
end
"""
    truncate_table!(table::DFTable)
Truncate table data
"""
function truncate_table!(table::DFTable) 
    table_exists(table.path) && rm(table.path, force = true, recursive = true)
make_table_files(table)    
    return table
end