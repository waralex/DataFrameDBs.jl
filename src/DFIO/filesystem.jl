write_table_meta(tb::DFTable) = open(f->write_table_meta(f, tb.meta), metapath(tb), "w")

read_table_meta(tb::DFTable) = open(f->read_table_meta(f), metapath(tb), "r")
    
make_table_dir(table::DFTable) = mkdir(table.path)

metapath(table::DFTable) = joinpath(table.path, "meta.meta")

columnpath(table::DFTable, id::Int32) = joinpath(table.path, string(id)) * ".bin"

function create_column(table::DFTable, id::Int32, ::Type{T}) where {T}
    path = columnpath(table, id)
    ispath(path) && error("Column file with $id already exists")
    open(f->write_column_type(f, T), path, "w")
    return DFColumn(path, T)
end

function create_column(table::DFTable, id::Int32, v::AbstractArray{T,1}) where {T}
    path = columnpath(table, id)
    ispath(path) && error("Column file with $id already exists")
    open(f->write_column_data(f, v, table.meta.block_size), path, "w")
    return DFColumn(path, T)
end

function create_table(path::String, names::Vector{Symbol}, types::Vector{DataType})
    (length(names) != length(types)) && error("length of types mistmach to length of types")
    
    isdir(path) && error("Table $(path) already exists")
        
    table = DFTable(path, DFTableMeta(names))
    make_table_dir(table)
    write_table_meta(table)
    create_columns!(table, types)    
    return table
end

function create_table(path::String, names::Vector{Symbol}, columns::Vector{<:AbstractArray})
    (length(names) != length(columns)) && error("length of types mistmach to length of columns")
    
    isdir(path) && error("Table $(path) already exists")
        
    table = DFTable(path, DFTableMeta(names))
    make_table_dir(table)
    write_table_meta(table)
    create_columns!(table, columns)    
    return table
end

function open_table(path::String)
    table = DFTable(path)
    open_table!(table)
    return table
end

function open_table!(table::DFTable)
    !isdir(table.path) && error("Table $(table.path) don't exists")
    
    table.meta = read_table_meta(table)
    load_columns!(table)   
end

function create_columns!(table::DFTable, types::Vector{DataType})
    length(table.meta.columns) != length(types) && error("length of types mistmach to length of columns")
    for i in 1:length(table.meta.columns)
        push!(table.columns, create_column(table, table.meta.columns[i][2], types[i]))        
    end
end

function create_columns!(table::DFTable, columns::Vector{<:AbstractArray})
    length(table.meta.columns) != length(columns) && error("length of meta columns mistmach to length of columns")
    for i in 1:length(table.meta.columns)
        push!(table.columns, create_column(table, table.meta.columns[i][2], columns[i]))        
    end
end

function load_columns!(table::DFTable)    
    for (name, id) in table.meta.columns
        column_file = joinpath(table.path, string(id)) * ".bin"
        type = open(f->read_column_type(f), column_file, "r")
        
        push!(table.columns, DFColumn(column_file, type))
    end
end