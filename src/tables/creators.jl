function open_table(path::String)
    !table_exists(path) && error("Table $path don't exists")
    !meta_exists(path) && error("Meta file $(metapath(path)) don't exists")
    
    meta = read_table_meta(path)
    check_column_files(path, meta)
    table = DFTable(path, meta)
    table.is_opened = true
    return table
end

function create_table(path::String, 
    column_names ::Union{AbstractVector{Symbol}, AbstractVector{String}},
    types ::AbstractVector{<:Type}; block_size = DEFAULT_BLOCK_SIZE)
    
    table = DFTable(path, DFTableMeta(column_names, types, block_size))
    make_table_files(table)
    table.is_opened = true
    return table
end

type_from_source(::AbstractVector{T}) where {T} = T

function create_table(path::String; from, block_size = DEFAULT_BLOCK_SIZE)
    schema = Tables.schema(from)
    isnothing(schema) && ArgumentError("Tables.schema undefinded for $(from)")    
    names = collect(schema.names)
    types = collect(schema.types)    
    table = create_table(path, names, types; block_size = block_size) 
    insert(table, from)   
    return table 
end