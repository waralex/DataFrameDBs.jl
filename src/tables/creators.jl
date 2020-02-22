function open_table(path::String)
    !table_exists(path) && error("Table $path don't exists")
    !meta_exists(path) && error("Meta file $(metapath(path)) don't exists")
    
    meta = read_table_meta(path)
    check_column_files(path, meta)
    table = DFTable{Editable}(path, meta)
    table.is_opened = true
    return table
end

function create_table(path::String, 
    column_names ::Union{AbstractVector{Symbol}, AbstractVector{String}},
    types ::AbstractVector{<:Type}; block_size = DEFAULT_BLOCK_SIZE)
    
    table = DFTable{Editable}(path, DFTableMeta(column_names, types, block_size))
    make_table_files(table)
    table.is_opened = true
    return table
end

type_from_source(::AbstractVector{T}) where {T} = T

function create_table(path::String; from, block_size = DEFAULT_BLOCK_SIZE)
    cols = Tables.columns(from)
    names = collect(Symbol, Tables.columnnames(cols))
    types = [type_from_source(Tables.getcolumn(from, nm)) for  nm in names]
    columns = AbstractVector[(Tables.getcolumn(from, nm)) for nm in names]
    table = create_table(path, names, types; block_size = block_size)
    ios = open_files(table, mode = :rewrite)
    write_columns(ios, columns, blocksize(table))
    return table 
end