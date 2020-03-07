
"""
    open_table(path::String)

Open existing table
"""
function open_table(path::String)
    !table_exists(path) && error("Table $path don't exists")
    !meta_exists(path) && error("Meta file $(metapath(path)) don't exists")
    
    meta = read_table_meta(path)
    check_column_files(path, meta)
    table = DFTable(path, meta)
    table.is_opened = true
    return table
end

"""
    create_table(path::String; from, column_names ::Union{AbstractVector{Symbol}, AbstractVector{String}}, types ::AbstractVector{<:Type}; block_size = DEFAULT_BLOCK_SIZE, show_progress = false)

Create new table

# Arguments
- `path` - directory to story table. Must not exists
- `column_names` - names of columns
- `types` - types of columns
- `block_size` - count of rows in processing block default is 65536

# Examples
```julia
import CSV
create_table("new_table", [:a, :b, :c], [Int64, String, Float64])

```
"""
function create_table(path::String, 
    column_names ::Union{AbstractVector{Symbol}, AbstractVector{String}},
    types ::AbstractVector{<:Type}; block_size = DEFAULT_BLOCK_SIZE)
    
    table = DFTable(path, DFTableMeta(column_names, types, block_size))
    make_table_files(table)
    table.is_opened = true
    return table
end

"""
    create_table(path::String; from, block_size = DEFAULT_BLOCK_SIZE, show_progress = false)

Create table from existing data 

# Arguments
- `path` - directory to story table. Must not exists
- `from` - exists data to insert to table. Must support Tables.schema and Tables.rows interfaces
- `block_size` - count of rows in processing block default is 65536
- `show_progress` - show progress string while inserting data

# Examples
```julia
import CSV
create_table("table_from_csv", from = CSV.Rows("some.csv"), show_progress = true)

using DataFrames
df = DataFrame((a=collect(1:100), b=collect(1:100)))
create_table("table_from_df", from = df, show_progress = true)
```
"""
function create_table(path::String; from, block_size = DEFAULT_BLOCK_SIZE, show_progress = false)
    schema = Tables.schema(from)
    isnothing(schema) && ArgumentError("Tables.schema undefinded for $(from)")    
    names = collect(schema.names)
    types = collect(schema.types)    
    table = create_table(path, names, types; block_size = block_size) 
    insert(table, from,  show_progress = show_progress)   
    return table 
end