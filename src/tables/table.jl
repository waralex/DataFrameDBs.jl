abstract type Editable end
abstract type ReadOnly end

"""
    DFTable

Class that represent table. Do not instantate it directly, use [`create_table`](@ref) or [`open_table`](@ref)
"""
mutable struct DFTable
    path ::String
    meta ::DFTableMeta
    is_opened ::Bool
    show_read_progress ::Bool
    DFTable(path::AbstractString, meta::DFTableMeta) = new(path, meta, false, false)    
end


function Base.:(==)(a::DFTable, b::DFTable)
    a.path == b.path &&
    columns_meta(a) == columns_meta(b)
end

function Base.show(io::IO, table::DFTable) 
    !isopen(table) && return print(io, "closed table")
    println("DFTable path: ", table.path)
    println(table_stats(table))
end

"""
    turnon_progress!(tb::DFTable)

Turn on showing progress of all read operation with this table
"""
turnon_progress!(tb::DFTable) = tb.show_read_progress = true

"""
    turnoff_progress!(tb::DFTable)

Turn off showing progress of all read operation with this table
"""
turnoff_progress!(tb::DFTable) = tb.show_read_progress = false

isshow_progress(tb::DFTable) = tb.show_read_progress



blocksize(t::DFTable) = t.meta.block_size
Base.isopen(t::DFTable) = t.is_opened
columns_meta(t::DFTable) = t.meta.columns


function getmeta(table::DFTable, name::Symbol)
    res = findfirst(x->x.name == name, table.meta.columns)
    isnothing(res) && KeyError(name)
    return columns_meta(table)[res]
end

Base.names(t::DFTable) = getproperty.(columns_meta(t), :name)
row_index(t::DFTable) = t.row_index

nrows(table::DFTable) = nrows(table[:,:])

"""
    rename_column!(table::DFTable, old::Symbol, new::Symbol)

Rename column in table
"""
function rename_column!(t::DFTable, old::Symbol, new::Symbol)
    meta = getmeta(t, old)
    (new in names(t)) && throw(ArgumentError("Column :$new already exists")) 
    meta.name = new
    write_table_meta(t)
end
"""
    drop_column!(table::DFTable, col::Symbol)

Drop column of table
"""
function drop_column!(t::DFTable, col::Symbol)    
    
    res = findfirst(x->x.name == col, t.meta.columns)
    isnothing(res) && throw(KeyError(col))
    meta = t.meta.columns[res]
    deleteat!(t.meta.columns, res)
    write_table_meta(t) 
    remove_column_file(t, meta)
    return t
end


"""
    add_column!(table::DFTable, name::Symbol, data; before::Union{Symbol, Nothing} = nothing, show_progress = false)

Add column to table `data` can be AbstractVector or Iteratable or DFColumn
"""
function add_column!(t::DFTable, name::Symbol, data; before::Union{Symbol, Nothing} = nothing, show_progress = false)
    (name in names(t)) && throw(ArgumentError("Column :$name already exists"))
    nrow(t) != length(data) && throw(ArgumentError("Column and table have different sizes"))
    (!isnothing(before) && !(before in names(t))) && throw(KeyError(before))
    
    new_id = isempty(t.meta.columns) ? 1 : maximum(m->m.id, t.meta.columns) + 1
    
    !isavailabletype(eltype(data)) && throw(ArgumentError("$(eltype(data)) is not available as column type"))

    new_meta = ColumnMeta(new_id, name, eltype(data))
    
    pos = isnothing(before) ?
            length(t.meta.columns) + 1 :
            findfirst(x->x.name == before, t.meta.columns) 
    
    insert!(t.meta.columns, pos, new_meta)
    
    write_table_meta(t)
    try
        make_column_file(t, new_meta)        

        write_column(t, name, data, show_progress = show_progress)
    catch e
        drop_column!(t, name)
        throw(e)
    end
    

end