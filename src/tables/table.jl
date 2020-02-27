abstract type Editable end
abstract type ReadOnly end


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

turnon_progress!(tb::DFTable) = tb.show_read_progress = true
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
