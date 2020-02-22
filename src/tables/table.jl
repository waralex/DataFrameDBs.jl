abstract type Editable end
abstract type ReadOnly end

mutable struct DFTable{T}
    path ::String
    meta ::DFTableMeta
    is_opened ::Bool    
    DFTable{T}(path::AbstractString, meta::DFTableMeta) where {T} = new{T}(path, meta, false)
end
DFTable(path::AbstractString, meta::DFTableMeta) = DFTable{Editable}(path, meta)

blocksize(t::DFTable) = t.meta.block_size
Base.isopen(t::DFTable) = t.is_opened
columns_meta(t::DFTable) = t.meta.columns

const ColumnIndexType = Union{Vector{Symbol}, Vector{<:Integer}, Colon, <:Integer, Symbol}

meta_by_condition(table::DFTable, ::Colon) = table.meta.columns

meta_by_condition(table::DFTable, i::Integer) = [table.meta.columns[i]]

function meta_by_name(table::DFTable, name::Symbol)
    res = findfirst(x->x.name == name, table.meta.columns)
    isnothing(res) && KeyError(name)
    return table.meta.columns[res]
end

meta_by_condition(table::DFTable, name::Symbol) = [meta_by_name(table, name)]


@inline function meta_by_condition(table::DFTable, names::Vector{Symbol}) 
    @boundscheck !allunique(names) && throw(ArgumentError("Elements of $(names) must be unique"))
    
    return meta_by_name.(Ref(table), names)
end
@inline function meta_by_condition(table::DFTable, positions::Vector{<:Integer}) 
    @boundscheck !allunique(positions) && throw(ArgumentError("Elements of $(positions) must be unique"))
    table.meta.columns[positions]
end

function Base.getindex(tb::DFTable, index::ColumnIndexType)
    !isopen(tb) && error("Table is not opened")
    res = DFTable{ReadOnly}(tb.path, DFTableMeta(tb.meta, meta_by_condition(tb, index)))
    res.is_opened = true
    return res
end

function Base.size(table::DFTable)
    isempty(columns_meta(table)) && return (0, 0)
    stats = table_stats(table[1], as_df=false)

    return (first(stats)[2].rows, length(columns_meta(table)))
end

function Base.size(table::DFTable, dim::Number)
    !(dim in 1:2) && throw(ArgumentError("dim must be in 1:2"))
    return size(table)[dim]
end

