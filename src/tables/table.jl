abstract type Editable end
abstract type ReadOnly end

mutable struct DFTable{T, RI}
    path ::String
    meta ::DFTableMeta
    is_opened ::Bool
    row_index ::RI
    rows ::Union{Nothing, Int64}
    DFTable{T}(path::AbstractString, meta::DFTableMeta) where {T} = new{T, Nothing}(path, meta, false, nothing, nothing)
    DFTable{T,RI}(path::AbstractString, meta::DFTableMeta, row_index::RI, is_opened::Bool) where {T, RI} = new{T, RI}(path, meta, is_opened, row_index, nothing)
end


function DFTable(base::DFTable{T, RI}, new_meta::DFTableMeta) where {T, RI} 
    DFTable{ReadOnly,RI}(base.path, new_meta, base.row_index, base.is_opened)
end

function DFTable(base::DFTable{T, RI}, new_index::RI2, new_meta::DFTableMeta) where {T, RI, RI2} 
    DFTable{ReadOnly,RI2}(base.path, new_meta, new_index, base.is_opened)
end

function Base.:(==)(a::DFTable{T1,RI1}, b::DFTable{T2,RI2}) where {T1, RI1, T2, RI2}
    T1 == T2 &&
    RI1 == RI2 &&
    a.row_index == b.row_index &&
    columns_meta(a) == columns_meta(b)
end

DFTable(path::AbstractString, meta::DFTableMeta) = DFTable{Editable}(path, meta)

function Base.show(io::IO, table::DFTable) 
    !isopen(table) && return print(io, "closed table")
    println(io, materialize(table[1:20,:]))
    print(io, "...")
end



blocksize(t::DFTable) = t.meta.block_size
Base.isopen(t::DFTable) = t.is_opened
columns_meta(t::DFTable) = t.meta.columns
row_index(t::DFTable) = t.row_index

function nrows(table::DFTable)
    isempty(columns_meta(table)) && return 0
    stats = table_stats(table[:,1], as_df=false)

    return first(stats)[2].rows
end

function Base.size(table::DFTable)
    isempty(columns_meta(table)) && return (0, 0)

    return (nrows(table), length(columns_meta(table)))
end

function Base.size(table::DFTable, dim::Number)
    !(dim in 1:2) && throw(ArgumentError("dim must be in 1:2"))
    return size(table)[dim]
end

