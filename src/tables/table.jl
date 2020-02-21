abstract type AbstractDFColumn{T} <: AbstractVector{T} end

struct DFColumn{T,VT} <: AbstractDFColumn{T}
    path::String
    DFColumn{T, VT}(path) where {T,VT} = new(path)
end
DFColumn(path::String, ::Type{T}) where {T} = DFColumn{T, Vector{T}}(path)
DFColumn(path::String, ::Type{String})  = DFColumn{String, FlatStringsVector}(path)

Base.show(io::IO, c::DFColumn) = print(io, c.path)

mutable struct DFTable
    path ::String
    meta ::DFTableMeta
    is_opened ::Bool    
    DFTable(path::AbstractString, meta::DFTableMeta) = new(path, meta, false)
end

#=struct DFColumn
    table ::DFTable
    meta ::ColumnMeta
end=#

blocksize(t::DFTable) = t.meta.block_size
Base.isopen(t::DFTable) = t.is_opened
columns_meta(t::DFTable) = t.meta.columns

