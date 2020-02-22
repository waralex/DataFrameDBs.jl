struct Column{T} <: AbstractVector{T}
    table ::DFTable
    meta ::DFTableMeta
    
end