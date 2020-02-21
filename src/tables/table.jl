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

