

meta_by_condition(table::DFTable, ::Colon) = table.meta.columns

meta_by_condition(table::DFTable, i::Integer) = [table.meta.columns[i]]

function meta_by_name(table::DFTable, name::Symbol)
    res = findfirst(x->x.name == name, table.meta.columns)
    isnothing(res) && KeyError(name)
    return columns_meta(table)[res]
end

meta_by_condition(table::DFTable, name::Symbol) = [meta_by_name(table, name)]

@inline function meta_by_condition(table::DFTable, names::Vector{Symbol}) 
    @boundscheck !allunique(names) && throw(ArgumentError("Elements of $(names) must be unique"))
    
    return meta_by_name.(Ref(table), names)
end
@inline function meta_by_condition(table::DFTable, positions::Vector{<:Integer}) 
    @boundscheck !allunique(positions) && throw(ArgumentError("Elements of $(positions) must be unique"))
    columns_meta(table)[positions]
end
@inline function meta_by_condition(table::DFTable, positions::AbstractRange{<:Integer})     
    columns_meta(table)[positions]
end

function Base.lastindex(tb::DFTable, dim)     
    dim == 2 && return Base.lastindex(columns_meta(tb))
    dim == 1 && return nrows(tb)
    error("not realized")
end

function Base.getindex(tb::DFTable, index::ColumnIndexType)    
    error(" [column] unsupported, use [:, column]")
end


function Base.getindex(tb::DFTable, row_index::Colon, col_index::ColumnIndexType)
    return DFTable(tb, DFTableMeta(tb.meta, meta_by_condition(tb, col_index)))    
end

function Base.getindex(tb::DFTable{T,Nothing}, row_index::RowIndexType, col_index::ColumnIndexType) where T
   return DFTable(tb, row_index, DFTableMeta(tb.meta, meta_by_condition(tb, col_index))) 
end

function Base.getindex(tb::DFTable{T,<:RowIndexType}, row_index::RowIndexType, col_index::ColumnIndexType) where T
    reindexed = Base.reindex((tb.row_index,), (row_index,))
    return DFTable(tb, reindexed[1], DFTableMeta(tb.meta, meta_by_condition(tb, col_index))) 
 end