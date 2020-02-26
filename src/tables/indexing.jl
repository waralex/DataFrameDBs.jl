

meta_by_condition(table::DFTable, ::Colon) = table.meta.columns

meta_by_condition(table::DFTable, i::Integer) = [table.meta.columns[i]]

meta_by_condition(table::DFTable, name::Symbol) = [getmeta(table, name)]

@inline function meta_by_condition(table::DFTable, names::AbstractVector{Symbol}) 
    @boundscheck !allunique(names) && throw(ArgumentError("Elements of $(names) must be unique"))    
    return getmeta.(Ref(table), names)
end
@inline function meta_by_condition(table::DFTable, positions::AbstractVector{<:Integer}) 
    @boundscheck !allunique(positions) && throw(ArgumentError("Elements of $(positions) must be unique"))
    columns_meta(table)[positions]
end
@inline function meta_by_condition(table::DFTable, positions::AbstractRange{<:Integer})     
    columns_meta(table)[positions]
end

Base.@propagate_inbounds colnames_by_index(tb::DFTable, index::ColumnIndexType) = getproperty.(meta_by_condition(tb, index), :name)

colnames_by_index(v::TableView, ::Colon) = v.columns[:]
colnames_by_index(v::TableView, i::Integer) = [v.columns[i]]
function colnames_by_index(v::TableView, name::Symbol)
    res = findfirst(x->x == name, v.columns)
    isnothing(res) && KeyError(name)
    return v.columns[res]
end
function colnames_by_index(v::TableView, positions::AbstractVector{<:Integer})
    @boundscheck !allunique(positions) && throw(ArgumentError("Elements of $(positions) must be unique"))
    return v.columns[positions]
end
function colnames_by_index(v::TableView, names::AbstractVector{Symbol})
    @boundscheck !allunique(names) && throw(ArgumentError("Elements of $(names) must be unique"))        
    return colnames_by_index.(Ref(v), names)
end

function colnames_by_index(v::TableView, positions::AbstractRange{<:Integer})     
    v.columns[positions]
end


function Base.lastindex(tb::DFTable, dim)     
    dim == 2 && return Base.lastindex(columns_meta(tb))
    dim == 1 && return nrows(tb)
    error("not realized")
end

function Base.getindex(tb::Union{DFTable, TableView}, index::ColumnIndexType)    
    error(" [column] unsupported, use [:, column]")
end


gettable(tb::DFTable) = tb
gettable(v::TableView) = v.table

Base.@propagate_inbounds function Base.getindex(tb::DFTable, row_index::Colon, col_index::ColumnIndexType)
    return TableView(tb, colnames_by_index(tb, col_index))
end

function Base.getindex(tb::DFTable, row_index::RowIndexType, col_index::ColumnIndexType)
    return TableView(tb, colnames_by_index(tb, col_index), add(FilterQueue(), row_index))    
end

function Base.getindex(v::TableView, row_index::Union{RowIndexType, Colon}, col_index::ColumnIndexType)
    return TableView(v.table, colnames_by_index(v, col_index), add(v.filter, row_index))    
end

selection(v::DFTable, filter::Pair{<:Tuple{Vararg{Symbol}}, <:Function}) = selection(v[:,:], filter)
    
function selection(v::TableView, filter::Pair{<:Tuple{Vararg{Symbol}}, <:Function})
    cols = collect(filter[1])
    !allunique(cols) && throw(ArgumentError("Elements of $(cols) must be unique"))
    metas = getmeta.(Ref(v), cols)
    
    types = typeof.(make_materialization.(metas))
    filter = FuncFilter(types, filter[1], filter[2])
    return TableView(v.table, v.columns[:], add(v.filter, filter))
end