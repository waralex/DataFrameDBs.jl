struct DFView{Proj<:Projection, Sel<:SelectionQueue}
    table::DFTable
    projection::Proj
    selection::Sel
    DFView(table::DFTable, proj::Proj, sel::Sel) where {Proj, Sel} = new{Proj, Sel}(table, proj, sel)
end

ref_by_meta(meta::ColumnMeta) = ColRef{meta.type}(meta.name)

function full_table_projection(table::DFTable)
    nm_tuple = (names(table)...,)
    ref_tuple = (ref_by_meta.(columns_meta(table))...,)
    return Projection((;zip(nm_tuple, ref_tuple)...))
    
end

DFView(table::DFTable) = DFView(table, full_table_projection(table), SelectionQueue())

function Base.show(io::IO, v::DFView) 
    println(io, "View of table ", v.table.path)
    println(io, v.projection)
    println(io, v.selection)
end

function selection(v::DFView, el::SelectionElemType)
    DFView(v.table, v.projection, add(v.selection, el))
end

function selection(v::DFView, broad::Pair{<:Tuple{Vararg{Symbol}}, <:Function})
    cols = collect(broad[1])
    args = values(v.projection[cols].cols)
    return DFView(v.table, v.projection, add(v.selection, BlockBroadcasting(broad[2], args)))
end

selection(v::DFView, broad::Pair{Symbol, <:Function}) = selection(v, (broad[1],)=>broad[2])


proj_elem(v::DFView, elem::Symbol) = v.projection[elem].cols[1]

function proj_elem(v::DFView, elem::Pair{<:Tuple{Vararg{Symbol}}, <:Function}) 
    args = proj_elem.(Ref(v), elem[1])
    BlockBroadcasting(elem[2], args)
end

function projection(v::DFView, p::NamedTuple)
    names = keys(p)
    args = proj_elem.(Ref(v), values(p))
    return DFView(v.table,
        Projection((;zip(names, args)...)),
        v.selection)
end

function required_columns(v::DFView)
    (unique(
        [
            required_columns(v.projection)...,
            required_columns(v.selection)...
        ]
        )...,)
end

function materialize(v::DFView)
    it = BlocksIterator(v)
    #rr = (a=1)
    while true
        isnothing(iterate(it)) && break
        #isnothing(r) && break
        #rr = deepcopy(r[1])
        
    end
    #DataFrames.DataFrame(rr, copycols = false)
end

struct TableView
    table::DFTable
    columns ::Vector{Symbol}
    filter ::Selection
    TableView(table::DFTable, columns::Vector{Symbol}, filter::Selection = Selection()) = new(table, columns, filter)    
end

Base.names(v::TableView) = v.columns
Base.parent(v::TableView) = v.table

Base.show(io::IO, v::TableView) = print(io, "table view")

getmeta(v::TableView, name::Symbol) = getmeta(v.table, name)

required_columns(v::TableView) = unique(vcat(v.columns, required_columns(v.filter)))


function nrows(table_view::TableView)
    isempty(table_view.columns) && return 0
    result = 0
    for n in eachsize(table_view)
        result += n
    end
    return result
end

function Base.size(table_view::TableView)
    isempty(table_view.columns) && return (0, 0)

    return (nrows(table_view), length(table_view.columns))
end

function Base.size(table_view::TableView, dim::Number)
    !(dim in 1:2) && throw(ArgumentError("dim must be in 1:2"))
    return size(table_view)[dim]
end

Base.size(table::DFTable, args...) = Base.size(table[:,:], args...)