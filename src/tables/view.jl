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

Base.names(v::DFView) = keys(v.projection)

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

selection(v::DFView, ::Colon) = v


function proj_elem(v::DFView, elem::Symbol) 
    pcols = v.projection[elem].cols
    isempty(pcols) && throw(ArgumentError("view don't have column :$(elem)"))
    return pcols[1]
end

function proj_elem(v::DFView, elem::Pair{<:Tuple{Vararg{Symbol}}, <:Function}) 
    args = proj_elem.(Ref(v), elem[1])
    BlockBroadcasting(elem[2], args)
end

function proj_elem(v::DFView, elem::Pair{Symbol, <:Function}) 
    args = proj_elem.(Ref(v), (elem[1],))
    BlockBroadcasting(elem[2], args)
end

function projection(v::DFView, p::NamedTuple)
    names = keys(p)
    args = proj_elem.(Ref(v), values(p))
    return DFView(v.table,
        Projection((;zip(names, args)...)),
        v.selection)
end

function projection(v::DFView, p::Union{AbstractRange{<:Integer}, AbstractArray{<:Integer}})    
    return DFView(v.table,
        v.projection[p],
        v.selection)
end

projection(v::DFView, p::AbstractVector{Symbol}) = projection(v, (;zip(p, p)...))

projection(v::DFView, p::AbstractVector{<:Pair{Symbol, Any}}) = projection(v, (;p...))

projection(v::DFView, ::Colon) = v


selproj(v::DFView, select::Any, project::Any) = projection(selection(v, select), project)


selproj(v::DFView, select::Colon, project::Any) = projection(v, project)

selproj(v::DFView, select::Any, project::Colon) = selection(v, select)
selproj(v::DFView, select::Colon, project::Colon) = v

Base.getindex(v::DFView, select::Any, project::Any) = selproj(v, select, project)


Base.getindex(v::DFView, s::Any, p::Union{Number, Symbol}) = DFColumn(selproj(v, s, [p]))


Base.getindex(v::DFTable, select::Any, project::Any) = Base.getindex(DFView(v), select, project)

function required_columns(v::DFView)
    (unique(
        [
            required_columns(v.projection)...,
            required_columns(v.selection)...
        ]
        )...,)
end

function nrow(v::DFView)
    res = 0
    for rows in BlockRowsIterator(v)
        res += rows
    end
    return res
end
function ncol(v::DFView)
    return length(v.projection)
end

nrow(t::DFTable) = nrow(DFView(t))
ncol(t::DFTable) = ncol(DFView(t))

Base.size(v::DFView) = (nrow(v), ncol(v))

function Base.size(v::DFView, dim::Number)
    
    !(dim in 1:2) && throw(ArgumentError("DFView have only 2 dimensions"))
    dim == 1 && return nrow(v)
    dim == 2 && return ncol(v)
    return 1
end

Base.size(t::DFTable) = Base.size(DFView(t))
Base.size(t::DFTable, dim::Number) = Base.size(DFView(t), dim)

function Base.lastindex(v::DFTable, dim::Number)
    !(dim in 1:2) && throw(ArgumentError("DFView have only 2 dimensions"))
    dim == 1 && return nrow(v)
    dim == 2 && return ncol(v)
end
