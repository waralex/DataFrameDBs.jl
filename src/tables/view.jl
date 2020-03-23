"""
    DFView

Lazy view of table. Do not instantate it directly, use indexing of table

# Notes 

DFView is characterized by projection and selection. Projection is columns of view and selection is conditions and/or range of rows
Indexing operations on the table are proxied to indexing on the full view of the table (i.e. view with no restrictions in selection and all table rows in projection).
Columns of DFView also accessible as properties
To get DataFrame from DFView use [materialize(v::DFView)](@ref)

# Examples
```julia
df = DataFrame((a=collect(1:100), b = collect(1:100), c = collect(1:100)))
t = create_table("test", from = df)
t[:,:]  #full view of table
v = t[:, [:a,:c]]
v2 = v[1:20, :] # == t[1:20, [:a,:c]]
v = t[:a=>(a)->a<50, :] #view with rows where value of column a less then 50
v = t[(:a, :b)=>(a, b)->a + b < 50, :] #view with rows where sum of columns a and b less then 50
v = t[t.a .+ t.b .< 50, :] #same as above, but using broadcast of columns
v = t[:, (e = :a, k=(:a, :c)=>(a,c)->a+c)] #view with columns :e (projection of origin column :a) and column :k (sum of origin columns a and c)
```julia 
"""
mutable struct DFView
    table::DFTable
    projection::Projection
    selection::SelectionQueue
    DFView(table::DFTable, proj::Projection, sel::SelectionQueue)  = new(table, proj, sel)
end


function Base.:(==)(a::DFView, b::DFView)
    a.table == b.table &&
    a.projection == b.projection &&
    a.selection == b.selection
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

Base.getindex(v::DFView, s::Number, p::Union{Number, Symbol}) = DFColumn(selproj(v, s, [p]))[1]

Base.getindex(v::DFView, s::Any, p::Pair{<:Tuple{Vararg{Symbol}},<:Function}) = DFColumn(selproj(v, s, (a=p,)))
Base.getindex(v::DFView, s::Any, p::Pair{Symbol,<:Function}) = DFColumn(selproj(v, s, (a=p,)))

function Base.getindex(v::DFView, s::Number, p::Any)
    for r in rows(selproj(v, [s], p))
        return r
    end
    throw(BoundsError(v, s))
end


Base.getindex(v::DFTable, select::Any, project::Any) = Base.getindex(DFView(v), select, project)



"""
    map_to_column(f::Function, v::DFView)
    map_to_column(f::Function, v::DFView)

Return DFColumn by applying function to each row of DFView
Result type of function must be supported by DFColumn

# Examples

```julia
df = DataFrame((a=collect(1:100), b = collect(1:100), c = collect(1:100)))
t = create_table("test", from = df)

map_to_column(t[1:50, [:a,:c]]) do a, c 
    return a < 10 ? a : b
end
```
"""
function map_to_column(f::Function, v::DFView)
    return v[:,names(v) => f]
end

map_to_column(f::Function, t::DFTable) = map_to_column(f, t[:,:])


function Base.getproperty(v::DFView, name::Symbol)  
    (name in fieldnames(typeof(v))) && return getfield(v, name)
    return v[:, name]
end



function Base.getproperty(v::DFTable, name::Symbol)  
    (name in fieldnames(typeof(v))) && return getfield(v, name)
    return v[:, name]
end


issametable(a::DFView, b::DFView) = a.table == b.table
issameselection(a::DFView, b::DFView) = issametable(a, b) && a.selection == b.selection

function required_columns(v::DFView)
    (unique(
        [
            required_columns(v.projection)...,
            required_columns(v.selection)...
        ]
        )...,)
end

function nrow(v::DFView)
    #error("check")
    res = 0
    iter = BlockRowsIterator(v)
    r = iterate(iter)
    while true        
        isnothing(r) && break        
        res += r[1]
        r = iterate(iter, r[2])
    end
    #for rows in BlockRowsIterator(v)
    #    res += rows
    #end
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

function Base.lastindex(v::DFView, dim::Number)
    !(dim in 1:2) && throw(ArgumentError("DFView have only 2 dimensions"))
    dim == 1 && return nrow(v)
    dim == 2 && return ncol(v)
end
Base.lastindex(v::DFTable, dim::Number) = Base.lastindex(v[:,:], dim)