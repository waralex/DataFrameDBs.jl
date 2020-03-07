"""
    DFColumn{T}

Lazy representation of table column. Do not instantate it directly, use indexing of table or view.

# Notes

DFColumn is not AbstractVector, but it support iteration and getindex. Iteration is much more efficient then consequentially
get index.
You can materialize DFColumn to Vector with [materialize(c::DFColumn)](@ref)
Broadcasting of DFColumn also supported. Broadcast which arguments is DFColumns and, optionally, scalars is DFColumn too.

# Examples
```julia
t = open_table("test_table") #table with `price` column

col = t.price

count = length(col)

price_condition = 10 .< t.price .< 40

in_condition_count = sum(price_condition)

in_condition = col[price_condition]

first_100_in_condition_vector = materialize(in_condition[1:100])
```
"""
struct DFColumn{T}
    view::DFView
    function DFColumn(view::DFView) 
        size(view, 2) != 1 && throw(ArgumentError("Column projection must contains singe element"))
        
        new{coltype(view.projection, 1)}(view)
    end
end

Base.:(==)(a::DFColumn, b::DFColumn) = a.view == b.view

Base.show(io::IO, c::DFColumn) = print(io, typeof(c))

Base.eltype(::Type{DFColumn{T}}) where {T} = T
Base.eltype(::DFColumn{T}) where {T} = T

Base.size(c::DFColumn) = (nrow(c.view),)

Base.size(c::DFColumn, dim::Number) = dim == 1 ? nrow(c.view) : 1

Base.length(c::DFColumn) = Base.size(c, 1)

Base.lastindex(c::DFColumn) = nrow(c.view)
    

Base.ndims(c::DFColumn) = 1
Base.ndims(c::Type{DFColumn}) = 1

Base.IndexStyle(::Type{<:DFColumn}) = IndexLinear()

function Base.getindex(c::DFColumn, i::AbstractRange) 
    DFColumn(selection(c.view, i))
end

function Base.getindex(c::DFColumn, i::DFColumn{Bool}) 
    c.view.selection != i.view.selection && throw(ArgumentError("cols must have same selections"))
    DFColumn(selection(c.view, i))
end


map_to_column(f::Function, c::DFColumn) = map_to_column(f, c.view)

function selection(v::DFView, col::DFColumn{Bool})
    v.selection != col.view.selection && throw(ArgumentError("col must have same selection as view"))
    selection(v, col.view.projection.cols[1])
end

function Base.setproperty!(v::DFView, name::Symbol, value::DFColumn)  
    !issameselection(v, value.view) && throw(ArgumentError("Can't add column with another selection"))
    
    return v.projection = add(v.projection, (;(name=>value.view.projection.cols[1],)...,))            
end

function Base.copyto!(dest::AbstractVector, src::DFColumn)
    
    offset = 1
    for block in BlocksIterator(src.view)
        view(dest, offset:(offset + length(block[1]) - 1)) .= block[1]
        offset += length(block[1])
    end
    return dest   
end

function Base.getindex(c::DFColumn, i::Number)
    println("aa ", i)
    tmp_view = selection(c.view, i)
    it = BlocksIterator(tmp_view)
    res = iterate(it)
    isnothing(res) && throw(BoundsError(c, i))
    return res[1][1][1]
end


function Base.iterate(c::DFColumn{T}) where {T}
    it = BlocksIterator(c.view)
    block_res = iterate(it)
    isnothing(block_res) && return nothing
    block_data = block_res[1][1]
    inblock_pos = 1
    return (
        block_data[inblock_pos], (it, block_data, inblock_pos)
        )
end

function Base.iterate(c::DFColumn, state)
    (it, block_data, inblock_pos) = state
    inblock_pos += 1
    if inblock_pos <= length(block_data)
        return (block_data[inblock_pos], (it, block_data, inblock_pos))
    end
    block_res = iterate(it)
    isnothing(block_res) && return nothing
    block_data = block_res[1][1]
    
    return (
        block_data[1], (it, block_data, 1)
        )
end
"""
    DFView(cols::NamedTuple{Cols, <:Tuple{Vararg{<:DFColumn}}})
    DFView(;kwargs...)

Make new view from DFColums. All columns must have same selection

# Examples

```julia
df = DataFrame((a=collect(1:100), b = collect(1:100)))

t = create_table("test", from = df)
v = DFView(col1 = t.a .+ t.b, col2 = t.a .- t.b, col3 = t.a .* t.a)
materialize(v)
```
"""
function DFView(cols::NamedTuple{Cols, <:Tuple{Vararg{<:DFColumn}}}) where {Cols}
    table_selection = nothing
    for col in cols
        if (isnothing(table_selection))
            table_selection = (col.view.table, col.view.selection)
        else
            ((col.view.table, col.view.selection) != table_selection) && throw(ArgumentError("All columns must have same selection and table"))
        end        
    end
    
    projection = Projection(
        (;zip(Cols, map(c->c.view.projection.cols[1], cols))...)
        )
    
    return DFView(
        table_selection[1],
        projection,
        table_selection[2]
    )
end

DFView(;kwargs...) = DFView((;kwargs...,))