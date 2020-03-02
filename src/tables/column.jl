struct DFColumn{T,ViewT}
    view::ViewT
    function DFColumn(view::ViewT) where {ViewT}         
        size(view, 2) != 1 && throw(ArgumentError("Column projection must contains singe element"))
        
        new{coltype(view.projection, 1), ViewT}(view)
    end
end

Base.eltype(::Type{DFColumn{T}}) where {T} = T
Base.eltype(::DFColumn{T}) where {T} = T

Base.size(c::DFColumn) = (nrow(c.view),)

Base.size(c::DFColumn, dim::Number) = dim == 1 ? nrow(c.view) : 1

Base.length(c::DFColumn) = Base.size(c, 1)

Base.ndims(c::DFColumn) = 1

Base.IndexStyle(::Type{<:DFColumn}) = IndexLinear()

Base.getindex(c::DFColumn, i::AbstractRange) = DFColumn(selection(c.view, i))

function Base.copyto!(dest::AbstractVector, src::DFColumn)
    
    offset = 1
    for block in BlocksIterator(src.view)
        view(dest, offset:(offset + length(block[1]) - 1)) .= block[1]
        offset += length(block[1])
    end
    return dest   
end

function Base.getindex(c::DFColumn, i::Number)
    tmp_view = selection(c.view, i)
    it = BlocksIterator(tmp_view)
    res = iterate(it)
    isnothing(res) && throw(BoundsError(c, i))
    return res[1][1][1]
end


function Base.iterate(c::DFColumn{T, ViewT}) where {T, ViewT}
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