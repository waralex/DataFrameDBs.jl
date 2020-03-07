mutable struct RowIterator{BlockItT}
    block_it::BlockItT
    function RowIterator(v::DFView)
        block_it = BlocksIterator(v)
        return new{typeof(block_it)}(block_it)
    end 
end
"""
    rows(v::DFView)
    rows(v::DFTable)
Return by row iterator of table or view. Rows represented as NamedTuples
"""
rows(v::DFView) = RowIterator(v)
rows(t::DFTable) = RowIterator(DFView(t))

row_tuple_from_res(res::NamedTuple{Cols}, i::Integer) where Cols = NamedTuple{Cols}(getindex.(values(res), i))

function Base.iterate(it::RowIterator)
    block_res = iterate(it.block_it)
    isnothing(block_res) && return nothing
    block_data = block_res[1]
    return (
        row_tuple_from_res(block_data, 1),
        (block_data, 1)
        )
end

function Base.iterate(it::RowIterator, state)
    (block_data, inblock_pos) = state
    inblock_pos += 1
    if inblock_pos <= length(first(block_data))
        return (
            row_tuple_from_res(block_data, inblock_pos),
            (block_data, inblock_pos)
            )
    end

    block_res = iterate(it.block_it)
    isnothing(block_res) && return nothing
    block_data = block_res[1]
    return (
        row_tuple_from_res(block_data, 1),
        (block_data, 1)
        )
end