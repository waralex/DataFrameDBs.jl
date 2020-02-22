abstract type DataIterate end
abstract type SizesIterate end

mutable struct BlockIterator{RI, IT}
    streams ::Vector{BlockStream}
    names ::Vector{Symbol}
    buffers ::Vector{AbstractVector}    
    block_size ::Int64
    position ::Int64
    row_index ::RI
    BlockIterator{IT}(streams::Vector{BlockStream}, names::Vector{Symbol},
                     buffers::Vector{<:AbstractVector}, block_size::Number,
                     row_index::RI) where {RI, IT} =
                      new{RI, IT}(streams, names, buffers, block_size, 1, row_index)
end

BlockIterator(streams::Vector{BlockStream}, names::Vector{Symbol}, buffers::Vector{<:AbstractVector}, block_size::Number) =
                BlockIterator{DataIterate}(streams, names, buffers, block_size, nothing)

BlockSizesIterator(streams::Vector{BlockStream}, names::Vector{Symbol}, block_size::Number, row_index::RI) where {RI} =
                BlockIterator{SizesIterate}(streams, names, AbstractVector[], block_size, row_index)

BlockSizesIterator(streams::Vector{BlockStream}, names::Vector{Symbol}, block_size::Number) =
                BlockSizesIterator(streams, names, block_size, nothing)



function eachblock(ios::Vector{<:IO}, columns ::Vector{ColumnMeta}, block_size::Integer, row_index = nothing) 
    length(ios) != length(columns) && error("Lenght of names must be equal to lenght of io")    
    streams = BlockStream.(ios)        
    buffers = make_materialization.(columns)
    return BlockIterator{DataIterate}(streams, getproperty.(columns, :name), buffers, block_size, row_index)
end

skipblocks(iter::BlockIterator{Nothing}) = eof(first(iter.streams))

@inline blockrange(iter::BlockIterator) = iter.position:iter.position + iter.block_size - 1

@inline function skipblocks(iter::BlockIterator{RI}) where {RI}
    
    max_row = maximum(iter.row_index)
    while !eof(first(iter.streams)) && iter.position <= max_row
        ind_intersect = intersect(blockrange(iter), iter.row_index)
        
        !isempty(ind_intersect) && return false
        skip_block.(iter.streams)
        
        iter.position += iter.block_size
    end
    
    return true
end

@inline function block_result(iter::BlockIterator{Nothing, DataIterate})    
        return OrderedDict{Symbol, AbstractVector}(
            Pair{Symbol, AbstractVector}.(iter.names, iter.buffers)
        )    
end

@inline function block_result(iter::BlockIterator{RI, DataIterate}) where {RI}
    
    row_index = intersect(blockrange(iter), iter.row_index) .- (iter.position - 1)
    return OrderedDict{Symbol, AbstractVector}(
        Pair{Symbol, AbstractVector}.(iter.names, getindex.(iter.buffers, Ref(row_index)))
    )    
end

function Base.iterate(iter::BlockIterator{T, DataIterate}, state = nothing) where {T}
    stop = !isempty(iter.streams) && skipblocks(iter)

    if (stop)
        close.(iter.streams)
        return nothing
    end

    sz = read_block!.(iter.streams, iter.buffers)
    @assert eof(first(iter.streams)) || first(sz).rows == iter.block_size "rows in col don't match to blocksize"
    result = (
        block_result(iter),
        nothing
    )    
    iter.position += iter.block_size
    
    return result
end

function eachsize(ios::Vector{<:IO}, columns ::Vector{ColumnMeta},  block_size::Integer, row_index = nothing)
    length(ios) != length(columns) && error("Lenght of names must be equal to lenght of io")    
    streams = BlockStream.(ios)    
    return BlockSizesIterator(streams, getproperty.(columns, :name), block_size, row_index)
end

@inline function block_result(iter::BlockIterator{Nothing, SizesIterate}, sizes::AbstractVector{SizeStats})    
    return OrderedDict{Symbol, SizeStats}(
        Pair{Symbol, SizeStats}.(iter.names, sizes)
    )    
end

@inline function block_result(iter::BlockIterator{RI, SizesIterate}, sizes::AbstractVector{SizeStats}) where {RI}
    
    nrows = length(intersect(blockrange(iter), iter.row_index) .- (iter.position - 1))    
    new_sizes = SizeStats.(nrows, getproperty.(sizes, :compressed), getproperty.(sizes, :uncompressed))    
    return OrderedDict{Symbol, SizeStats}(
        Pair{Symbol, SizeStats}.(iter.names, new_sizes)
    )    
end

function Base.iterate(iter::BlockIterator{T, SizesIterate}, state = nothing) where {T}
    #println(iter.row_index)
    stop = !isempty(iter.streams) && skipblocks(iter)

    if (stop)
        close.(iter.streams)
        return nothing
    end
    sizes = skip_block.(iter.streams)    
    result = (
        block_result(iter, sizes),
        nothing
    )    
    iter.position += iter.block_size
    return result
end