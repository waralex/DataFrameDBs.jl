mutable struct BlockIterator    
    streams ::Vector{BlockStream}
    names ::Vector{Symbol}
    buffers ::Vector{AbstractVector}
end

mutable struct BlockSizesIterator
    streams ::Vector{BlockStream}
    names ::Vector{Symbol}
end

function eachblock(ios::Vector{<:IO}, columns ::Vector{ColumnMeta})
    length(ios) != length(columns) && error("Lenght of names must be equal to lenght of io")    
    streams = BlockStream.(ios)    
    
    buffers = make_materialization.(columns)
    return BlockIterator(streams, getproperty.(columns, :name), buffers)
end

function Base.iterate(iter::BlockIterator, state = nothing)
    
    if (isempty(iter.streams) || eof(first(iter.streams)))
        close.(iter.streams)
        return nothing
    end
    read_block!.(iter.streams, iter.buffers)    
    return (
        OrderedDict{Symbol, AbstractVector}(
            Pair{Symbol, AbstractVector}.(iter.names, iter.buffers)
        ),
        nothing
    )    
end


function eachsize(ios::Vector{<:IO}, columns ::Vector{ColumnMeta})
    length(ios) != length(columns) && error("Lenght of names must be equal to lenght of io")    
    streams = BlockStream.(ios)    
    return BlockSizesIterator(streams, getproperty.(columns, :name))
end

function Base.iterate(iter::BlockSizesIterator, state = nothing)
    
    if (isempty(iter.streams) || eof(first(iter.streams)))
        close.(iter.streams)
        return nothing
    end
    sizes = skip_block.(iter.streams)    
    return (
        OrderedDict{Symbol, SizeStats}(
            Pair{Symbol, SizeStats}.(iter.names, sizes)
        ),
        nothing
    )    
end