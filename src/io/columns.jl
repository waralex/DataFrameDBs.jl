mutable struct BlockIterator    
    streams ::Vector{BlockStream}
    names ::Vector{Symbol}
    buffers ::Vector{AbstractVector} 
end

#write_column_type(io::IO, ::AbstractArray{T}) where {T} = write(s, string(T))

function write_columns(ios::Vector{<:IO}, data::Vector{<:AbstractVector}, block_size; close_on_done = true)
    @assert length(ios) == length(data) "ios and data have must equal sizes"
    total_rows = minimum(length.(data))
    @assert total_rows == maximum(length.(data)) "data columns must has equal lengths"

    streams = BlockStream.(ios)

    offset = 1
    while offset + block_size  <= total_rows
        r = offset:(offset + block_size - 1)
        prepare_block_write!.(streams, getindex.(data, Ref(r)))  
        commit_block_write!.(streams)        
        offset += block_size
    end
    if offset <= total_rows
        r = offset:total_rows
        prepare_block_write!.(streams, getindex.(data, Ref(r)))
        commit_block_write!.(streams)                
    end    
    close_on_done && close.(streams)
end

make_column_storage(::Type{T}) where {T} = Vector{T}(undef, 0)
make_column_storage(::Type{String}) = FlatStringsVector()

function check_column_type_header(s::BlockStream, expected::Symbol)
    type_string = read(s, String)
    Symbol(type_string) != expected && error("read column error - column has type ", Symbol(type_string), " but expected type is ", expected)    
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




