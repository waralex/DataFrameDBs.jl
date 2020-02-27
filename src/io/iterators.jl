abstract type DataIterate end
abstract type SizesIterate end

mutable struct IteratorProgress
    
end

function start_progress_task()
    return Channel(spawn = true) do ch
        progress = ProgressMeter.ProgressUnknown("Processing data rows")
        total_rows = 0
        start = Dates.now()
        last_show = Dates.now()
        while (rows = take!(ch)) !== nothing            
            total_rows += rows
            if Dates.now() - last_show > Dates.Millisecond(100)
                row_per_sec = total_rows / Dates.value(Dates.now() - start) * 1000
                prow_per_sec = humanrows(row_per_sec) * "/sec"
                ProgressMeter.update!(progress, total_rows,
                showvalues = [
                    ("rows", humanrows(total_rows)),
                    ("processing", prow_per_sec)
                    ]
                )
                last_show = Dates.now()
            end
        end
        row_per_sec = rows / Dates.value(Dates.now() - start) * 1000
        prow_per_sec = humanrows(total_rows) * "/sec"
        ProgressMeter.update!(progress, total_row,
        
        )
        ProgressMeter.finish!(progress)
        row_per_sec = rows / Dates.value(Dates.now() - start) * 1000
        prow_per_sec = humanrows(row_per_sec) * "/sec"
        println(stdout, "Processed $(total_rows) rows in $(Dates.canonicalize(Dates.CompoundPeriod(now() - start))), $(prow_per_sec)")
    end
end

mutable struct BlockIterator{IT}
    streams ::Vector{BlockStream}
    names ::Vector{Symbol}
    buffers ::Vector{AbstractVector}    
    block_size ::Int64
    position ::Int64
    filter ::FilterQueue
    progress ::Union{Nothing, Channel}
    BlockIterator{IT}(streams::Vector{BlockStream}, names::Vector{Symbol},
                     buffers::Vector{<:AbstractVector}, block_size::Number,
                     filter::FilterQueue, progress::Union{Nothing, <:Channel} = nothing) where {RI, IT} =
                      new{IT}(streams, names, buffers, block_size, 1, filter, progress)
end

mutable struct ProjectionIterator{IT}
    block_it ::BlockIterator{IT}
    names ::Vector{Symbol}
    buffers ::Vector{AbstractVector}
    filter ::FilterQueue

    ProjectionIterator(block_it ::BlockIterator{IT}, names::Vector{Symbol},
         buffers::Vector{<:AbstractVector}, filter::Filter) where {RI, IT, Filter} = 
            new{IT}(block_it, names, buffers, filter)
end

BlockIterator(streams::Vector{BlockStream}, names::Vector{Symbol}, buffers::Vector{<:AbstractVector}, block_size::Number) =
                BlockIterator{DataIterate}(streams, names, buffers, block_size, FilterQueue(), Nothing)

BlockSizesIterator(streams::Vector{BlockStream}, names::Vector{Symbol}, buffers::Vector{<:AbstractVector},
         block_size::Number, filter::FilterQueue = FilterQueue(), progress::Union{Nothing, Channel} = nothing) =
                BlockIterator{SizesIterate}(streams, names, buffers, block_size, filter, progress)
                

function eachblock(ios::Vector{<:IO}, columns ::Vector{ColumnMeta}, block_size::Integer, filter::FilterQueue = FilterQueue(), progress::Union{Nothing, Channel} = nothing) 
    length(ios) != length(columns) && error("Lenght of names must be equal to lenght of io")    
    streams = BlockStream.(ios)        
    buffers = make_materialization.(columns)    
    prepare!(filter) 
    return BlockIterator{DataIterate}(streams, getproperty.(columns, :name), buffers, block_size, filter, progress)
end

function eachprojection(block_it::BlockIterator, columns ::Vector{ColumnMeta}) 
    buffers = make_materialization.(columns)    
    return ProjectionIterator(block_it, getproperty.(columns, :name), buffers, block_it.filter)
end

function eachprojection(table_view::TableView) 
    req_columns = required_columns(table_view)
    req_meta = getmeta.(Ref(table_view.table), req_columns)

    ios = open_files(table_view)
    
    local_filter = deepcopy(table_view.filter)
    progress = nothing
    if (isshow_progress(table_view.table))
        progress = start_progress_task()
    end
    
    read_iterator = eachblock(ios, req_meta, blocksize(table_view.table), local_filter, progress)

    return eachprojection(read_iterator, getmeta.(Ref(table_view.table), table_view.columns))
end


@inline function skipblocks(iter::BlockIterator)
        
    while !eof(first(iter.streams))  
        iscompleted(iter.filter) && return true
        !skip_if_can(iter.filter, iter.block_size) && return false        
        skip_block.(iter.streams)
        
        iter.position += iter.block_size
    end
    
    return true
end


@inline function block_result(iter::BlockIterator{DataIterate}) where {RI}
    return OrderedDict{Symbol, AbstractVector}(
            Pair{Symbol, AbstractVector}.(iter.names, iter.buffers)
        )    
end

function Base.iterate(iter::BlockIterator{DataIterate}, state = nothing) 
    stop = !isempty(iter.streams) && skipblocks(iter)

    if (stop)
        close.(iter.streams)
        !isnothing(iter.progress) && put!(iter.progress, nothing)
        return nothing
    end

    sz = read_block!.(iter.streams, iter.buffers)
    @assert eof(first(iter.streams)) || first(sz).rows == iter.block_size "rows in col don't match to blocksize"
    !isnothing(iter.progress) && put!(iter.progress, first(sz).rows)
    result = (
        block_result(iter),
        nothing
    )    
    iter.position += iter.block_size
    
    return result
end

function fill_projection_buffer!(buffer::AbstractVector{T}, read_buffer::AbstractVector{T}, index) where {T}
    (length(buffer) != length(index)) && resize!(buffer, length(index))
    buffer .= view(read_buffer, index)
end

#FIXME it's not good, but allocation / dealocation of string is worse
function fill_projection_buffer!(buffer::FlatStringsVectors.FlatStringsVector, read_buffer::FlatStringsVectors.FlatStringsVector, index) where {T}
    resize!(buffer.sizes, length(index))
    new_data_size = 0
    for ri in index
        (read_buffer.sizes[ri] > 0) && (new_data_size += read_buffer.sizes[ri])
    end
    
    FlatStringsVectors.resize_data!(buffer, new_data_size)
    
    offset = 0
    for (i, ri) in enumerate(index)
        if read_buffer.sizes[ri] > 0
            GC.@preserve buffer read_buffer begin
                unsafe_copyto!(pointer(buffer.data) + offset, pointer(read_buffer.data) + read_buffer.offsets[ri], read_buffer.sizes[ri])            
            end
            offset += read_buffer.sizes[ri]
        end
        buffer.sizes[i] = read_buffer.sizes[ri]        
    end
    
    FlatStringsVectors.unsafe_remake_offsets!(buffer)
    
end

function try_fill_projection_buffer(iter::ProjectionIterator{DataIterate}, block, index)
    
    result = false
    if !isempty(index)
        for (i, name) in enumerate(iter.names)
            fill_projection_buffer!(iter.buffers[i], block[1][name], index)        
        end
        result = true
    end
    
    return result
end

function Base.iterate(iter::ProjectionIterator{DataIterate}, state = nothing) where {T}  
    while true
        block = Base.iterate(iter.block_it)
        (isnothing(block) || iscompleted(iter.filter)) && return nothing

        
        res = try_fill_projection_buffer(iter, block, apply(iter.filter, block[1]))
        if res        
            result = (
                OrderedDict{Symbol, AbstractVector}(
                    Pair{Symbol, AbstractVector}.(iter.names, iter.buffers)
                ),    
                nothing
            )                            
            return result
        end

    
    end
    
    
end

function eachsize(ios::Vector{<:IO}, columns ::Vector{ColumnMeta},  block_size::Integer, filter::FilterQueue = FilterQueue(), progress = nothing)
    length(ios) != length(columns) && error("Lenght of names must be equal to length of io")    
    streams = BlockStream.(ios)    
    buffers = make_materialization.(columns)   
    prepare!(filter)
    return BlockSizesIterator(streams, getproperty.(columns, :name), buffers, block_size, filter, progress)
end

function eachsize(table_view::TableView) 
    req_columns = required_columns(table_view.filter)
    isempty(req_columns) && (req_columns = [first(table_view.columns)])
    
    req_meta = getmeta.(Ref(table_view.table), req_columns)
    ios = open_files(table_view, req_columns)
    local_filter = deepcopy(table_view.filter)    
    progress = nothing
    if (isshow_progress(table_view.table))
        progress = start_progress_task()
    end
    return eachsize(ios, req_meta, blocksize(table_view.table), local_filter, progress)
end


#=@inline function block_result(iter::BlockIterator{Nothing, SizesIterate}, sizes::AbstractVector{SizeStats})    
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
end=#

function Base.iterate(iter::BlockIterator{SizesIterate}, state = nothing)
    while true
        stop = (!isempty(iter.streams)) && skipblocks(iter)
                
        if (stop)
            close.(iter.streams)
            !isnothing(iter.progress) && put!(iter.progress, nothing)
            return nothing
        end
        nrows = Int64(0)
        if isonly_range(iter.filter) 
            sizes = skip_block.(iter.streams)
            !isnothing(iter.progress) && put!(iter.progress, first(sizes).rows)
            nrows = length(apply_only_range(iter.filter, first(sizes).rows))
        else
            sizes = read_block!.(iter.streams, iter.buffers)
            !isnothing(iter.progress) && put!(iter.progress, first(sizes).rows)
            tmp = OrderedDict{Symbol, AbstractVector}(
                Pair{Symbol, AbstractVector}.(iter.names, iter.buffers)
            )    
            nrows = length(apply(iter.filter, tmp))
        end
        iter.position += iter.block_size
        
        nrows > 0 && return (nrows, nothing)
    end
    
end