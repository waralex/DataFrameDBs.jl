struct DataReader end
struct SizeReader end
mutable struct BlocksIterator{T, StreamsTuple, BuffTuple, ProjT, SelT, ProjColsTuple, SelColsTuple}
    streams ::StreamsTuple
    buffers ::BuffTuple
    projection ::ProjT
    selection ::SelT
    proj_cols ::ProjColsTuple
    sel_cols ::SelColsTuple
    block_size ::Int64
    progress ::Union{Nothing, Channel}
    BlocksIterator{T}(streams::StreamsTuple,
                   buff::BuffTuple, proj::ProjT,
                   sel::SelT, proj_cols::ProjColsTuple,
                   sel_cols::SelColsTuple,
                   block_size::Int64, progress::Union{Nothing, Channel} = nothing) where {T, StreamsTuple, BuffTuple, ProjT, SelT, ProjColsTuple, SelColsTuple} =
                   new{T, StreamsTuple, BuffTuple, ProjT, SelT, ProjColsTuple, SelColsTuple}(streams, buff, proj, sel, proj_cols, sel_cols, block_size, progress)
end

function BlocksIterator(v::DFView)
    req_columns = required_columns(v)
    req_meta = getmeta.(Ref(v.table), req_columns)
    ios = open_files(v.table, req_columns)
    streams = (; zip(req_columns, BlockStream.(ios))...)
    buffers = (; zip(req_columns, make_buffer.(req_meta))...)
    
    sel_cols = required_columns(v.selection)
    proj_cols = required_columns(v.projection)
    #if sel_cols is empty we need any proj col to read sizes 
    (isempty(sel_cols) && !isempty(proj_cols)) && (sel_cols = (first(proj_cols),))
    proj_cols = (setdiff(
                    required_columns(v.projection), sel_cols
                    )...,)


    progress = nothing
    if (isshow_progress(v.table))
        progress = read_progress_channel()
    end
    result = BlocksIterator{DataReader}(streams, buffers, ProjectionExecutor(v.projection),
                         SelectionExecutor(v.selection), proj_cols, sel_cols, blocksize(v.table), progress)
    precompile(Base.iterate, (typeof(result), Nothing))
    return result
end

function BlockRowsIterator(v::DFView)
    sel_cols = required_columns(v.selection)
    proj_cols = required_columns(v.projection)
    (isempty(sel_cols) && !isempty(proj_cols)) && (sel_cols = (first(proj_cols),))
    req_columns = sel_cols
    proj_cols = ()
    
    req_meta = getmeta.(Ref(v.table), req_columns)
    ios = open_files(v.table, req_columns)
    streams = (; zip(req_columns, BlockStream.(ios))...)
    buffers = (; zip(req_columns, make_buffer.(req_meta))...)

    progress = nothing
    if (isshow_progress(v.table))
        progress = read_progress_channel()
    end
    result = BlocksIterator{SizeReader}(streams, buffers, ProjectionExecutor(v.projection),
                         SelectionExecutor(v.selection), proj_cols, sel_cols, blocksize(v.table), progress)
    precompile(Base.iterate, (typeof(result), Nothing))
    return result
end


@inline function skipblocks(it::BlocksIterator)
        
    while !eof(first(it.streams))  
        is_finished(it.selection) && return true
        !skip_if_can(it.selection, it.block_size) && return false        
        skip_block.(values(it.streams))
    end
    
    return true
end

function read_cols(it::BlocksIterator, c::Tuple)::SizeStats
    read_block!(it.streams[c[1]], it.buffers[c[1]])
    read_cols(it, Base.tail(c))
end

function read_cols(it::BlocksIterator, c::Tuple{Symbol})::SizeStats
    read_block!(it.streams[c[1]], it.buffers[c[1]])
end
read_cols(it::BlocksIterator, c::Tuple{})::SizeStats = SizeStats()

function skip_cols(it::BlocksIterator, c::Tuple)::SizeStats
    skip_block(it.streams[c[1]])
    skip_cols(it, Base.tail(c))
end

skip_cols(it::BlocksIterator, c::Tuple{Symbol})::SizeStats = skip_block(it.streams[c[1]])
skip_cols(it::BlocksIterator, c::Tuple{})::SizeStats = SizeStats()

function Base.iterate(it::BlocksIterator{DataReader}, state = nothing)
    while true
        
        stop = !isempty(it.streams) && skipblocks(it)
        if stop            
            close.(values(it.streams))
            if !isnothing(it.progress)
                put!(it.progress, nothing)
                take!(it.progress)
            end            
            return nothing
        end
        sz = read_cols(it, it.sel_cols)
        range = apply(it.selection, sz.rows, it.buffers)        
        if isempty(range)
            skip_cols(it, it.proj_cols)
        else
            !isempty(it.proj_cols) && (sz = read_cols(it, it.proj_cols))
            !isnothing(it.progress) && put!(it.progress, sz.rows)
            return (eval_on_range(it.buffers, it.projection, range), nothing)
        end
        !isnothing(it.progress) && put!(it.progress, sz.rows)
    end
end

function Base.iterate(it::BlocksIterator{SizeReader}, state = nothing)
    while true
        
        stop = !isempty(it.streams) && skipblocks(it)
        if stop            
            close.(values(it.streams))
            if !isnothing(it.progress)
                put!(it.progress, nothing)
                take!(it.progress)
            end            
            return nothing
        end
        sz = read_cols(it, it.sel_cols)
        range = apply(it.selection, sz.rows, it.buffers)
        if !isempty(range)            
            !isnothing(it.progress) && put!(it.progress, sz.rows)
            return (length(range), nothing)
        end
        !isnothing(it.progress) && put!(it.progress, sz.rows)
    end
end