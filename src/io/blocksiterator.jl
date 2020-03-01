mutable struct BlocksIterator{StreamsTuple, BuffTuple, ProjT, SelT, ProjColsTuple, SelColsTuple}
    streams ::StreamsTuple
    buffers ::BuffTuple
    projection ::ProjT
    selection ::SelT
    proj_cols ::ProjColsTuple
    sel_cols ::SelColsTuple
    block_size ::Int64
    BlocksIterator(streams::StreamsTuple, buff::BuffTuple, proj::ProjT,
                   sel::SelT, proj_cols::ProjColsTuple,
                   sel_cols::SelColsTuple, block_size::Int64) where {StreamsTuple, BuffTuple, ProjT, SelT, ProjColsTuple, SelColsTuple} =
                   new{StreamsTuple, BuffTuple, ProjT, SelT, ProjColsTuple, SelColsTuple}(streams, buff, proj, sel, proj_cols, sel_cols, block_size)
end

function BlocksIterator(v::DFView)
    req_columns = required_columns(v)
    req_meta = getmeta.(Ref(v.table), req_columns)
    ios = open_files(v.table, req_columns)
    streams = (; zip(req_columns, BlockStream.(ios))...)
    buffers = (; zip(req_columns, make_materialization.(req_meta))...)
    
    sel_cols = required_columns(v.selection)
    proj_cols = required_columns(v.projection)
    #if sel_cols is empty we need any proj col to read sizes 
    (isempty(sel_cols) && !isempty(proj_cols)) && (sel_cols = (first(proj_cols),))
    proj_cols = (setdiff(
                    required_columns(v.projection), sel_cols
                    )...,)

    result = BlocksIterator(streams, buffers, ProjectionExecutor(v.projection),
                         SelectionExecutor(v.selection), proj_cols, sel_cols, blocksize(v.table))
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

function Base.iterate(it::BlocksIterator, state = nothing)
    while true
        
        stop = !isempty(it.streams) && skipblocks(it)
        if stop            
            close.(values(it.streams))
            return nothing
        end
        sz = read_cols(it, it.sel_cols)
        range = apply(it.selection, sz.rows, it.buffers)        
        if isempty(range)
            skip_cols(it, it.proj_cols)
        else
            read_cols(it, it.proj_cols)
            return (eval_on_range(it.buffers, it.projection, range), nothing)
        end


    end
end