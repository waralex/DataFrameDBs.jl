function check_schema(table, data)
    schema = Tables.schema(data)
    isnothing(schema) && ArgumentError("Tables.schema undefinded for $(data)")
    (length(columns_meta(table)) != length(schema.names)) && throw(ArgumentError("column count in $data don't match table columns count"))
    (collect(schema.types) != getproperty.(columns_meta(table), :type)) && throw(ArgumentError("column type in $data don't match table columns types"))
end

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

function insert(table::DFTable, rows; show_progress = false, close_on_done = true)
    progress = show_progress ? 
        ProgressMeter.ProgressUnknown("Insertion to table $(table.path): ") :
        nothing
    
    start_time = Dates.now()
    last_progress = Dates.now()

    check_schema(table, rows)
    ios = open_files(table, mode = :rewrite)
    streams = BlockStream.(ios)
    writed_sizes = SizeStats[SizeStats() for i in  1:length(streams)]
    

    buffers = make_write_buffer.(columns_meta(table))
    lb_rows = first(seek_to_lastblock.(streams, blocksize(table)))
    if lb_rows > 0
        last_block_cols = make_materialization.(columns_meta(table))
        read_block_and_reset!.(streams, last_block_cols)
        buffers .= collect.(last_block_cols)
    end
    
    resize!.(buffers, blocksize(table))
    offset = lb_rows + 1
    size = lb_rows
    for row in Tables.rows(rows)
        
        for i in 1:length(buffers)
            buffers[i][offset] = Tables.getcolumn(row, i)
        end
        
        size += 1
        offset += 1
        if offset == blocksize(table) + 1
            prepare_block_write!.(streams, buffers)
            writed_sizes .+= commit_block_write!.(streams)
            if !isnothing(progress) && Dates.now() - last_progress > Dates.Millisecond(100)
                
                totals = totalstats(values(writed_sizes))
                    
                pretty = pretty_stats(totals)

                row_per_sec = totals.rows / Dates.value(Dates.now() - start_time) * 1000
                prow_per_sec = humanrows(row_per_sec) * "/sec"

                ProgressMeter.update!(progress, totals.rows,
                showvalues = [
                    ("rows", pretty.rows),
                    ("uncompressed size", pretty.uncompressed),
                    ("compressed size", pretty.compressed),
                    ("compression ratio", pretty.compression_ratio),
                    ("processing", prow_per_sec)
                    ]
                )
                last_progress = Dates.now()
            end
            offset = 1
            size = 0
        end
    end
    
    if size > 0
        resize!.(buffers, size)        
        prepare_block_write!.(streams, buffers)
        writed_sizes += commit_block_write!.(streams)
    end
    if !isnothing(progress)
                
        totals = totalstats(values(writed_sizes))
            
        pretty = pretty_stats(totals)

        row_per_sec = totals.rows / Dates.value(Dates.now() - start_time) * 1000
        prow_per_sec = humanrows(row_per_sec) * "/sec"

        ProgressMeter.update!(progress, totals.rows,
        showvalues = [
            ("rows", pretty.rows),
            ("uncompressed size", pretty.uncompressed),
            ("compressed size", pretty.compressed),
            ("compression ratio", pretty.compression_ratio),
            ("processing", prow_per_sec)
            ]
        )
        last_progress = Dates.now()
    end
    !isnothing(progress) && ProgressMeter.finish!(progress)
    close_on_done && close.(streams)
    return table
end

make_column_storage(::Type{T}) where {T} = Vector{T}(undef, 0)
make_column_storage(::Type{String}) = FlatStringsVector()


make_write_buffer(meta ::ColumnMeta) where {T} = Vector{meta.type}(undef, 0)