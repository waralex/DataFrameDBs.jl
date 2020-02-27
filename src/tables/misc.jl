function table_stats(table::DFTable; as_df = true)
 meta = columns_meta(table)
    isempty(table.meta.columns) && return as_df ? DataFrames.DataFrame : OrderedDict()
    result = OrderedDict(Pair{Symbol, SizeStats}.(getproperty.(meta, :name), Ref(SizeStats())))
    
    ios = open_files(table, mode = :read)
    streams = BlockStream.(ios)
    #println(result)
    while !eof(first(streams))
        sizes = skip_block.(streams)

        for (i, r) in enumerate(result)            
            result[r[1]] += sizes[i]
        end
    end    

    
    !as_df && return result

    
    pretty = pretty_stats.(values(result))

    df = DataFrames.DataFrame(
        [
            collect(keys(result)),
            string.(getproperty.(getmeta.(Ref(table),keys(result)), :type)),
            getproperty.(pretty, :rows),
            getproperty.(pretty, :uncompressed),
            getproperty.(pretty, :compressed),
            getproperty.(pretty, :compression_ratio),            
        ],
        [:column, :type, :rows, Symbol("uncompressed size"), Symbol("compressed size"), Symbol("compression ratio")]
    )

    totals = pretty_stats(totalstats(values(result)))
    
    push!(df, [Symbol("Table total"), "", totals.rows, totals.uncompressed, totals.compressed, totals.compression_ratio])
    
    return df
end