function table_stats(table::DFTable; as_df = true)
    meta = columns_meta(table)
    result = Dict(Pair{Symbol, SizeStats}.(getproperty.(meta, :name), Ref(SizeStats())))
    ios = open_files(table, mode = :read)
    for block in DataFrameDBs.eachsize(ios, meta)
        for (n, size) in block
            @inbounds result[n] += size
        end
    end    
    !as_df && return result

    
    pretty = pretty_stats.(values(result))

    df = DataFrames.DataFrame(
        [
            collect(keys(result)),
            getproperty.(meta_by_name.(Ref(table),keys(result)), :type),
            getproperty.(pretty, :rows),
            getproperty.(pretty, :uncompressed),
            getproperty.(pretty, :compressed),
            getproperty.(pretty, :compression_ratio),            
        ],
        [:column, :type, :rows, Symbol("uncompressed size"), Symbol("compressed size"), Symbol("compression ratio")]
    )

    totals = pretty_stats(totalstats(values(result)))
    push!(df, [Symbol("Table total"), Any, totals.rows, totals.uncompressed, totals.compressed, totals.compression_ratio])
    
    return df
end