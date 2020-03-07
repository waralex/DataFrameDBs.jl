"""
    table_stats(table::DFTable)

Show row count and table space
"""
function table_stats(table::DFTable)
 meta = columns_meta(table)
    isempty(table.meta.columns) && DataFrames.DataFrame
    result = OrderedDict(Pair{Symbol, SizeStats}.(getproperty.(meta, :name), Ref(SizeStats())))
    
    ios = open_files(table, mode = :read)
    streams = BlockStream.(ios)
    
    while !eof(first(streams))
        sizes = skip_block.(streams)

        for (i, r) in enumerate(result)            
            result[r[1]] += sizes[i]
        end
    end    

    
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

function _isavailableunion(::Type{Union{T, Missing}}) where {T} 
    isbitstype(T) && return true
    T == String && return true
    return false
end
function _isavailableunion(::Type{Union})
    return false
end

function isavailabletype(::Type{T}) where {T}
    isbitstype(T) && return true
    T == String && return true
    return _isavailableunion(T)
end