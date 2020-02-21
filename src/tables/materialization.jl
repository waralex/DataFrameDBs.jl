make_materialization(::Type{T}) where {T} = Vector{T}(undef, 0)

make_materialization(::Type{String}) = FlatStringsVector{String}()
make_materialization(::Type{Union{String, Missing}}) = FlatStringsVector{Union{String, Missing}}()

make_materialization(meta::ColumnMeta) = make_materialization(meta.type)


function materialize(table::DFTable)
    meta = columns_meta(table)
    result = make_materialization.(meta)
    ios = open_files(table, mode = :read)
    for block in DataFrameDBs.eachblock(ios, meta)
        for (i, col) in enumerate(block)
            append!(result[i], col[2])
        end
    end
    
    df =  DataFrames.DataFrame(
        result,
        map(m->m.name, meta),
        copycols = false
    )
    return df
end