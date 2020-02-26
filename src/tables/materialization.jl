make_materialization(::Type{T}) where {T} = Vector{T}(undef, 0)

make_materialization(::Type{String}) = FlatStringsVector{String}()
make_materialization(::Type{Union{String, Missing}}) = FlatStringsVector{Union{String, Missing}}()

make_materialization(meta::ColumnMeta) = make_materialization(meta.type)

make_materialization(table::DFTable, col::Symbol) = make_materialization(getmeta(table, col))


function materialize(table::DFTable)
    return materialize(table[:,:])
end

function materialize(table_view::TableView)
    result = make_materialization.(getmeta.(Ref(table_view), table_view.columns))
    
    for block in eachprojection(table_view)
        for (i, col) in enumerate(block)
            append!(result[i], col[2])
        end
    end
    
    df =  DataFrames.DataFrame(
        result,
        table_view.columns,
        copycols = false
    )
    return df
end