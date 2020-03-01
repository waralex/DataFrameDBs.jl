make_buffer(::Type{T}) where {T} = Vector{T}(undef, 0)

make_buffer(::Type{String}) = FlatStringsVector{String}()
make_buffer(::Type{Union{String, Missing}}) = FlatStringsVector{Union{String, Missing}}()

make_buffer(meta::ColumnMeta) = make_buffer(meta.type)

make_buffer(table::DFTable, col::Symbol) = make_buffer(getmeta(table, col))

make_materialization(::Type{T}) where {T} = Vector{T}(undef, 0)
make_materialization(::Type{Bool}) = BitVector(undef, 0)

make_materialization(meta ::ColumnMeta) where {T} = make_materialization(meta.type)
make_materialization(table::DFTable, col::Symbol) = make_materialization(getmeta(table, col))

function make_materialization(v::DFView)
    req_columns = required_columns(v.projection)
    req_meta = getmeta.(Ref(v.table), req_columns)
    return (; zip(keys(v.projection.cols), make_materialization.(req_meta))...)
end



function materialize(v::DFView)
    result = make_materialization(v)
    for block in BlocksIterator(v)
        for (res, bl) in zip(result, block)
            append!(res, bl)
        end
    end
    
    DataFrames.DataFrame(result, copycols = false)
end

function materialize(table::DFTable)
    return materialize(DFView(table))
end