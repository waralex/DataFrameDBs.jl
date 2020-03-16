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
    return make_materialization(v.projection)
end


"""
    materialize(v::DFView)
    materialize(table::DFTable)

Materialize DFView or DFTable as DataFrame
"""
function materialize(v::DFView)
    result = make_materialization(v)
    rows = nrow(v)
    for r in result
        sizehint!(r, rows)
    end
    for block in BlocksIterator(v)
        for (res, bl) in zip(result, block)
            append!(res, bl)
        end
    end
    
    DataFrames.DataFrame(result, copycols = false)
end
"""
    materialize(v::DFColumn)

Materialize DFColumn{T} as Vector{T}. Materialize is more efficient then collect(T, c::DFColumn{T})
"""
function materialize(c::DFColumn)
    result = make_materialization(c.view)[1]
    for block in BlocksIterator(c.view)        
        append!(result, block[1])        
    end    
    return result
end

function materialize(table::DFTable)
    return materialize(DFView(table))
end

"""
    head(v::DFView, rows = 10)
    head(t::DFTable, rows = 10)

Materialize first `rows` rows of DFView or DFTable
"""
head(v::DFView, rows = 10) = v[1:rows,:] |> materialize

head(t::DFTable, rows = 10) = head(DFView(t), rows)