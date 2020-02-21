
function write(path::String, data)
    cols = Tables.columns(data)
    names = collect(Symbol, Base.names(Tables.columns(cols)))
    columns = [Tables.getcolumn(data, nm) for nm in names]
    return create_table(path, names, columns)
end

#=

getvector(Tables.getcolumn(x, nm)) for nm in names
function DataFrame(x::T; copycols::Bool=true) where {T}
    if x isa AbstractVector && all(col -> isa(col, AbstractVector), x)
        return DataFrame(Vector{AbstractVector}(x), copycols=copycols)
    end
    if x isa AbstractVector || x isa Tuple
        if all(v -> v isa Pair{Symbol, <:AbstractVector}, x)
            return DataFrame(AbstractVector[last(v) for v in x], [first(v) for v in x],
                             copycols=copycols)
        end
    end
    cols = Tables.columns(x)
    names = collect(Symbol, Tables.columnnames(cols))
    return fromcolumns(cols, names, copycols=copycols)
end=#