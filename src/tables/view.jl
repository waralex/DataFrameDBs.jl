struct TableView
    table::DFTable
    columns ::Vector{Symbol}
    filter ::FilterQueue
    TableView(table::DFTable, columns::Vector{Symbol}, filter::FilterQueue = FilterQueue()) = new(table, columns, filter)    
end

Base.names(v::TableView) = v.columns
Base.parent(v::TableView) = v.table

Base.show(io::IO, v::TableView) = print(io, "table view")

getmeta(v::TableView, name::Symbol) = getmeta(v.table, name)

required_columns(v::TableView) = unique(vcat(v.columns, required_columns(v.filter)))


function nrows(table_view::TableView)
    isempty(table_view.columns) && return 0
    result = 0
    for n in eachsize(table_view)
        result += n
    end
    return result
end

function Base.size(table_view::TableView)
    isempty(table_view.columns) && return (0, 0)

    return (nrows(table_view), length(table_view.columns))
end

function Base.size(table_view::TableView, dim::Number)
    !(dim in 1:2) && throw(ArgumentError("dim must be in 1:2"))
    return size(table_view)[dim]
end

Base.size(table::DFTable, args...) = Base.size(table[:,:], args...)