const ColumnIndexType = Union{Vector{Symbol}, Vector{<:Integer}, Colon, <:Integer, Symbol, AbstractRange{<:Integer}}
const RowIndexType = Union{Vector{<:Integer}, <:Integer, AbstractRange{<:Integer}}

struct FuncFilter{F, Cols, Types<:Tuple}
    f::F
    function FuncFilter(types::AbstractVector{<:Type}, cols::Tuple{Vararg{Symbol}}, func::F) where {F<:Function}        
        check_type(x) = x <: AbstractVector
        !all(check_type.(types)) &&  throw(ArgumentError("all types must by abstract vectors"))
        length(types) != length(cols) &&  throw(ArgumentError("length of types != length of columns"))
        types_tuple = tuple(types...)
        elt = eltype.(types_tuple)
        !hasmethod(func, elt) && throw(ArgumentError("function hasn't method for columns types $(elt)"))
        res_types = Base.return_types(func, elt)
        (length(res_types) != 1 || res_types[1] != Bool) && throw(ArgumentError("funciton must return Bool for all cases"))        
        new{F, cols, Tuple{types_tuple...}}(func)
    end
    #FuncFilter(pair::Pair{<:Tuple{Vararg{Symbol}}, <:Function}) = FuncFilter(pair[1], pair[2])

end

Base.@propagate_inbounds _extract_args(args::Tuple, d::AbstractDict) =(d[args[1]], _extract_args(Base.tail(args), d)...)
Base.@propagate_inbounds _extract_args(args::Tuple{Symbol}, d::AbstractDict) = (d[args[1]],)
Base.@propagate_inbounds _extract_args(args::Tuple{}, d::AbstractDict) = ()
Base.@propagate_inbounds _getindex(args::Tuple, i) = (args[1][i], _getindex(Base.tail(args), i)...)
Base.@propagate_inbounds _getindex(args::Tuple{Any}, i) = (args[1][i],)
Base.@propagate_inbounds _getindex(args::Tuple{}, i) = ()

Base.@propagate_inbounds function extract_args(d::AbstractDict, ::FuncFilter{F, Cols, Types}) where {F, Cols, Types}
    return _extract_args(Cols, d)
end


function eval_on_range(d::AbstractDict, filter::FuncFilter{F, Cols, Types}, range::RowIndexType) where {F, Cols, Types}
    args = extract_args(d, filter)
    result = BitArray(undef, length(range))
    for (i, ind) in enumerate(range)
        @inbounds result[i] = filter.f(_getindex(args, ind)...)
    end
    return Base.reindex((range,), (result,))[1]
end

struct FilterQueue
    queue ::Vector{Union{RowIndexType, <:FuncFilter}}
    processed ::Vector{Int64}
    finished ::Bool
    FilterQueue() = new(Union{RowIndexType, FuncFilter}[], Int64[], false)
end

Base.length(q::FilterQueue) = Base.length(q.queue)

function _try_merge(elem::RowIndexType, new::RowIndexType)
    return Base.reindex((elem,), (new,))[1]
end
function _try_merge(elem::FuncFilter, new)
    return nothing
end

function read_range(q::FilterQueue)
    (isempty(q.queue) || first(q.queue) isa FuncFilter) && return nothing
    return first(q.queue)
end

_append_to_collist!(a::Vector{Symbol}, b::FuncFilter{F, Cols, Types}) where {F, Cols, Types} = append!(a, collect(Cols))

_append_to_collist!(a::Vector{Symbol}, b::RowIndexType) = a

function required_columns(q::FilterQueue)
    columns = Symbol[]
    _append_to_collist!.(Ref(columns), q.queue)
    return unique(columns)
    
end

function add(q::FilterQueue, range::RowIndexType)
    result = deepcopy(q)
    if isempty(result.queue) 
        push!(result.queue, range)
    else    
        merged = _try_merge(result.queue[end], range)
        if !isnothing(merged)
            result.queue[end] = merged
        else
            push!(result.queue, range)
        end
    end
    
    resize!(result.processed, length(result.queue))
    fill!(result.processed, 0)
    return result
end

function add(q::FilterQueue, func::FuncFilter)
    result = deepcopy(q)    
    push!(result.queue, func)
    resize!(result.processed, length(result.queue))
    fill!(result.processed, 0)
    return result
end

#=function add(q::FilterQueue, pair::Pair{<:Tuple{Vararg{Symbol}}, <:Function})
    result = deepcopy(q)
    push!(result.queue, FuncFilter(pair))    
    return result
end=#

@inline function apply_part(range::RowIndexType, chunk::AbstractDict{Symbol, <:AbstractVector}, elem::RowIndexType, already_processed::Int64)
    chunk_elem = intersect(elem .- already_processed, 1:length(range))
    isempty(chunk_elem) && return Int64[]
    res = Base.reindex((range,), (chunk_elem,))[1]      
    return res
end

#Base.@propagate_inbounds _getindex(args::Tuple, I) = (_broadcast_getindex(args[1], I), _getindex(tail(args), I)...)
#Base.@propagate_inbounds _getindex(args::Tuple{Any}, I) = (_broadcast_getindex(args[1], I),)
#Base.@propagate_inbounds _getindex(args::Tuple{}, I) = ()

@inline function apply_part(range::RowIndexType, chunk::AbstractDict{Symbol, <:AbstractVector}, elem::FuncFilter, already_processed::Int64)
    
    return eval_on_range(chunk, elem, range)
    
end

prepare!(q::FilterQueue) = fill!(q.processed, 0)

function apply(q::FilterQueue, chunk::AbstractDict{Symbol, <:AbstractVector})
    rows = length(first(chunk)[2])
    range = 1:rows    
    
    for i in 1:length(q.queue)        
        new_range = apply_part(range, chunk, q.queue[i], q.processed[i])        
        q.processed[i] += length(range)
        range = new_range
        isempty(range) && break
    end
    return range
end

iscompleted(elem::FuncFilter, processed::Int64) = false
iscompleted(elem::RowIndexType, processed::Int64) = maximum(elem .- processed) < 1

function iscompleted(q::FilterQueue)
    for i in 1:length(q.queue)        
        iscompleted(q.queue[i], q.processed[i]) && return true
    end
    return false
end