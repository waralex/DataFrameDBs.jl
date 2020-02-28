const ColumnIndexType = Union{Vector{Symbol}, Vector{<:Integer}, Colon, <:Integer, Symbol, AbstractRange{<:Integer}}
const RowIndexType = Union{Vector{<:Integer}, <:Integer, AbstractRange{<:Integer}}

struct FuncFilter{F, Cols, Types<:Tuple}
    f::F
    buffer::BitArray
    function FuncFilter(types::AbstractVector{<:Type}, cols::Tuple{Vararg{Symbol}}, func::F) where {F<:Function}        
        check_type(x) = x <: AbstractVector
        !all(check_type.(types)) &&  throw(ArgumentError("all types must by abstract vectors"))
        length(types) != length(cols) &&  throw(ArgumentError("length of types != length of columns"))
        types_tuple = tuple(types...)
        elt = eltype.(types_tuple)
        !hasmethod(func, elt) && throw(ArgumentError("function hasn't method for columns types $(elt)"))
        res_types = Base.return_types(func, elt)
        (length(res_types) != 1 || res_types[1] != Bool) && throw(ArgumentError("funciton must return Bool for all cases"))        
        new{F, cols, Tuple{types_tuple...}}(func,  BitArray(undef, 0))
    end
    #FuncFilter(pair::Pair{<:Tuple{Vararg{Symbol}}, <:Function}) = FuncFilter(pair[1], pair[2])

end

Base.@propagate_inbounds _extract_args(args::Tuple, d::AbstractDict) =(d[args[1]], _extract_args(Base.tail(args), d)...)
Base.@propagate_inbounds _extract_args(args::Tuple{Symbol}, d::AbstractDict) = (d[args[1]],)
Base.@propagate_inbounds _extract_args(args::Tuple{}, d::AbstractDict) = ()

Base.:(==)(a::FuncFilter{F, Cols, Types}, b::FuncFilter{F, Cols, Types}) where {F, Cols, Types} = a.f == b.f

function Base.show(io::IO, f::FuncFilter{F, Cols, Types}) where {F, Cols, Types}
    print(io, "(")
    join(io, Cols, ", ")    
    print(io, ")=>", f.f)
end


Base.@propagate_inbounds function extract_args(d::AbstractDict, ::FuncFilter{F, Cols, Types}) where {F, Cols, Types}
    return _extract_args(Cols, d)
end


function eval_on_range(d::AbstractDict, filter::FuncFilter{F, Cols, Types}, range::RowIndexType) where {F, Cols, Types}
    
    
    args = extract_args(d, filter)    
    args = view.(args, Ref(range))
    resize!(filter.buffer, length(range))
    
    broadcast!(filter.f, filter.buffer, args...)
        
    result = Base.reindex((range,), (filter.buffer,))[1]
    
    return result
end

struct Selection
    queue ::Vector{Union{RowIndexType, <:FuncFilter}}
    processed ::Vector{Int64}
    finished ::Bool
    Selection() = new(Union{RowIndexType, FuncFilter}[], Int64[], false)
end

function Base.:(==)(a::Selection, b::Selection)
    a.queue == b.queue
end

function Base.show(io::IO, s::Selection)
    print(io, "Selection: ")
    join(io, s.queue, " |> ")
end

Base.length(q::Selection) = Base.length(q.queue)
Base.isempty(q::Selection) = Base.isempty(q.queue)

function _try_merge(elem::RowIndexType, new::RowIndexType)
    return Base.reindex((elem,), (new,))[1]
end
function _try_merge(elem::FuncFilter, new)
    return nothing
end

function read_range(q::Selection)
    (isempty(q.queue) || first(q.queue) isa FuncFilter) && return nothing
    return first(q.queue)
end


isonly_range(q::Selection) = Base.isempty(q) || (length(q.queue) == 1 && first(q.queue) isa RowIndexType)

_append_to_collist!(a::Vector{Symbol}, b::FuncFilter{F, Cols, Types}) where {F, Cols, Types} = append!(a, collect(Cols))

_append_to_collist!(a::Vector{Symbol}, b::RowIndexType) = a

function required_columns(q::Selection)
    columns = Symbol[]
    _append_to_collist!.(Ref(columns), q.queue)
    return unique(columns)
    
end

add(q::Selection, ::Colon) = deepcopy(q)

function add(q::Selection, range::RowIndexType)
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

function add(q::Selection, func::FuncFilter)
    result = deepcopy(q)    
    push!(result.queue, func)
    resize!(result.processed, length(result.queue))
    fill!(result.processed, 0)
    return result
end

@inline function _apply_range(range::RowIndexType, elem::RowIndexType, already_processed::Int64)
    chunk_elem = intersect(elem .- already_processed, 1:length(range))
    isempty(chunk_elem) && return Int64[]
    res = Base.reindex((range,), (chunk_elem,))[1]      
    return res
end

@inline function apply_part(range::RowIndexType, chunk::AbstractDict{Symbol, <:AbstractVector}, elem::RowIndexType, already_processed::Int64)
    return _apply_range(range, elem, already_processed)
end


@inline function apply_part(range::RowIndexType, chunk::AbstractDict{Symbol, <:AbstractVector}, elem::FuncFilter, already_processed::Int64)
    
    return eval_on_range(chunk, elem, range)
    
end

prepare!(q::Selection) = fill!(q.processed, 0)

function _can_skip(elem::RowIndexType, processed::Integer, size_to_skip::Integer)
    return processed + size_to_skip < minimum(elem)
end
_can_skip(elem::FuncFilter, processed::Integer, size_to_skip::Integer) = false


function skip_if_can(q::Selection, size_to_skip::Integer)
    isempty(q.queue) && return false
    !_can_skip(q.queue[1], q.processed[1], size_to_skip) && return false
    q.processed[1] += size_to_skip
    return true
end

function apply(q::Selection, chunk::AbstractDict{Symbol, <:AbstractVector})
    
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

function apply_only_range(q::Selection, chunk_size)
    @assert isonly_range(q)
    isempty(q) && return 1:chunk_size
    range = 1:chunk_size
    result = _apply_range(range, q.queue[1], q.processed[1])
    q.processed[1] += length(range)
    return result
end

iscompleted(elem::FuncFilter, processed::Int64) = false
iscompleted(elem::RowIndexType, processed::Int64) = maximum(elem .- processed) < 1

function iscompleted(q::Selection)
    for i in 1:length(q.queue)        
        iscompleted(q.queue[i], q.processed[i]) && return true
    end
    return false
end