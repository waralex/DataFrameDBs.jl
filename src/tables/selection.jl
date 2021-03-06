const SelectionRangeType = Union{<:AbstractVector{<:Integer}, <:Integer, AbstractRange{<:Integer}}
const SelectionElemType = Union{SelectionRangeType, BlockBroadcasting}

struct SelectionQueue{Args<:Tuple}
    queue ::Args
    SelectionQueue() = new{Tuple{}}(Tuple{}())
    function SelectionQueue(queue ::T) where T<:Tuple{Vararg{SelectionElemType}}
         new{T}(queue)
    end
end

function Base.show(io::IO, s::SelectionQueue)
    print(io, "Selection: ")
    for i in 1:length(s.queue)
        i > 1 && print(io, " |> ")
        print(io, s.queue[i])
    end    
end

Base.last(s::SelectionQueue) = Base.last(s.queue)
Base.isempty(s::SelectionQueue) = Base.isempty(s.queue)
Base.length(s::SelectionQueue) = Base.length(s.queue)

_sel_required_el(el::SelectionRangeType) = ()
_sel_required_el(el::BlockBroadcasting) = required_columns(el)
_sel_required_columns(t::Tuple) = [
        _sel_required_el(t[1])..., _sel_required_columns(Base.tail(t))...
    ]
_sel_required_columns(t::Tuple{}) = ()

function required_columns(s::SelectionQueue)
    (
        unique(_sel_required_columns(s.queue))...,
    )
end

add(q::SelectionQueue, ::Colon) = q

Base.@propagate_inbounds _new_queue(old::Tuple, elem::SelectionElemType) = (old[1], _new_queue(Base.tail(old), elem)...)
Base.@propagate_inbounds _new_queue(old::Tuple{SelectionRangeType}, elem::SelectionRangeType) = (old[1][elem], )#(Base.reindex((old[1],), (elem,))[1],)
Base.@propagate_inbounds _new_queue(old::Tuple{SelectionRangeType}, elem::BlockBroadcasting) = (old[1], elem)
Base.@propagate_inbounds _new_queue(old::Tuple{BlockBroadcasting}, elem::SelectionRangeType) = (old[1], elem)

Base.@propagate_inbounds _new_queue(old::Tuple{BlockBroadcasting}, elem::BlockBroadcasting) =
                        (
                            BlockBroadcasting(Base.:(&), (old[1], elem)),
                        )

Base.@propagate_inbounds _new_queue(old::Tuple{}, elem::SelectionElemType) = (elem,)
#Base.@propagate_inbounds _new_queue(old::Tuple{}, elem::BitArray) = ((1:length(elem))[elem],)

_check_element(r::SelectionRangeType) = 0
function _check_element(r::BlockBroadcasting)    
    (eltype(r) != Bool) && throw(ArgumentError("Function for selection must have Bool result type"))
end

function add(q::SelectionQueue, r::SelectionElemType)
    _check_element(r)
    SelectionQueue(_new_queue(q.queue, r)) 
end

_read_range(v::Tuple{}) = nothing
_read_range(v::Tuple{BlockBroadcasting, Vararg}) = nothing
_read_range(v::Tuple{SelectionRangeType, Vararg}) = v

read_range(s::SelectionQueue) = _read_range(s.queue)

mutable struct RangeToProcess{T}
    range::T
    offset::Int64
    first::Int64
    last::Int64
    RangeToProcess(range::T) where {T} = new{T}(range, 0, minimum(range), maximum(range))
    RangeToProcess(range::Union{BitArray, <:AbstractVector{Bool}}) = new{BitArray}(range, 0, 1, length(range))
end

const SelectionExRangeType = Union{
        RangeToProcess{<:AbstractVector{<:Integer}}, RangeToProcess{<:Integer}, RangeToProcess{<:AbstractRange{<:Integer}},
        RangeToProcess{<:BitArray}
    }
const SelectionExElemType = Union{SelectionExRangeType, BroadcastExecutor}

_sel_convert_to_exe(t::Tuple{SelectionRangeType, Vararg}) = (RangeToProcess(t[1]), _sel_convert_to_exe(Base.tail(t))...)
_sel_convert_to_exe(t::Tuple{BlockBroadcasting, Vararg}) = (BroadcastExecutor(t[1]), _sel_convert_to_exe(Base.tail(t))...)
_sel_convert_to_exe(t::Tuple{}) = ()

struct SelectionExecutor{Args}
    queue ::Args
    range_buffer ::Vector{Bool}#::Vector{Int64}
    SelectionExecutor(args::Args) where {Args} = new{Args}(args, Int64[])
end
SelectionExecutor(s::SelectionQueue{T}) where {T} = SelectionExecutor(_sel_convert_to_exe(s.queue))

function _apply_to_block(range, t::Tuple{SelectionExRangeType, Vararg}, block::Union{NamedTuple, Nothing})
    index = Base.LogicalIndex(range)
    inblock_part = intersect((1:length(index)) .+ t[1].offset, t[1].range) .- t[1].offset
    if inblock_part != 1:length(index)        
        @inbounds for (i, k) in enumerate(index)
            range[k] = i in inblock_part
        end
    end
    

    #new_range = Base.reindex((range,), (inblock_part,))[1]    
    #new_range = view(range, inblock_part)
    #t[1].range = t[1].range .- length(range)    
    t[1].offset += length(index)
    return isempty(range) ? 
            view(range, 1:0) :
            _apply_to_block(range, Base.tail(t), block)
end

#=function _apply_to_block(range, t::Tuple{RangeToProcess{<:BitArray}, Vararg}, block::Union{NamedTuple, Nothing})
    r = (1:length(range)) .+ t[1].offset
    new_range = view(range, view(t[1].range, r))
    
    t[1].offset += length(range)
    return isempty(new_range) ? 
            view(range, 1:0) :
            _apply_to_block(new_range, Base.tail(t), block)
end=#

#=function _apply_to_block(range, t::Tuple{RangeToProcess{<:AbstractArray{Bool}}, Vararg}, block::Union{NamedTuple, Nothing})
    r = (1:length(range)) .+ t[1].offset
    new_range = view(range, view(t[1].range, r))
    
    t[1].offset += length(range)
    return isempty(new_range) ? 
            view(range, 1:0) :
            _apply_to_block(new_range, Base.tail(t), block)
end=#

function _apply_to_block(range, t::Tuple{BroadcastExecutor, Vararg}, block::NamedTuple)
    index = Base.LogicalIndex(range)
    r = eval_on_range(block, t[1], index)
    
    if length(index) == length(range)
        @simd for i in 1:length(range)
            @inbounds range[i] = r[i]
        end            
    else
        i::Int = 1
        for k in index
            @inbounds range[k] = r[i]
            i+=1        
        end
    end
    
    
    
    
    
    #new_range = view(range, r)
    return isempty(range) ? 
            view(range, 1:0) :
            _apply_to_block(range, Base.tail(t), block)
end

_apply_to_block(range, ::Tuple{}, ::Union{NamedTuple, Nothing}) = range

function apply(s::SelectionExecutor, rows::Integer, block::Union{NamedTuple, Nothing})
    resize!(s.range_buffer, rows)
    fill!(s.range_buffer, 1)
    #s.range_buffer .= (1:rows)
    _apply_to_block(s.range_buffer, s.queue, block)
    return Base.LogicalIndex(s.range_buffer)
end

_isonly_range(t::Tuple{BroadcastExecutor, Vararg}) = false
_isonly_range(t::Tuple) = _isonly_range(Base.tail(t))
_isonly_range(t::Tuple{}) = true

function isonly_range(s::SelectionExecutor)
    return _isonly_range(s.queue)
end

function _skip_if_can(elem::SelectionExRangeType, size_to_skip::Integer)
    if elem.first - elem.offset > size_to_skip
        elem.offset += size_to_skip
        return true
    else
        return false
    end
end
_skip_if_can(elem::BroadcastExecutor, size_to_skip::Integer) = false

function skip_if_can(q::SelectionExecutor, size_to_skip::Integer)
    isempty(q.queue) && return false
    return _skip_if_can(q.queue[1], size_to_skip)
end

_is_finished(t::Tuple{SelectionExRangeType, Vararg}) = t[1].last <= t[1].offset ? true : _is_finished(Base.tail(t))
_is_finished(t::Tuple{BroadcastExecutor, Vararg}) = _is_finished(Base.tail(t))
_is_finished(::Tuple{}) = false

is_finished(s::SelectionExecutor) = _is_finished(s.queue)
