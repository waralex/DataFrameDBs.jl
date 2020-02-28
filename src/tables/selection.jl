const SelectionRangeType = Union{Vector{<:Integer}, <:Integer, AbstractRange{<:Integer}}
const SelectionElemType = Union{SelectionRangeType, BlockBroadcasting}

struct SelectionQueue{Types<:Tuple}
    queue ::Tuple{Vararg{SelectionElemType}}
    SelectionQueue() = new{Tuple{}}(Tuple{}())
    function SelectionQueue(queue ::T) where T<:Tuple{Vararg{SelectionElemType}}
         new{T}(queue)
    end
end

Base.last(s::SelectionQueue) = Base.last(s.queue)
Base.isempty(s::SelectionQueue) = Base.isempty(s.queue)
Base.length(s::SelectionQueue) = Base.length(s.queue)

add(q::SelectionQueue, ::Colon) = q

#add(q::SelectionQueue{Tuple{}}, r::SelectionElemType) = SelectionQueue((r,))

Base.@propagate_inbounds _new_queue(old::Tuple, elem::SelectionElemType) = (old[1], _new_queue(Base.tail(old), elem)...)
Base.@propagate_inbounds _new_queue(old::Tuple{SelectionRangeType}, elem::SelectionRangeType) = (Base.reindex((old[1],), (elem,))[1],)
Base.@propagate_inbounds _new_queue(old::Tuple{SelectionRangeType}, elem::BlockBroadcasting) = (old[1], elem)
Base.@propagate_inbounds _new_queue(old::Tuple{}, elem::SelectionElemType) = (elem,)

_check_element(r::SelectionRangeType) = 0
function _check_element(r::BlockBroadcasting)
    (eltype(r) != Bool) && throw(ArgumentError("Broadcasting for selection must have Bool type"))
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
    RangeToProcess(range::T) where {T} = new{T}(range)
end

const SelectionExRangeType = Union{
        RangeToProcess{Vector{<:Integer}}, RangeToProcess{<:Integer}, RangeToProcess{<:AbstractRange{<:Integer}}
    }
const SelectionExElemType = Union{SelectionExRangeType, BroadcastExecutor}

_convert_to_exe(t::Tuple{SelectionRangeType, Vararg}) = (RangeToProcess(t[1]), _convert_to_exe(Base.tail(t))...)
_convert_to_exe(t::Tuple{BlockBroadcasting, Vararg}) = (BroadcastExecutor(t[1]), _convert_to_exe(Base.tail(t))...)
_convert_to_exe(t::Tuple{}) = ()

struct SelectionExecutor{SelT}
    queue ::Tuple{Vararg{SelectionExElemType}}
    SelectionExecutor(s::SelectionQueue{T}) where {T} = new{T}(_convert_to_exe(s.queue))
end

function _apply_to_block(range, t::Tuple{SelectionExRangeType, Vararg}, block::Union{NamedTuple, Nothing})    
    inblock_part = intersect(1:length(range), t[1].range)
    new_range = Base.reindex((range,), (inblock_part,))[1]    
    t[1].range = t[1].range .- length(range)    
    return isempty(new_range) ? 
            Int64[] :
            _apply_to_block(new_range, Base.tail(t), block)
end

function _apply_to_block(range, t::Tuple{BroadcastExecutor, Vararg}, block::NamedTuple)        
    new_range = Base.reindex((range,), (eval_on_range(block, t[1], range),))[1]
    return isempty(new_range) ? 
            Int64[] :
            _apply_to_block(new_range, Base.tail(t), block)
end

_apply_to_block(range, ::Tuple{}, ::Union{NamedTuple, Nothing}) = range

function apply(s::SelectionExecutor, rows::Integer, block::Union{NamedTuple, Nothing})
    _apply_to_block(1:rows, s.queue, block)
end

_is_finished(t::Tuple{SelectionExRangeType, Vararg}) = maximum(t[1].range) < 1 ? true : _is_finished(Base.tail(t))
_is_finished(t::Tuple{BroadcastExecutor, Vararg}) = _is_finished(Base.tail(t))
_is_finished(::Tuple{}) = false

is_finished(s::SelectionExecutor) = _is_finished(s.queue)
