
struct ColRef{T}
    name::Symbol
end

struct BlockBroadcasting{RT, F, Args<:Tuple}
    f::F
    args::Args
    function BlockBroadcasting(func::F, args::Args) where {F<:Function, Args<:Tuple{Vararg{Union{<:ColRef, <:BlockBroadcasting}}}}                
        types_tuple = _sig_tuple(args)        
        !hasmethod(func, types_tuple) && throw(ArgumentError("function hasn't method for columns types $(types_tuple)"))
        res_types = Base.return_types(func, types_tuple)
        (length(res_types) != 1) && throw(ArgumentError("funciton must have single return type"))        
        new{res_types[1], F, Args}(func, args)
    end    
end

_sig_tuple(args::Tuple) = (Base.eltype(args[1]), _sig_tuple(Base.tail(args))...)
_sig_tuple(args::Tuple{}) = ()

function required_columns(b::BlockBroadcasting)
   keys(columns_buffers(b))
end

Base.eltype(::Type{<:BlockBroadcasting{RT}}) where {RT} = RT 
Base.eltype(::Type{ColRef{T}}) where {T} = T 

Base.show(io::IO, c::ColRef{T}) where {T} = print(io, "col(", c.name, ")::", T)

function Base.show(io::IO, f::BlockBroadcasting{RT, F, Args}) where {RT, F, Args}
    print(io, f.f, "(")
    for i in 1:length(f.args)
        i > 1 && print(io, ", ")
        print(io, f.args[i])
    end
    print(io, ")::", RT)
end

struct BroadcastExecutor{BT, InBuff, OutBuff}
    broadcasting::BT
    in_buffers ::InBuff
    buffer ::OutBuff
    BroadcastExecutor(broadcasting:: BT, in_buffers::InBuff, buffer::OutBuff) where {BT, InBuff, OutBuff} = 
                        new{BT, InBuff, OutBuff}(broadcasting, in_buffers, buffer)
    
end

function BroadcastExecutor(block_broad::BlockBroadcasting{RT, F, Args}) where {RT, F, Args}
    in_buffers = columns_buffers(block_broad)
    buffer = _coltype_buffer(RT)
    args = _boradcasted_args(in_buffers, block_broad.args)
    broad = Base.Broadcast.broadcasted(block_broad.f, args...)
    broad = Base.Broadcast.flatten(broad)
    return BroadcastExecutor(broad, in_buffers, buffer)

end

_coltype_buffer(::Type{T}) where {T} = Vector{T}(undef, 0)
_coltype_buffer(::Type{Bool}) = BitVector(undef, 0)
_column_buffer(el::ColRef{T}) where {T} = NamedTuple{(el.name,)}((_coltype_buffer(T),))
_column_buffer(el::BlockBroadcasting) = _columns_buffers(el.args)
_columns_buffers(args::Tuple) = merge(_column_buffer(args[1]), _columns_buffers(Base.tail(args)))
_columns_buffers(args::Tuple{T}) where {T} = _column_buffer(args[1])
_columns_buffers(args::Tuple{}) = NamedTuple{(), Tuple{}}(())

function columns_buffers(b::BlockBroadcasting)
    _columns_buffers(b.args)
end

_boradcasted_arg(buffers::NamedTuple, arg::ColRef) = buffers[arg.name]
function _boradcasted_arg(buffers::NamedTuple, arg::BlockBroadcasting) 
    Base.Broadcast.broadcasted(arg.f, _boradcasted_args(buffers, arg.args)...)
end

_boradcasted_args(buffers::NamedTuple, args::Tuple) = (
                    _boradcasted_arg(buffers, args[1]),
                    _boradcasted_args(buffers, Base.tail(args))...
                )
_boradcasted_args(buffers::NamedTuple, args::Tuple{}) = ()

function _extract_for_eval!(dest::NamedTuple, data::NamedTuple, range, cols::Tuple)    
    @inbounds begin
        resize!(dest[cols[1]], length(range))
        dest[cols[1]] .= data[cols[1]][range]
        _extract_for_eval!(dest, data, range, Base.tail(cols))    
    end
end
   

_extract_for_eval!(dest::NamedTuple, data::NamedTuple, range, cols::Tuple{}) = nothing


function eval_on_range(all_columns::NamedTuple,
    exec::BroadcastExecutor{BT, InBuff, OutBuff},
    range::Union{<:AbstractVector{<:Integer}, <:Integer, AbstractRange{<:Integer}}) where {BT, InBuff, OutBuff}
    
    _extract_for_eval!(exec.in_buffers, all_columns, range, keys(exec.in_buffers))
    
   resize!(exec.buffer, length(range))    
   Base.Broadcast.materialize!(exec.buffer, exec.broadcasting)
        
   return exec.buffer
end