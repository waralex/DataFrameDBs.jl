struct BlockBroadcasting{RT, F, Cols, Types<:Tuple}
    f::F
    function BlockBroadcasting(types::AbstractVector{<:Type}, cols::Tuple{Vararg{Symbol}}, func::F) where {F<:Function}        
        check_type(x) = x <: AbstractVector
        !all(check_type.(types)) &&  throw(ArgumentError("all types must by abstract vectors"))
        length(types) != length(cols) &&  throw(ArgumentError("length of types != length of columns"))
        types_tuple = tuple(types...)
        elt = eltype.(types_tuple)
        !hasmethod(func, elt) && throw(ArgumentError("function hasn't method for columns types $(elt)"))
        res_types = Base.return_types(func, elt)
        (length(res_types) != 1) && throw(ArgumentError("funciton must have single return type"))        
        new{res_types[1], F, cols, Tuple{types_tuple...}}(func)
    end    
end

function Base.show(io::IO, f::BlockBroadcasting{RT, F, Cols, Types}) where {RT, F, Cols, Types}
    print(io, "(")
    join(io, Cols, ", ")    
    print(io, ")=>", f.f, "::", RT)
end

Base.eltype(b::BlockBroadcasting{RT}) where {RT} = RT 

Base.@propagate_inbounds _extract_cols(args::Tuple, d::NamedTuple) =(d[args[1]], _extract_cols(Base.tail(args), d)...)
Base.@propagate_inbounds _extract_cols(args::Tuple{Symbol}, d::NamedTuple) = (d[args[1]],)
Base.@propagate_inbounds _extract_cols(args::Tuple{}, d::NamedTuple) = ()

Base.@propagate_inbounds function extract_cols(d::NamedTuple, ::BlockBroadcasting{RT, F, Cols, Types}) where {RT, F, Cols, Types}
    return _extract_cols(Cols, d)
end


broadbuffer(::Type{T}) where T = Vector{T}(undef, 0)
broadbuffertype(::Type{T}) where T = Vector{T}

broadbuffer(::Type{Bool}) where T = BitVector(undef, 0)
broadbuffertype(::Type{Bool}) where T = BitVector

buffertype(b::BlockBroadcasting{RT}) where {RT}  = buffertype(RT)

struct BroadcastExecutor{BuffT, F, Cols, Types<:Tuple}
    f::F
    buffer ::BuffT
    function BroadcastExecutor(broadcasting ::BlockBroadcasting{RT, F, Cols, Types}) where {RT, F, Cols, Types}
        return new{broadbuffertype(RT), F, Cols, Types}(broadcasting.f, broadbuffer(RT)) 
    end
end

Base.@propagate_inbounds function extract_cols(d::NamedTuple, ::BroadcastExecutor{BuffT, F, Cols, Types}) where {BuffT, F, Cols, Types}
    return _extract_cols(Cols, d)
end

function eval_on_range(all_columns::NamedTuple,
     broadcasting::BroadcastExecutor{BuffT, F, Cols, Types},
     range::Union{Vector{<:Integer}, <:Integer, AbstractRange{<:Integer}}) where {F, Cols, Types, BuffT}
        
    args = extract_cols(all_columns, broadcasting)    
    args = view.(args, Ref(range))
    resize!(broadcasting.buffer, length(range))    
    broadcast!(broadcasting.f, broadcasting.buffer, args...)        
    return broadcasting.buffer
end