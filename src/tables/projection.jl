struct Projection{Args<:NamedTuple}
    cols::Args
    function Projection(cols::Args) where {Args<:NamedTuple} 
        new{Args}(cols)
    end
    function Projection()
        new{NamedTuple{()}}(NamedTuple{()}(()))
    end
end

function Base.show(io::IO, s::Projection)
    print(io, "Projection: ")
    nms = keys(s)
    for i in 1:length(s.cols)
        i > 1 && print(io, "; ")
        print(io, nms[i], "=>", s.cols[i])
    end    
end

Base.keys(::Projection{NamedTuple{Cols, T}}) where {Cols, T} = Cols

Base.isempty(p::Projection) = Base.isempty(p.cols)
Base.length(p::Projection) = Base.length(p.cols)

function add(p::Projection, el::NamedTuple{Cols, <:Tuple{Vararg{<:Union{<:ColRef, <:BlockBroadcasting}}}}) where {Cols}
    for c in Cols
        (c in Base.keys(p)) && throw(ArgumentError("Duplicated column $(Cols[1])"))
    end
    return Projection(merge(p.cols, el))
end



#All indexes not type stable, but it is rare operations
function Base.getindex(p::Projection, i::Integer)
    Projection(
        NamedTuple{(keys(p)[i],)}((p.cols[i],))
    )
end

function Base.getindex(p::Projection, i::Union{AbstractRange{<:Integer}, AbstractArray{<:Integer}})
    Projection(
        NamedTuple{keys(p)[i]}(values(p.cols)[i])
    )
end

function _get_indexes(ids::Union{AbstractVector{Symbol},Tuple{Vararg{Symbol}}}, t::NamedTuple{Cols, T}) where {Cols, T}
    
    return Cols[1] in ids ?
        merge(NamedTuple{(Cols[1],)}((t[1],)), _get_indexes(ids, Base.tail(t))) :
        _get_indexes(ids, Base.tail(t))
end

_get_indexes(ids::Union{AbstractVector{Symbol},Tuple{Vararg{Symbol}}}, t::NamedTuple{(), Tuple{}}) = NamedTuple{(),Tuple{}}(())

function Base.getindex(p::Projection, i::Symbol)     
    Projection(
        _get_indexes([i], p.cols)
    )
end

function Base.getindex(p::Projection, i::Union{AbstractVector{Symbol},Tuple{Vararg{Symbol}}})     
    
    Projection(
        _get_indexes(i, p.cols)
    )
end

_proj_eltype(::ColRef{T}) where {T} = T
_proj_eltype(::BlockBroadcasting{T}) where {T} = T

coltype(p::Projection, i::Number) = _proj_eltype(p.cols[i])
coltype(p::Projection, s::Symbol) = _proj_eltype(p.cols[s])

_proj_required_elem(e::ColRef) = (e.name,)
_proj_required_elem(e::BlockBroadcasting) = required_columns(e)

_proj_required_columns(t::Tuple) = (
            _proj_required_elem(t[1])..., _proj_required_columns(Base.tail(t))...
        )
_proj_required_columns(t::Tuple{}) = ()

function required_columns(p::Projection)
    return (
        unique( 
        collect(_proj_required_columns(values(p.cols)))
        )...,
    )
end

_proj_coltype_buffer(::Type{T}) where {T} = Vector{T}(undef, 0)
_proj_coltype_buffer(::Type{Bool}) = BitVector(undef, 0)

struct ColProjExec{T}
    name::Symbol
    buffer::T
    ColProjExec(name::Symbol, buffer::T) where{T} = new{T}(name, buffer)
    ColProjExec(c::ColRef{T}) where{T} = new{typeof(_proj_coltype_buffer(T))}(c.name, _proj_coltype_buffer(T))
end

_proj_convert_to_exe(t::ColRef) = ColProjExec(t)
_proj_convert_to_exe(t::BlockBroadcasting) = BroadcastExecutor(t)

_proj_convert_to_exe(t::NamedTuple{Cols}) where {Cols} =
                merge(
                    NamedTuple{(Cols[1],)}((_proj_convert_to_exe(t[1]),)),
                    _proj_convert_to_exe(Base.tail(t))
                )
_proj_convert_to_exe(t::NamedTuple{(), Tuple{}}) = NamedTuple{(), Tuple{}}(())

struct ProjectionExecutor{Args}
    cols::Args    
    ProjectionExecutor(args::Args) where {Args} = new{Args}(args)
end

function ProjectionExecutor(p::Projection)     
    ProjectionExecutor(_proj_convert_to_exe(p.cols))
end

_proj_elem_eval_on_range(elem::BroadcastExecutor, data::NamedTuple, range) = eval_on_range(data, elem, range)

function _proj_elem_eval_on_range(elem::ColProjExec, data::NamedTuple, range) 
    resize!(elem.buffer, length(range))
    elem.buffer .= data[elem.name][range]
end

function _proj_eval_on_range(dest::Tuple, data::NamedTuple, range)
    _proj_elem_eval_on_range(dest[1], data, range)
    _proj_eval_on_range(Base.tail(dest), data, range)
end
_proj_eval_on_range(dest::Tuple{}, data::NamedTuple, range) = nothing
    
_proj_res_elem(el::ColProjExec) = el.buffer
_proj_res_elem(el::BroadcastExecutor) = el.buffer

_proj_res_tuple(t::Tuple) = (
    _proj_res_elem(t[1]), _proj_res_tuple(Base.tail(t))...
)
_proj_res_tuple(t::Tuple{}) = ()

function eval_on_range(all_columns::NamedTuple,
    exec::ProjectionExecutor{<:NamedTuple{Cols}},
    range::Union{<:AbstractVector{<:Integer}, <:Integer, AbstractRange{<:Integer}}) where {Cols}
    _proj_eval_on_range(values(exec.cols), all_columns, range)
    return NamedTuple{Cols}(_proj_res_tuple(values(exec.cols)))
end