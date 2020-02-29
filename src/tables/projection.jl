struct Projection{Args<:NamedTuple}
    cols::Args
    function Projection(cols::Args) where {Args<:NamedTuple} 
        new{Args}(cols)
    end
end

Base.keys(::Projection{NamedTuple{Cols, T}}) where {Cols, T} = Cols


function add(p::Projection, el::NamedTuple{Cols, <:Tuple{Vararg}}) where {Cols}
    for c in Cols
        (c in Base.keys(p)) && throw(ArgumentError("Duplicated column $(Cols[1])"))
    end
    return Projection(merge(p.cols, el))
end

#function _get_index(cols::Tuple, vals::Tuple, )

#All indexes not type stable, but in rare operations
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

function _get_indexes(ids::Vector{Symbol}, t::NamedTuple{Cols, T}) where {Cols, T}
    return Cols[1] in ids ?
        merge(NamedTuple{(Cols[1],)}((t[1],)), _get_indexes(ids, Base.tail(t))) :
        _get_indexes(ids, Base.tail(t))
end

_get_indexes(ids::Vector{Symbol}, t::NamedTuple{(), Tuple{}}) = NamedTuple{(),Tuple{}}(())

function Base.getindex(p::Projection, i::Symbol)     
    Projection(
        _get_indexes([i], p.cols)
    )
end

function Base.getindex(p::Projection, i::AbstractArray{Symbol})     
    Projection(
        _get_indexes(i, p.cols)
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