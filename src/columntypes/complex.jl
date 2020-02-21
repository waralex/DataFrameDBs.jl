function serialize(::Type{Union{T, Missing}}) where {T} 
    res = Ast(Symbol("Missing")) 
    push!(res, serialize(T))
    return res
end

deserialize(a::Val{Symbol("Missing")}, base) = Union{Missing, deserialize(base)}

function serialize(t::Type{<:Tuple})
    res = Ast(Symbol("Tuple")) 
    for sub_type in t.types
        push!(res, serialize(sub_type))
    end
    return res
end

function deserialize(a::Val{Symbol("Tuple")}, args...)
    isempty(args) && throw(UndefinedType("Tuple"))
    return Tuple{deserialize.(args)...}
end