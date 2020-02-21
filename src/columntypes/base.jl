mutable struct Ast
    name ::Symbol
    childs ::Vector{Ast}
    Ast(name::Symbol) = new(name, Ast[])
end
Base.push!(a::Ast, child::Ast) = Base.push!(a.childs, deepcopy(child))

function human_typestring(a::Ast)
    res = string(a.name)
    if !isempty(a.childs)
        res *= "("
        res *= join(human_typestring.(a.childs), ", ")
        res *= ")"
    end
    return res
end

function human_typestring(a::Val{N}) where {N}
    res = string(N)    
    return res
end

function human_typestring(a::Val{N}, args...) where {N}
    res = string(N)
    if !isempty(args)
        res *= "("
        res *= join(human_typestring.(CName.(args)), ", ")
        res *= ")"
    end
    return res
end

Base.show(io::IO, a::Ast) = print(io, human_typestring(a))


function parse_typestring(s::AbstractString)
    s = strip(s)
    (isempty(s)||s[1]=='(') && error("typename parse error in $S")
    brace_pos = findfirst('(', s)
    if isnothing(brace_pos)
        return Ast(Symbol(s))
    end
    s[end] != ')' && error("typename parse error in $s")
    ast = Ast(Symbol(s[1:brace_pos-1]))
    inner = SubString(s, brace_pos + 1, length(s) - 1)
    cursor = 1
    elem_pos = 1
    opened_braces = 0    
    while true
        range = findnext(r"\(|\)|,", inner, cursor)
        
        if isnothing(range) || isempty(range)
            if elem_pos < length(inner) 
                push!(ast, parse_typestring(inner[elem_pos:end]))            
            end
            break
        end
        pos = first(range)                
        inner[pos] == '(' && (opened_braces += 1)
        inner[pos] == ')' && (opened_braces -= 1)
        if inner[pos] == ',' && opened_braces == 0
            push!(ast, parse_typestring(inner[elem_pos:pos-1]))            
            elem_pos = last(range) + 1
        end
        cursor = last(range) + 1        
    end
    return ast
end



deserialize(a::Symbol, args...) = deserialize(Val(a), args...)

deserialize(a::Ast) = deserialize(a.name, a.childs...)

deserialize(s::AbstractString) = deserialize(parse_typestring(s))

function checktype(t::Type)
    serialize(t)
    return t
end


typestring(t::Type{T}) where {T} = human_typestring(serialize(t))

serialize(::Type{T}) where {T} = throw(UnsupportedType(T))
deserialize(a::Val{N}, args...) where {N} = throw(UndefinedType(human_typestring(a, args...)))

macro _trivia_serializes(args...)
    funcs = Expr[]
    for arg in args
        push!(funcs, quote serialize(::Type{$arg}) = Ast(Symbol($arg)) end )
        push!(funcs, quote deserialize(::Val{Symbol($arg)}) = $arg end )
    end
    return esc(:($(funcs...),))
end
#(?'all'(?'name'\w+)(?:\((?'inner'(?:[^()]|(?1))*)\))?)

@_trivia_serializes(
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float16,
    Float32,
    Float64,
    Bool,
    Char,
    String,
    Date,
    DateTime,
    Time 
)