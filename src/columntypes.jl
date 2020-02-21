module ColumnTypes
using Dates
struct CName{T} end

CName(s::Symbol) = CName{s}()

struct UndefinedType <: Exception
    type_string ::String
end

struct UnsupportedType <: Exception
    type ::Type
end

show(io, e::UndefinedType) = print(io, "Undefined column type: ", e.type_string)
show(io, e::UnsupportedType) = print(io, "Unsupported type: ", e.type)

include("columntypes/base.jl")
include("columntypes/complex.jl")

end