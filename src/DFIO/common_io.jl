@inline function write_varint(io::IO, v::UInt64)
    for i = 0:8
        byte::UInt8 = v & 0x7F
        if v > 0x7F
            byte |= 0x80
        end
        Base.write(io, byte)
        v >>= 7
        if v == 0
            return    
        end
    end
end

@inline write_varint(io::IO, v::Unsigned) = write_varint(io, UInt64(v))
@inline write_varint(op::IO, v::Bool) = write_varint(io, UInt64(v))

@inline function write_varint(io::IO, v::Int64) 
    @assert v >= 0 "value is positive"
    write_varint(io, reinterpret(UInt64, v))
end

@inline function write_varint(io::IO, v::Signed)   
    @assert v >= 0 "value is positive"
    write_varint(io, UInt64(v))
end



@inline function read_varint(io::IO, ::Type{UInt64})
    value::UInt64 = 0
    for i = 0:8
        byte::UInt8 = Base.read(io, UInt8)
        value |= (convert(UInt64, byte) & 0x7F) << (7 * i)
        if byte & 0x80 == 0 
            break
        end
    end
    return value
end

@inline read_varint(io::IO, ::Type{T}) where {T<:Integer} = T(read_varint(io, UInt64))

function write_string(io::IO, s::AbstractString)
    Base.write(io, Int16(sizeof(s)))
    Base.write(io, s)
end
function read_string(io::IO)
    length = Base.read(io, Int16)
    return String(Base.read(io, length))    
end
function read_string_f(io::IO)
    length = Base.read(io, Int16)
    Base.read(io, length)
    return ""
end