function write_string(io::IO, s::AbstractString)
    Base.write(io, Int32(sizeof(s)))
    Base.write(io, s)
end
function read_string(io::IO)
    length = Base.read(io, Int32)
    return String(Base.read(io, length))    
end


write_symbol(io::IO, s::Symbol) = write_string(io, string(s))
read_symbol(io::IO) = Symbol(read_string(io))