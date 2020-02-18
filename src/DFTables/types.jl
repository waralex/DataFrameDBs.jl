
struct DFTableMeta
    protocol_version ::Int16  #for possible future changes of serialization format
    rows ::Int64
    block_size ::Int64
    columns ::Vector{Pair{Symbol, Int32}}
    DFTableMeta() = new(0,0,0, [])
    DFTableMeta(columns ::Vector{Pair{Symbol, Int32}},
      block_size = DEFAULT_BLOCK_SIZE, rows = 0, protocol_version = PROTOCOL_VERSION) = new(protocol_version, rows, block_size, columns)    

    DFTableMeta(column_names ::AbstractVector{Symbol},
     block_size = DEFAULT_BLOCK_SIZE, rows = 0, protocol_version = PROTOCOL_VERSION) = 
     new(protocol_version, rows, block_size, map(p->p[2]=>p[1], enumerate(column_names)))                        
end

         

Base.:(==)(a::DFTableMeta, b::DFTableMeta) = a.protocol_version == b.protocol_version &&
                                            a.rows == b.rows &&
                                            a.block_size == b.block_size &&
                                            a.columns == b.columns

abstract type AbstractDFColumn{T} <: AbstractVector{T} end

struct DFColumn{T,VT} <: AbstractDFColumn{T}
    path::String
    DFColumn{T, VT}(path) where {T,VT} = new(path)
end
DFColumn(path::String, ::Type{T}) where {T} = DFColumn{T, Vector{T}}(path)

Base.show(io::IO, c::DFColumn) = print(io, c.path)

mutable struct DFTable
    path ::String
    meta ::DFTableMeta
    columns ::Vector{DFColumn}
    is_opened ::Bool
    DFTable(path::AbstractString) = new(path, DFTableMeta(), [], false)
    DFTable(path::AbstractString, meta::DFTableMeta) = new(path, meta, [], false)
end

function materialize(c::DFColumn{T,VT}; sizehint = 100000) where {T, VT}
    result = VT(undef, 0)
    open(c.path, "r") do f
        @time result = read_column_data(f, T)
    end
    return result    
end

function materialize(c::DFTable)
    return DataFrames.DataFrame(
        materialize.(c.columns; sizehint = c.meta.rows),
        map(p->p[1], c.meta.columns)
    )
end