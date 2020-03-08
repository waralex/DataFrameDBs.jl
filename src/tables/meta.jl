
mutable struct ColumnMeta        
    id ::Int64
    name ::Symbol
    type ::Type        
    ColumnMeta(name::Union{String, Symbol}, type::Type) = new(0, Symbol(name), ColumnTypes.checktype(type))    
    ColumnMeta(id::Int,name::Union{String, Symbol}, type::Type) = new(id, Symbol(name), ColumnTypes.checktype(type))
    
    
end


struct DFTableMeta    
    columns ::Vector{ColumnMeta}
    block_size ::Int64
    format_version ::Int64  #for possible future changes of serialization format
    
    DFTableMeta(block_size = DEFAULT_BLOCK_SIZE) = new(Vector{ColumnMeta}(undef, 0), block_size, 0)
    DFTableMeta(columns ::Vector{ColumnMeta},
      block_size = DEFAULT_BLOCK_SIZE, format_version = FORMAT_VERSION) = new(columns, block_size, format_version)    

    function DFTableMeta(column_names ::Union{AbstractVector{Symbol}, AbstractVector{String}},
                        types ::AbstractVector{<:Type},
                        block_size = DEFAULT_BLOCK_SIZE, format_version = FORMAT_VERSION)
        length(column_names) != length(types) && error("lengths of names and types don't match")
        
        columns_meta = map(enumerate(zip(column_names, types))) do (id, (name, type)) 
            ColumnMeta(id, Symbol(name), type)
        end
        
        new(columns_meta, block_size, format_version)                        
    end
    

end

DFTableMeta(old::DFTableMeta, new_columns ::Vector{ColumnMeta}) = DFTableMeta(new_columns, old.block_size, old.format_version)

Base.:(==)(a::ColumnMeta, b::ColumnMeta) = a.id == b.id &&
                                           a.name == b.name &&
                                           a.type == b.type

Base.:(==)(a::DFTableMeta, b::DFTableMeta) = a.format_version == b.format_version &&                                            
                                            a.block_size == b.block_size &&
                                            a.columns == b.columns