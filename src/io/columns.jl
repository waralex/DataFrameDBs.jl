function write_columns(ios::Vector{<:IO}, data::Vector{<:AbstractVector}, block_size; close_on_done = true)
    @assert length(ios) == length(data) "ios and data have must equal sizes"
    total_rows = minimum(length.(data))
    @assert total_rows == maximum(length.(data)) "data columns must has equal lengths"

    streams = BlockStream.(ios)

    offset = 1
    while offset + block_size  <= total_rows
        r = offset:(offset + block_size - 1)
        prepare_block_write!.(streams, getindex.(data, Ref(r)))  
        commit_block_write!.(streams)        
        offset += block_size
    end
    if offset <= total_rows
        r = offset:total_rows
        prepare_block_write!.(streams, getindex.(data, Ref(r)))
        commit_block_write!.(streams)                
    end    
    close_on_done && close.(streams)
end

make_column_storage(::Type{T}) where {T} = Vector{T}(undef, 0)
make_column_storage(::Type{String}) = FlatStringsVector()