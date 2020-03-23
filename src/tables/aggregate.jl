function groupreduce(view::DFView, by::Tuple{Vararg{Symbol}}; cols...)
    group_view = view[:, (__by = by=>(a...)->tuple(a...),)]
    result_names = Vector{Symbol}(undef, 0)
    reducers = Vector{OnlineStat}(undef, 0)
    

    group_map = RobinDict{eltype(group_view[:,:__by]), Int64}()
    
    for col in cols        
        push!(result_names, col[1])
        view_column = view[:, col[2][1]]
        add_column!(group_view, col[1], view_column)        
        push!(reducers, col[2][2])
    end
    
    
    group_positions = Vector{Int64}(undef, 0)
    
    for block in BlocksIterator(group_view)
        by_vector = block.__by
        resize!(group_positions, length(by_vector))
        for i in eachindex(by_vector)
            elem = by_vector[i]            
            if haskey(group_map, elem)
                group_positions[i] = group_map[elem]            
            else                
                group_map[elem] = length(group_map)  + 1
                group_positions[i] = length(group_map)
            end            
        end

    end

    println(group_map)
    
    
end