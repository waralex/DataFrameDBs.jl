using Printf
struct SizeStats
    rows::Int64
    compressed::Int64
    uncompressed::Int64 
    SizeStats() = new(0,0,0)   
    SizeStats(rows, compressed, uncompressed) = new(rows, compressed, uncompressed)    
end

Base.:(+)(a::SizeStats, b::SizeStats) = SizeStats(a.rows+b.rows, a.compressed + b.compressed, a.uncompressed + b.uncompressed)

function totalstats(stats)
    isempty(stats) && return SizeStats()
    return SizeStats(first(stats).rows,
     sum(getproperty.(stats, :compressed)), 
     sum(getproperty.(stats, :uncompressed)), 
     )
end

function show_humansize(io, size::Number)
    for unit in ["bytes", "KB", "MB", "GB"]
        if abs(size) < 1024
            print(io, round(size, digits = 2), " ", unit)
            return
        end 
        size /= 1024
    end
    print(io, size, "TB")
end

function show_humanrows(io, rows::Number)
    for unit in ["Rows", "KRows", "MRows"]
        if abs(rows) < 1000
            print(io, round(rows, digits = 2), " ", unit)
            return
        end 
        rows /= 1000
    end
    print(io, rows, "BRows")
end

function humansize(size::Number) 
    io = IOBuffer()
    show_humansize(io, size)
    return String(take!(io))
end
function humanrows(rows::Number) 
    io = IOBuffer()
    show_humanrows(io, rows)
    return String(take!(io))
end

compression_ratio(s::SizeStats) = round(s.uncompressed / s.compressed, digits = 2) 

function pretty_stats(s::SizeStats)
    (
        rows = humanrows(s.rows),
        uncompressed = humansize(s.uncompressed),
        compressed = humansize(s.compressed),
        compression_ratio = compression_ratio(s) 
    )
end


Base.show(io::IO, stats::SizeStats) = Base.show(io, pretty_stats(stats))
    
