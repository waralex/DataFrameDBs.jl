using Printf: @sprintf


function read_progress_channel(out::IO = stderr;spawn = true, update = 0.1)  
  
    function progress_func(ch)
        progress = ProgressMeter.ProgressUnknown("Processing data rows")
        total_rows = 0
        start = time_ns()
        last_show = time()
        while (rows = take!(ch)) !== nothing                        
            total_rows += rows            
            if time() - last_show > update
                duration = time_ns() - start
                row_per_sec = total_rows * 1e9 / duration
                prow_per_sec = humanrows(row_per_sec) * "/sec"

                printover_head(out)
                    print(out, "Time: ")
                    printstyled(out, durationstring(duration / 1e9);bold = true)
                    print(out, " read: ")
                    printstyled(out, humanrows(total_rows);bold = true)
                    print(out, " (")
                    printstyled(out, prow_per_sec;bold = true)
                    print(out, ")")
                printover_tail(out)
                last_show = time()
            end
        end

        duration = time_ns() - start
        row_per_sec = total_rows * 1e9 / duration
        prow_per_sec = humanrows(row_per_sec) * "/sec"
        printover_head(out)
            print(out, "Time: ")
            printstyled(out, durationstring(duration / 1e9);bold = true)
            print(out, " read: ")
            printstyled(out, humanrows(total_rows);bold = true)
            print(out, " (")
            printstyled(out, prow_per_sec;bold = true)
            print(out, ")")
        printover_tail(out)
        print(out, "\n")
        put!(ch, 0)
    end


    chnl = Channel{Any}(0)
    task = Task(() -> progress_func(chnl))
    task.sticky = !spawn
    bind(chnl, task)
    if spawn
        schedule(task) # start it on (potentially) another thread
    else
        yield(task) # immediately start it, yielding the current thread
    end    
    return chnl
end

function write_progress_channel(cols, out::IO = stderr;spawn = true, update = 0.05)  
    function progress_func(ch)
        progress = ProgressMeter.ProgressUnknown("Processing data rows")
        total_rows = 0
        start = time()
        last_show = time()
        writed_sizes = SizeStats[SizeStats() for i in  1:cols]
        while (szs = take!(ch)) !== nothing                        
            writed_sizes .+= szs
            
            if time() - last_show > update
                totals = totalstats(values(writed_sizes))                    
                pretty = pretty_stats(totals)
                
                duration = time() - start
                row_per_sec = totals.rows / duration
                prow_per_sec = humanrows(row_per_sec) * "/sec"

                printover_head(out)
                    print(out, "Time: ")
                    printstyled(out, durationstring(duration);bold = true)
                    print(out, " written: ")
                    printstyled(out, pretty.rows;bold = true)
                    print(out, " (")
                    printstyled(out, prow_per_sec;bold = true)
                    print(out, ")")
                    print(out, ", uncompressed size: ")
                    printstyled(out, pretty.uncompressed;bold = true)
                    print(out, ", compressed size: ")
                    printstyled(out, pretty.compressed;bold = true)
                    print(out, ", compression ratio: ")
                    printstyled(out, pretty.compression_ratio;bold = true)
                printover_tail(out)
                last_show = time()
            end
        end

        totals = totalstats(values(writed_sizes))                    
        pretty = pretty_stats(totals)
        
        duration = time() - start
        row_per_sec = totals.rows / duration
        prow_per_sec = humanrows(row_per_sec) * "/sec"

        printover_head(out)
            print(out, "Time: ")
            printstyled(out, durationstring(duration);bold = true)
            print(out, " written: ")
            printstyled(out, pretty.rows;bold = true)
            print(out, " (")
            printstyled(out, prow_per_sec;bold = true)
            print(out, ")")
            print(out, ", uncompressed size: ")
            printstyled(out, pretty.uncompressed;bold = true)
            print(out, ", compressed size: ")
            printstyled(out, pretty.compressed;bold = true)
            print(out, ", compression ratio: ")
            printstyled(out, pretty.compression_ratio;bold = true)
        printover_tail(out)
        print(out, "\n")
        put!(ch, 0)
    end

    chnl = Channel{Any}(0)
    task = Task(() -> progress_func(chnl))
    task.sticky = !spawn
    bind(chnl, task)
    if spawn
        schedule(task) # start it on (potentially) another thread
    else
        yield(task) # immediately start it, yielding the current thread
    end    
    return chnl
end

function durationstring(nsec)
    
    days = div(nsec, 60*60*24)
    r = nsec - 60*60*24*days
    hours = div(r,60*60)
    r = r - 60*60*hours
    minutes = div(r, 60)
    seconds = r - 60*minutes
    

    return @sprintf "%u:%02u:%02.4f" hours minutes seconds
end

function move_cursor_up_while_clearing_lines(io, numlinesup)
    for _ in 1:numlinesup
        print(io, "\r\u1b[K\u1b[A")
    end
end

function printover_head(io::IO)
    print(io, "\r")
end
function printover_tail(io::IO)   
    if isdefined(Main, :IJulia)
        Main.IJulia.stdio_bytes[] = 0 
    elseif isdefined(Main, :ESS) || isdefined(Main, :Atom)
    else
        print(io, "\u1b[K")
    end
end