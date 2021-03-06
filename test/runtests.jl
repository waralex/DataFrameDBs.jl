using Test
using DataFrameDBs
import DataFrames
using Dates
include("column_types.jl")
include("flat_strings.jl")
include("block_streams.jl")
include("tables.jl")
include("table_io.jl")
#include("column_io.jl")
include("missings.jl")
include("create_from_data.jl")

include("broadcast.jl")
include("selection.jl")
include("projection.jl")
include("view.jl")
include("column.jl")
include("rows.jl")
include("table_changes.jl")
include("columnbroadcast.jl")
#include("aggregate.jl")
#include("devel.jl")