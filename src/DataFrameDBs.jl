
module DataFrameDBs
using DataStructures
include("FlatStringsVectors.jl")
const DEFAULT_BLOCK_SIZE = 65536
const FORMAT_VERSION = 1

using .FlatStringsVectors
import DataFrames, Tables
import ProgressMeter
using Dates
export DFView, rows, create_table, insert, rename_column!, drop_column!, add_column!, drop_table!, truncate_table!,
         materialize, open_table, turnon_progress!, turnoff_progress!, head,
         map_to_column, table_stats
export ColumnTypes
include("columntypes.jl")
include("common.jl")
include("tables.jl")
include("io.jl")


end # module
