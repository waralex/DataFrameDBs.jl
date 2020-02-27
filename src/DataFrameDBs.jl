
module DataFrameDBs
using DataStructures
include("FlatStringsVectors.jl")
const DEFAULT_BLOCK_SIZE = 65536
const FORMAT_VERSION = 1

using .FlatStringsVectors
import DataFrames, Tables
import ProgressMeter
import Dates
export DFTable, DFTableMeta, ColumnTypes
include("columntypes.jl")
include("common.jl")
include("tables.jl")
include("io.jl")


end # module
