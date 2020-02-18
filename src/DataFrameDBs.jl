
module DataFrameDBs
include("FlatStringsVectors.jl")
const DEFAULT_BLOCK_SIZE = 65536
const PROTOCOL_VERSION = 1

using .FlatStringsVectors
import DataFrames, Tables
import Dates
export DFTable, DFTableMeta
include("DFTables.jl")
include("DFIO.jl")


end # module
