using DataFrameDBs: DFTable, create_table, 
selection, projection,
DFView, proj_elem, required_columns, BlocksIterator, materialize, names, selproj, DFColumn
using DataFrames
using InteractiveUtils
@testset "column broadcast" begin
end