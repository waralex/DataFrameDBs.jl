using Documenter
using DataFrameDBs

makedocs(
    sitename = "DataFrameDBs",
    format = Documenter.HTML(),
    modules = [DataFrameDBs]
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
#=deploydocs(
    repo = "<repository url>"
)=#
