using DataFrames
import Tables, CSV



@testset "devel" begin

t = open_table("../dev_files/ecommerce")
turnon_progress!(t)
v = t[:, :]
#@time r = size(v[:price=>(p)->p > 100, :])

#@time r = size(v[:price=>(p)->p > 100, :])
@time r = size(t[:brand=>(p)->p == "apple", :])
#@time r = size(t[:brand=>(p)->p == "apple", :])
println(r)
end