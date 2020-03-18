# DataFrameDBs.jl

The DateFrameDBs is the persistent, space efficient columnar database, inspired by [DataFrames](https://github.com/JuliaData/DataFrames.jl) and columnar databases like [ClickHouse](https://github.com/ClickHouse/ClickHouse). 

The DateFrameDBs  allows you to work with a large amount of data that does not fit in memory

It's experimental package, so, please report bugs by [opening an issue] (https://github.com/waralex/DataFrameDBs.jl/issues/new).



## Installation

Julia >=1.2 required

The DataFrameDBs is not yet part of Julia package system, you can install it directly from github:
```julia
import Pkg; Pkg.add(Pkg.PackageSpec(url = "https://github.com/waralex/DataFrameDBs.jl.git"))
```

## Introduction

The DataFramesDBs is a columnar database. It stores each data column as a separate file in a table folder. When writing to disk, each column is divided into blocks (the default block is 65536 elements), and each block is compressed using lz4 compression.

Currently DataFramesDB can store arrays for which eltype is one of:
 - any subtype of Numbers
 - Date, DateTime, Time
 - Tuple with elements of types above
 - String
 - Union{Missing, T} where T is one types above

You can also store any custom `isbitstype` type with little efforts

In the future, I plan to get support of arrays, nested arrays and categorial arrays
 
If you want data only from the specific columns with some condition mathcing, when only that data will be allocated, not entire table. For example if you want see the product name and price for sales in a particular category, then only that data will be materialized. 

## Get Started

### Create and fill test table

Lets create empty table:
```julia
julia> using DataFrameDBs
julia> t = empty_table("test_table")
```
Thats create dir `test_table` in you current directory and write meta of table to it

Lets add some columns to our table:

```julia
julia> size = 3000000
julia> add_column!(t, :id, 1:size, show_progress = true)
Time: 0:00:00 written: 3.0 MRows (16.27 MRows/sec), uncompressed size: 22.89 MB, compressed size: 11.45 MB, compression ratio: 2.0

julia> add_column!(t, :code, rand(1:1000, size), show_progress = true)
Time: 0:00:00 writerd: 3.0 MRows (49.7 MRows/sec), uncompressed size: 22.89 MB, compressed size: 8.97 MB, compression ratio: 2.55

julia> brands = ["apple", "samsung", "huawai", "microsoft", "dell", "xbox", "sony", "intel"]
julia> add_column!(t, :brand, rand(brands, size), show_progress = true)
Time: 0:00:00 writerd: 3.0 MRows (19.91 MRows/sec), uncompressed size: 27.18 MB, compressed size: 9.54 MB, compression ratio: 2.85

julia> add_column!(t, :price, rand(1.:0.1:2000., size), show_progress = true)
Time: 0:00:00 writerd: 3.0 MRows (20.34 MRows/sec), uncompressed size: 22.89 MB, compressed size: 11.85 MB, compression ratio: 1.93

julia> table_stats(t)
5×6 DataFrames.DataFrame
│ Row │ column      │ type    │ rows      │ uncompressed size │ compressed size │ compression ratio │
│     │ Symbol      │ String  │ String    │ String            │ String          │ Float64           │
├─────┼─────────────┼─────────┼───────────┼───────────────────┼─────────────────┼───────────────────┤
│ 1   │ id          │ Int64   │ 3.0 MRows │ 22.89 MB          │ 11.45 MB        │ 2.0               │
│ 2   │ code        │ Int64   │ 3.0 MRows │ 22.89 MB          │ 8.97 MB         │ 2.55              │
│ 3   │ brand       │ String  │ 3.0 MRows │ 27.18 MB          │ 9.54 MB         │ 2.85              │
│ 4   │ price       │ Float64 │ 3.0 MRows │ 22.89 MB          │ 11.85 MB        │ 1.93              │
│ 5   │ Table total │         │ 3.0 MRows │ 95.84 MB          │ 41.82 MB        │ 2.29              │
```
Now we have the table with 4 columns and 3 million row. it takes 42 MB of disk space and will allocate 95 MB of memory with full materialization.
This table is stored on disc, so if you close REPL you can reopen it with
```julia
julia> t = open_table("test_table/")
DFTable path: test_table/
5×6 DataFrames.DataFrame
│ Row │ column      │ type    │ rows      │ uncompressed size │ compressed size │ compression ratio │
│     │ Symbol      │ String  │ String    │ String            │ String          │ Float64           │
├─────┼─────────────┼─────────┼───────────┼───────────────────┼─────────────────┼───────────────────┤
│ 1   │ id          │ Int64   │ 3.0 MRows │ 22.89 MB          │ 11.45 MB        │ 2.0               │
│ 2   │ code        │ Int64   │ 3.0 MRows │ 22.89 MB          │ 8.97 MB         │ 2.55              │
│ 3   │ brand       │ String  │ 3.0 MRows │ 27.18 MB          │ 9.54 MB         │ 2.85              │
│ 4   │ price       │ Float64 │ 3.0 MRows │ 22.89 MB          │ 11.85 MB        │ 1.93              │
│ 5   │ Table total │         │ 3.0 MRows │ 95.84 MB          │ 41.82 MB        │ 2.29              │
```

You can materialize entire DFTable to DataFrame:
```julia
julia> materialize(t)
3000000×4 DataFrames.DataFrame
```
or can see the head of the table:
```julia
julia> head(t)
10×4 DataFrames.DataFrame
│ Row │ id    │ code  │ brand     │ price   │
│     │ Int64 │ Int64 │ String    │ Float64 │
├─────┼───────┼───────┼───────────┼─────────┤
│ 1   │ 1     │ 434   │ sony      │ 413.2   │
│ 2   │ 2     │ 384   │ xbox      │ 1533.5  │
│ 3   │ 3     │ 928   │ huawai    │ 1988.1  │
│ 4   │ 4     │ 644   │ xbox      │ 1566.4  │
│ 5   │ 5     │ 794   │ apple     │ 194.4   │
│ 6   │ 6     │ 330   │ huawai    │ 619.1   │
│ 7   │ 7     │ 248   │ samsung   │ 781.0   │
│ 8   │ 8     │ 766   │ samsung   │ 842.2   │
│ 9   │ 9     │ 424   │ xbox      │ 48.2    │
│ 10  │ 10    │ 783   │ microsoft │ 628.1   │
```

### Selections on a table

The main advantage of DataFramesDB is that it only materializes the data that you need and when you need it
```julia
julia> view = t[:,[:brand,:price]]
View of table test_table/
Projection: brand=>col(brand)::String; price=>col(price)::Float64
Selection:
```
`view` is lazy view of the table with 2 columns :brand and :price. Its only hold information about source table, projection and selection rules, but not data of table. You can materialize it with `materialize(view)` or use it in future selections
```julia
julia> view2 = view[1:10:end, :]
View of table dev_files/test_table/
Projection: brand=>col(brand)::String; price=>col(price)::Float64
Selection: 1:10:2999991

julia> view = view[1:5:end, :]
View of table dev_files/test_table/
Projection: id=>col(id)::Int64; code=>col(code)::Int64; brand=>col(brand)::String; price=>col(price)::Float64
Selection: 1:50:2999951

julia> view3 = view2[1:10, :]
View of table dev_files/test_table/
Projection: brand=>col(brand)::String; price=>col(price)::Float64
Selection: 1:10:91

julia> view4 = view3[[1,4,5], :]
View of table dev_files/test_table/
Projection: brand=>col(brand)::String; price=>col(price)::Float64
Selection: [1, 31, 41]
```

Now materialization of view4 need only rows 1, 31 and 41, which  is  rows 1, 4 and 5 form every 5 out of every ten rows of origin table

```julia
julia> materialize(view4)
3×2 DataFrames.DataFrame
│ Row │ brand  │ price   │
│     │ String │ Float64 │
├─────┼────────┼─────────┤
│ 1   │ sony   │ 413.2   │
│ 2   │ huawai │ 1321.7  │
│ 3   │ huawai │ 188.8   │
```

Single column represented by type `DFColumn{T}` where `T` is the element type. The `DFColumn` is not a AbstractVector, but it support iterations and `getindex`. You can get a column from view with `view.<column_name>` or `view[:, :<column_name>]`
```julia
julia> column = view.brand
DataFrameDBs.DFColumn{String}

julia> view[:,:id]
DataFrameDBs.DFColumn{Int64}
```
The DFColumn is lazy too. It can be materialized to a vector with `materialize`, or it can be used with a function that supports iterators as arguments.
```julia
julia> unique(column)
8-element Array{Any,1}:
 "sony"
 "xbox"
 "huawai"
 "apple"
 "samsung"
 "microsoft"
 "dell"
 "intel"
```
In this example column is not fully materialized. Its materialize one block (65536 rows of source table) at time and send it to `unique`.

DFColumn can be broadcated. Broadcast of DFColumns and, if any, scalar vars is DFColumn too:
```julia
julia> t.price .* 10
DataFrameDBs.DFColumn{Float64}

julia> t.code .> t.id
DataFrameDBs.DFColumn{Bool}
```
At iterating, broadcast of DFColums read only required columns from disc, one block at time, and run broadcast function at that block. So iteration don't require full allocation of columns

`DFColumn{Bool}` can be used as a row index in view.
```julia
julia> view = t[t.brand.=="sony", :]
View of table test_table/
Projection: id=>col(id)::Int64; code=>col(code)::Int64; brand=>col(brand)::String; price=>col(price)::Float64
Selection: ==(col(brand)::String, Base.RefValue{String}("sony"))::Bool

julia> head(view)
10×4 DataFrames.DataFrame
│ Row │ id    │ code  │ brand  │ price   │
│     │ Int64 │ Int64 │ String │ Float64 │
├─────┼───────┼───────┼────────┼─────────┤
│ 1   │ 1     │ 434   │ sony   │ 413.2   │
│ 2   │ 11    │ 523   │ sony   │ 1643.4  │
│ 3   │ 12    │ 753   │ sony   │ 785.1   │
│ 4   │ 14    │ 408   │ sony   │ 1971.9  │
│ 5   │ 21    │ 534   │ sony   │ 914.1   │
│ 6   │ 24    │ 500   │ sony   │ 307.6   │
│ 7   │ 46    │ 109   │ sony   │ 1537.2  │
│ 8   │ 49    │ 621   │ sony   │ 761.0   │
│ 9   │ 54    │ 689   │ sony   │ 1616.3  │
│ 10  │ 55    │ 738   │ sony   │ 410.6   │
```
You can construct new view from DFColumns, that have a similar selection:
```julia
julia> view = t[1:100:end, :]
View of table test_table/
Projection: id=>col(id)::Int64; code=>col(code)::Int64; brand=>col(brand)::String; price=>col(price)::Float64
Selection: 1:100:2999901

julia> new_view = DFView(id = view.id, double_price = view.price.*2, id_plus_code = view.id.+view.code)
View of table test_table/
Projection: id=>col(id)::Int64; double_price=>*(col(price)::Float64, 2)::Float64; id_plus_code=>+(col(id)::Int64, col(code)::Int64)::Int64
Selection: 1:100:2999901

julia> head(new_view)
10×3 DataFrames.DataFrame
│ Row │ id    │ double_price │ id_plus_code │
│     │ Int64 │ Float64      │ Int64        │
├─────┼───────┼──────────────┼──────────────┤
│ 1   │ 1     │ 826.4        │ 435          │
│ 2   │ 101   │ 2546.0       │ 408          │
│ 3   │ 201   │ 3624.0       │ 469          │
│ 4   │ 301   │ 3783.4       │ 760          │
│ 5   │ 401   │ 3851.4       │ 417          │
│ 6   │ 501   │ 2455.0       │ 1030         │
│ 7   │ 601   │ 1092.6       │ 1120         │
│ 8   │ 701   │ 1737.4       │ 978          │
│ 9   │ 801   │ 567.6        │ 1088         │
│ 10  │ 901   │ 2537.8       │ 1429         │
```
To drop test table use `drop_table!(t)` or just remove dir `test_table/` from disc

## Real Data Example

### Import data

I use [this](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset as example.
Before start, please, download and unzip it (registraion on kaggle is required). This dataset contains 100 millions rows and take 14GB in csv format.
Let's create the DataFrameDBs table from the first CSV file, it will take several minutes:
```julia
julia> using DataFrameDBs
julia> using CSV
julia> t = create_table("ecommerce", from = CSV.Rows("ecommerce-behavior-data-from-multi-category-store/2019-Oct.csv", reuse_row=true), show_progress=true)
Time: 0:03:24 writerd: 42.45 MRows (207.12 KRows/sec), uncompressed size: 6.35 GB, compressed size: 2.15 GB, compression ratio: 2.95
DFTable path: ecommerce
10×6 DataFrames.DataFrame
│ Row │ column        │ type                   │ rows        │ uncompressed size │ compressed size │ compression ratio │
│     │ Symbol        │ String                 │ String      │ String            │ String          │ Float64           │
├─────┼───────────────┼────────────────────────┼─────────────┼───────────────────┼─────────────────┼───────────────────┤
│ 1   │ event_time    │ Union{Missing, String} │ 42.45 MRows │ 1.07 GB           │ 28.82 MB        │ 37.93             │
│ 2   │ event_type    │ Union{Missing, String} │ 42.45 MRows │ 326.69 MB         │ 14.99 MB        │ 21.8              │
│ 3   │ product_id    │ Union{Missing, String} │ 42.45 MRows │ 461.02 MB         │ 240.1 MB        │ 1.92              │
│ 4   │ category_id   │ Union{Missing, String} │ 42.45 MRows │ 931.1 MB          │ 161.99 MB       │ 5.75              │
│ 5   │ category_code │ Union{Missing, String} │ 42.45 MRows │ 782.07 MB         │ 179.64 MB       │ 4.35              │
│ 6   │ brand         │ Union{Missing, String} │ 42.45 MRows │ 367.39 MB         │ 159.58 MB       │ 2.3               │
│ 7   │ price         │ Union{Missing, String} │ 42.45 MRows │ 392.19 MB         │ 208.9 MB        │ 1.88              │
│ 8   │ user_id       │ Union{Missing, String} │ 42.45 MRows │ 526.27 MB         │ 216.89 MB       │ 2.43              │
│ 9   │ user_session  │ Union{Missing, String} │ 42.45 MRows │ 1.58 GB           │ 995.78 MB       │ 1.63              │
│ 10  │ Table total   │                        │ 42.45 MRows │ 6.35 GB           │ 2.15 GB         │ 2.95              │
```
I use the CSV.Rows as csv parser because it don't load entire csv to memory. The disadvantage of this approach is that the CSV.Rows does not determine column types - all columns are imported as Union{String, Missing}. You can use CSV.File for smaller datasets.
Let's append second file of the dataset to the table:
```julia
julia> insert(t, CSV.Rows("ecommerce-behavior-data-from-multi-category-store/2019-Nov.csv", reuse_row=true), show_progress=true)
Time: 0:05:35 written: 67.55 MRows (201.38 KRows/sec), uncompressed size: 10.09 GB, compressed size: 3.77 GB, compression ratio: 2.68
DFTable path: ecommerce
10×6 DataFrames.DataFrame
│ Row │ column        │ type                   │ rows         │ uncompressed size │ compressed size │ compression ratio │
│     │ Symbol        │ String                 │ String       │ String            │ String          │ Float64           │
├─────┼───────────────┼────────────────────────┼──────────────┼───────────────────┼─────────────────┼───────────────────┤
│ 1   │ event_time    │ Union{Missing, String} │ 109.95 MRows │ 2.76 GB           │ 59.22 MB        │ 47.81             │
│ 2   │ event_type    │ Union{Missing, String} │ 109.95 MRows │ 845.2 MB          │ 43.02 MB        │ 19.65             │
│ 3   │ product_id    │ Union{Missing, String} │ 109.95 MRows │ 1.17 GB           │ 630.31 MB       │ 1.9               │
│ 4   │ category_id   │ Union{Missing, String} │ 109.95 MRows │ 2.36 GB           │ 425.3 MB        │ 5.67              │
│ 5   │ category_code │ Union{Missing, String} │ 109.95 MRows │ 1.97 GB           │ 470.16 MB       │ 4.28              │
│ 6   │ brand         │ Union{Missing, String} │ 109.95 MRows │ 956.34 MB         │ 418.92 MB       │ 2.28              │
│ 7   │ price         │ Union{Missing, String} │ 109.95 MRows │ 1015.72 MB        │ 542.99 MB       │ 1.87              │
│ 8   │ user_id       │ Union{Missing, String} │ 109.95 MRows │ 1.33 GB           │ 614.19 MB       │ 2.22              │
│ 9   │ user_session  │ Union{Missing, String} │ 109.95 MRows │ 4.1 GB            │ 2.79 GB         │ 1.47              │
│ 10  │ Table total   │                        │ 109.95 MRows │ 16.43 GB          │ 5.92 GB         │ 2.78              │
```
For now we have table, that takes 6 GB on disc (compare with 14GB of origin csv). All colums have type Union{Missing, String}. 
Let's see to our data:
```julia
julia> head(t)
10×9 DataFrames.DataFrame
│ Row │ event_time              │ event_type │ product_id │ category_id         │ category_code                       │ brand    │ price   │ user_id   │ user_session                         │
│     │ Union{Missing, String}  │ String⍰    │ String⍰    │ String⍰             │ Union{Missing, String}              │ String⍰  │ String⍰ │ String⍰   │ Union{Missing, String}               │
├─────┼─────────────────────────┼────────────┼────────────┼─────────────────────┼─────────────────────────────────────┼──────────┼─────────┼───────────┼──────────────────────────────────────┤
│ 1   │ 2019-10-01 00:00:00 UTC │ view       │ 44600062   │ 2103807459595387724 │ missing                             │ shiseido │ 35.79   │ 541312140 │ 72d76fde-8bb3-4e00-8c23-a032dfed738c │
│ 2   │ 2019-10-01 00:00:00 UTC │ view       │ 3900821    │ 2053013552326770905 │ appliances.environment.water_heater │ aqua     │ 33.20   │ 554748717 │ 9333dfbd-b87a-4708-9857-6336556b0fcc │
│ 3   │ 2019-10-01 00:00:01 UTC │ view       │ 17200506   │ 2053013559792632471 │ furniture.living_room.sofa          │ missing  │ 543.10  │ 519107250 │ 566511c2-e2e3-422b-b695-cf8e6e792ca8 │
│ 4   │ 2019-10-01 00:00:01 UTC │ view       │ 1307067    │ 2053013558920217191 │ computers.notebook                  │ lenovo   │ 251.74  │ 550050854 │ 7c90fc70-0e80-4590-96f3-13c02c18c713 │
│ 5   │ 2019-10-01 00:00:04 UTC │ view       │ 1004237    │ 2053013555631882655 │ electronics.smartphone              │ apple    │ 1081.98 │ 535871217 │ c6bd7419-2748-4c56-95b4-8cec9ff8b80d │
│ 6   │ 2019-10-01 00:00:05 UTC │ view       │ 1480613    │ 2053013561092866779 │ computers.desktop                   │ pulser   │ 908.62  │ 512742880 │ 0d0d91c2-c9c2-4e81-90a5-86594dec0db9 │
│ 7   │ 2019-10-01 00:00:08 UTC │ view       │ 17300353   │ 2053013553853497655 │ missing                             │ creed    │ 380.96  │ 555447699 │ 4fe811e9-91de-46da-90c3-bbd87ed3a65d │
│ 8   │ 2019-10-01 00:00:08 UTC │ view       │ 31500053   │ 2053013558031024687 │ missing                             │ luminarc │ 41.16   │ 550978835 │ 6280d577-25c8-4147-99a7-abc6048498d6 │
│ 9   │ 2019-10-01 00:00:10 UTC │ view       │ 28719074   │ 2053013565480109009 │ apparel.shoes.keds                  │ baden    │ 102.71  │ 520571932 │ ac1cd4e5-a3ce-4224-a2d7-ff660a105880 │
│ 10  │ 2019-10-01 00:00:11 UTC │ view       │ 1004545    │ 2053013555631882655 │ electronics.smartphone              │ huawei   │ 566.01  │ 537918940 │ 406c46ed-90a4-4787-a43b-59a410c1a5fb │
```

### Prepare data

Before transforming the data, enable the display of query progress for the table
```julia
julia> turnon_progress!(t)
```
You can turn off it later with `turnoff_progress!(t)`

Let's convert numeric columns to a numeric type using the category_id column example. First check is where missings in category_id
```julia
julia> sum(ismissing.(t.category_id))
Time: 0:00:07 read: 109.95 MRows (14.4 MRows/sec)
0
```
There are 0 missings in column. 

Let's convert category_id to Int64 column.

First rename it:
```julia
julia> rename_column!(t, :category_id, :category_id_raw)
```
Create DFColumn:
```julia
c_id = parse.(Int64, t.category_id_raw)
DataFrameDBs.DFColumn{Int64}

materialize(c_id[1:10])
10-element Array{Int64,1}:
 2103807459595387724
 2053013552326770905
 2053013559792632471
 2053013558920217191
 2053013555631882655
 2053013561092866779
 2053013553853497655
 2053013558031024687
 2053013565480109009
```
Add new column before :category_column :
```julia
julia> add_column!(t, :category_id, c_id, before=:category_code)
Time: 0:00:14 read: 109.95 MRows (7.81 MRows/sec)

julia> head(t)
Time: 0:00:00 read: 65.54 KRows (260.05 MRows/sec)
10×10 DataFrames.DataFrame
│ Row │ event_time              │ event_type │ product_id │ category_id_raw     │ category_id         │ category_code                       │ brand    │ price   │ user_id   │ user_session                         │
│     │ Union{Missing, String}  │ String⍰    │ String⍰    │ String⍰             │ Int64               │ Union{Missing, String}              │ String⍰  │ String⍰ │ String⍰   │ Union{Missing, String}               │
├─────┼─────────────────────────┼────────────┼────────────┼─────────────────────┼─────────────────────┼─────────────────────────────────────┼──────────┼─────────┼───────────┼──────────────────────────────────────┤
│ 1   │ 2019-10-01 00:00:00 UTC │ view       │ 44600062   │ 2103807459595387724 │ 2103807459595387724 │ missing                             │ shiseido │ 35.79   │ 541312140 │ 72d76fde-8bb3-4e00-8c23-a032dfed738c │
│ 2   │ 2019-10-01 00:00:00 UTC │ view       │ 3900821    │ 2053013552326770905 │ 2053013552326770905 │ appliances.environment.water_heater │ aqua     │ 33.20   │ 554748717 │ 9333dfbd-b87a-4708-9857-6336556b0fcc │
│ 3   │ 2019-10-01 00:00:01 UTC │ view       │ 17200506   │ 2053013559792632471 │ 2053013559792632471 │ furniture.living_room.sofa          │ missing  │ 543.10  │ 519107250 │ 566511c2-e2e3-422b-b695-cf8e6e792ca8 │
│ 4   │ 2019-10-01 00:00:01 UTC │ view       │ 1307067    │ 2053013558920217191 │ 2053013558920217191 │ computers.notebook                  │ lenovo   │ 251.74  │ 550050854 │ 7c90fc70-0e80-4590-96f3-13c02c18c713 │
│ 5   │ 2019-10-01 00:00:04 UTC │ view       │ 1004237    │ 2053013555631882655 │ 2053013555631882655 │ electronics.smartphone              │ apple    │ 1081.98 │ 535871217 │ c6bd7419-2748-4c56-95b4-8cec9ff8b80d │
│ 6   │ 2019-10-01 00:00:05 UTC │ view       │ 1480613    │ 2053013561092866779 │ 2053013561092866779 │ computers.desktop                   │ pulser   │ 908.62  │ 512742880 │ 0d0d91c2-c9c2-4e81-90a5-86594dec0db9 │
│ 7   │ 2019-10-01 00:00:08 UTC │ view       │ 17300353   │ 2053013553853497655 │ 2053013553853497655 │ missing                             │ creed    │ 380.96  │ 555447699 │ 4fe811e9-91de-46da-90c3-bbd87ed3a65d │
│ 8   │ 2019-10-01 00:00:08 UTC │ view       │ 31500053   │ 2053013558031024687 │ 2053013558031024687 │ missing                             │ luminarc │ 41.16   │ 550978835 │ 6280d577-25c8-4147-99a7-abc6048498d6 │
│ 9   │ 2019-10-01 00:00:10 UTC │ view       │ 28719074   │ 2053013565480109009 │ 2053013565480109009 │ apparel.shoes.keds                  │ baden    │ 102.71  │ 520571932 │ ac1cd4e5-a3ce-4224-a2d7-ff660a105880 │
│ 10  │ 2019-10-01 00:00:11 UTC │ view       │ 1004545    │ 2053013555631882655 │ 2053013555631882655 │ electronics.smartphone              │ huawei   │ 566.01  │ 537918940 │ 406c46ed-90a4-4787-a43b-59a410c1a5fb │
```
As before the column is not fully allocated. It reads one by one blocks from broadcast `parse.(Int64, t.category_id_raw)` and writes it to disc

Finaly remove the old column
```julia
julia> drop_column!(t, :category_id_raw)
DFTable path: ecommerce
10×6 DataFrames.DataFrame
│ Row │ column        │ type                   │ rows         │ uncompressed size │ compressed size │ compression ratio │
│     │ Symbol        │ String                 │ String       │ String            │ String          │ Float64           │
├─────┼───────────────┼────────────────────────┼──────────────┼───────────────────┼─────────────────┼───────────────────┤
│ 1   │ event_time    │ Union{Missing, String} │ 109.95 MRows │ 2.76 GB           │ 59.22 MB        │ 47.81             │
│ 2   │ event_type    │ Union{Missing, String} │ 109.95 MRows │ 845.2 MB          │ 43.02 MB        │ 19.65             │
│ 3   │ product_id    │ Union{Missing, String} │ 109.95 MRows │ 1.17 GB           │ 630.31 MB       │ 1.9               │
│ 4   │ category_id   │ Int64                  │ 109.95 MRows │ 838.86 MB         │ 298.06 MB       │ 2.81              │
│ 5   │ category_code │ Union{Missing, String} │ 109.95 MRows │ 1.97 GB           │ 470.16 MB       │ 4.28              │
│ 6   │ brand         │ Union{Missing, String} │ 109.95 MRows │ 956.34 MB         │ 418.92 MB       │ 2.28              │
│ 7   │ price         │ Union{Missing, String} │ 109.95 MRows │ 1015.72 MB        │ 542.99 MB       │ 1.87              │
│ 8   │ user_id       │ Union{Missing, String} │ 109.95 MRows │ 1.33 GB           │ 614.19 MB       │ 2.22              │
│ 9   │ user_session  │ Union{Missing, String} │ 109.95 MRows │ 4.1 GB            │ 2.79 GB         │ 1.47              │
│ 10  │ Table total   │                        │ 109.95 MRows │ 14.9 GB           │ 5.8 GB          │ 2.57              │

julia> head(t)
Time: 0:00:00 read: 65.54 KRows (2.25 MRows/sec)
10×9 DataFrames.DataFrame
│ Row │ event_time              │ event_type │ product_id │ category_id         │ category_code                       │ brand    │ price   │ user_id   │ user_session                         │
│     │ Union{Missing, String}  │ String⍰    │ String⍰    │ Int64               │ Union{Missing, String}              │ String⍰  │ String⍰ │ String⍰   │ Union{Missing, String}               │
├─────┼─────────────────────────┼────────────┼────────────┼─────────────────────┼─────────────────────────────────────┼──────────┼─────────┼───────────┼──────────────────────────────────────┤
│ 1   │ 2019-10-01 00:00:00 UTC │ view       │ 44600062   │ 2103807459595387724 │ missing                             │ shiseido │ 35.79   │ 541312140 │ 72d76fde-8bb3-4e00-8c23-a032dfed738c │
│ 2   │ 2019-10-01 00:00:00 UTC │ view       │ 3900821    │ 2053013552326770905 │ appliances.environment.water_heater │ aqua     │ 33.20   │ 554748717 │ 9333dfbd-b87a-4708-9857-6336556b0fcc │
│ 3   │ 2019-10-01 00:00:01 UTC │ view       │ 17200506   │ 2053013559792632471 │ furniture.living_room.sofa          │ missing  │ 543.10  │ 519107250 │ 566511c2-e2e3-422b-b695-cf8e6e792ca8 │
│ 4   │ 2019-10-01 00:00:01 UTC │ view       │ 1307067    │ 2053013558920217191 │ computers.notebook                  │ lenovo   │ 251.74  │ 550050854 │ 7c90fc70-0e80-4590-96f3-13c02c18c713 │
│ 5   │ 2019-10-01 00:00:04 UTC │ view       │ 1004237    │ 2053013555631882655 │ electronics.smartphone              │ apple    │ 1081.98 │ 535871217 │ c6bd7419-2748-4c56-95b4-8cec9ff8b80d │
│ 6   │ 2019-10-01 00:00:05 UTC │ view       │ 1480613    │ 2053013561092866779 │ computers.desktop                   │ pulser   │ 908.62  │ 512742880 │ 0d0d91c2-c9c2-4e81-90a5-86594dec0db9 │
│ 7   │ 2019-10-01 00:00:08 UTC │ view       │ 17300353   │ 2053013553853497655 │ missing                             │ creed    │ 380.96  │ 555447699 │ 4fe811e9-91de-46da-90c3-bbd87ed3a65d │
│ 8   │ 2019-10-01 00:00:08 UTC │ view       │ 31500053   │ 2053013558031024687 │ missing                             │ luminarc │ 41.16   │ 550978835 │ 6280d577-25c8-4147-99a7-abc6048498d6 │
│ 9   │ 2019-10-01 00:00:10 UTC │ view       │ 28719074   │ 2053013565480109009 │ apparel.shoes.keds                  │ baden    │ 102.71  │ 520571932 │ ac1cd4e5-a3ce-4224-a2d7-ff660a105880 │
│ 10  │ 2019-10-01 00:00:11 UTC │ view       │ 1004545    │ 2053013555631882655 │ electronics.smartphone              │ huawei   │ 566.01  │ 537918940 │ 406c46ed-90a4-4787-a43b-59a410c1a5fb │
```
You can convert product_id, user_id and price in similar way.

Converting event_time is a bit more complicated:
```julia
julia> sum(ismissing.(t.event_time)) #check missings
Time: 0:00:06 read: 109.95 MRows (17.64 MRows/sec)
0
julia> rename_column!(t, :event_time, :event_time_raw)

julia> string_col = string.(t.event_time_raw) #get DFColumn{String} from DFColumn{Union{String, Missing}}

julia> date_convert(s)::DateTime = DateTime(parse.(Int64, SubString.(string.(s), (1:4, 6:7, 9:10, 12:13, 15:16, 18:19)))...) #Conversion function

julia> result_col = date_convert.(string_col)
Time: 0:00:00 read: 109.95 MRows (237.47 MRows/sec)
DataFrameDBs.DFColumn{DateTime}

julia> add_column!(t, :event_time, result_col, before = :event_type)
Time: 0:00:43 read: 109.95 MRows (2.54 MRows/sec)

julia> drop_column!(t, :event_time_raw)
```
Finaly convert all String,Missing columns to String columns with replacing missings by empty strings
```julia
julia> rename_column!(t, :event_type, :event_type_raw)

julia> string_convert(x) = ismissing(x) ? "" : String(x)
string_convert (generic function with 1 method)

julia> string_convert.(t.event_type_raw)
Time: 0:00:00 read: 109.95 MRows (174.5 MRows/sec)
DataFrameDBs.DFColumn{String}

julia> add_column!(t, :event_type, string_convert.(t.event_type_raw), before = :product_id)
Time: 0:00:06 read: 109.95 MRows (17.9 MRows/sec)

julia> drop_column!(t, :event_type_raw)
```
Other columns are converted in the same way.

Now we have the prepared table:
```julia
ulia> table_stats(t)
10×6 DataFrames.DataFrame
│ Row │ column        │ type     │ rows         │ uncompressed size │ compressed size │ compression ratio │
│     │ Symbol        │ String   │ String       │ String            │ String          │ Float64           │
├─────┼───────────────┼──────────┼──────────────┼───────────────────┼─────────────────┼───────────────────┤
│ 1   │ event_time    │ DateTime │ 109.95 MRows │ 838.86 MB         │ 43.81 MB        │ 19.15             │
│ 2   │ event_type    │ String   │ 109.95 MRows │ 845.2 MB          │ 43.02 MB        │ 19.65             │
│ 3   │ product_id    │ Int64    │ 109.95 MRows │ 838.86 MB         │ 403.31 MB       │ 2.08              │
│ 4   │ category_id   │ Int64    │ 109.95 MRows │ 838.86 MB         │ 298.06 MB       │ 2.81              │
│ 5   │ category_code │ String   │ 109.95 MRows │ 1.97 GB           │ 467.32 MB       │ 4.31              │
│ 6   │ brand         │ String   │ 109.95 MRows │ 956.34 MB         │ 418.3 MB        │ 2.29              │
│ 7   │ price         │ Float64  │ 109.95 MRows │ 838.86 MB         │ 475.22 MB       │ 1.77              │
│ 8   │ user_id       │ Int64    │ 109.95 MRows │ 838.86 MB         │ 424.92 MB       │ 1.97              │
│ 9   │ user_session  │ String   │ 109.95 MRows │ 4.1 GB            │ 2.79 GB         │ 1.47              │
│ 10  │ Table total   │          │ 109.95 MRows │ 11.92 GB          │ 5.3 GB          │ 2.25              │
```
It's typed and takes up more than two times less disk space than csv

### Work with data

Get unique event_type and brands:
```julia
julia> unique(t.event_type)
Time: 0:00:09 read: 109.95 MRows (11.14 MRows/sec)
3-element Array{Any,1}:
 "view"
 "purchase"
 "cart"

julia> unique(t.brand[t.brand .!= ""])
Time: 0:00:14 read: 109.95 MRows (7.54 MRows/sec)
4303-element Array{Any,1}:
 "shiseido"
 "aqua"
 "lenovo"
 "apple"
 "pulser"
 "creed"
 "luminarc"
 "baden"
 "huawei"
 ....
```
Mean price of huawai and apple
```julia
julia> using Statistics
julia> mean(t.price[t.brand.=="huawei"])
Time: 0:00:04 read: 109.95 MRows (22.55 MRows/sec)
264.23702928355846

julia> mean(t.price[t.brand.=="apple"])
Time: 0:00:05 read: 109.95 MRows (18.97 MRows/sec)
828.5794773596991
```

Materialize all rows, where price is more then 2000, event_type is "purchase" and brand is "samsung"

```julia
julia> t[(t.price.>2000).&(t.event_type.=="purchase").&(t.brand.=="samsung"), :] |> materialize
Time: 0:00:11 read: 109.95 MRows (9.83 MRows/sec)
217×9 DataFrames.DataFrame
│ Row │ event_time          │ event_type │ product_id │ category_id         │ category_code          │ brand   │ price   │ user_id   │ user_session                         │
│     │ DateTime            │ String     │ Int64      │ Int64               │ String                 │ String  │ Float64 │ Int64     │ String                               │
├─────┼─────────────────────┼────────────┼────────────┼─────────────────────┼────────────────────────┼─────────┼─────────┼───────────┼──────────────────────────────────────┤
│ 1   │ 2019-10-01T06:33:37 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2573.79 │ 548673724 │ 0fa51c80-9a1d-40cc-a9c8-cb409a8f2baa │
│ 2   │ 2019-10-02T16:02:30 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2573.79 │ 546970144 │ 9f1dea21-6679-4129-ba05-ed32147cdbc8 │
│ 3   │ 2019-10-05T08:54:49 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2573.79 │ 534273035 │ 419cd92d-0395-48ed-86bc-293d0a7e44fb │
│ 4   │ 2019-10-05T09:37:14 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2573.79 │ 555860702 │ e9f509b7-6cbb-43ce-b877-3a08d197b73c │
......
│ 215 │ 2019-11-30T20:37:32 │ purchase   │ 1005284    │ 2053013555631882655 │ electronics.smartphone │ samsung │ 2562.49 │ 556588365 │ 6a939884-9605-406b-a5c8-45e1a31e9956 │
│ 216 │ 2019-11-30T22:27:16 │ purchase   │ 100015658  │ 2053013555631882655 │ electronics.smartphone │ samsung │ 2562.49 │ 512762058 │ 8b65dc47-baaf-4348-8db6-3801b2ff13f9 │
│ 217 │ 2019-11-30T22:28:47 │ purchase   │ 100015658  │ 2053013555631882655 │ electronics.smartphone │ samsung │ 2562.49 │ 512762058 │ 53effbbc-7cc9-4e37-9d69-136b02cb88e9 │
```

Check above condition only on each 10th row of the table:
```julia
v = t[1:10:end, :]
Time: 0:00:00 read: 109.95 MRows (221.21 MRows/sec)
View of table ecommerce
Projection: event_time=>col(event_time)::Dates.DateTime; event_type=>col(event_type)::String; product_id=>col(product_id)::Int64; category_id=>col(category_id)::Int64; category_code=>col(category_code)::String; brand=>col(brand)::String; price=>col(price)::Float64; user_id=>col(user_id)::Int64; user_session=>col(user_session)::String
Selection: 1:10:109950741

julia> v[(v.price.>2000).&(v.event_type.=="purchase").&(v.brand.=="samsung"), :] |> materialize
Time: 0:00:02 read: 109.95 MRows (40.87 MRows/sec)
23×9 DataFrames.DataFrame
│ Row │ event_time          │ event_type │ product_id │ category_id         │ category_code          │ brand   │ price   │ user_id   │ user_session                         │
│     │ Dates.DateTime      │ String     │ Int64      │ Int64               │ String                 │ String  │ Float64 │ Int64     │ String                               │
├─────┼─────────────────────┼────────────┼────────────┼─────────────────────┼────────────────────────┼─────────┼─────────┼───────────┼──────────────────────────────────────┤
│ 1   │ 2019-10-29T11:20:54 │ purchase   │ 1800579    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2201.68 │ 563223250 │ 8f1e2791-72cf-4f2f-9782-4f064771b20b │
│ 2   │ 2019-11-01T19:25:50 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2574.04 │ 562294362 │ f3b2be78-853e-4627-9540-8a1a02ff6bdd │
│ 3   │ 2019-11-10T17:48:23 │ purchase   │ 1005284    │ 2053013555631882655 │ electronics.smartphone │ samsung │ 2562.49 │ 569266155 │ 173075bd-dfe1-43ae-9b3a-82639936a6ea │
│ 4   │ 2019-11-11T16:52:10 │ purchase   │ 1005284    │ 2053013555631882655 │ electronics.smartphone │ samsung │ 2562.49 │ 513105762 │ 1ac781e3-db0c-48e6-a332-46562402ccc9 │
│ 5   │ 2019-11-13T08:57:56 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2573.79 │ 567131154 │ e4fc43ef-36c2-4f2d-9512-0cfcf1126d4a │
│ 6   │ 2019-11-16T13:01:29 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2573.79 │ 518371713 │ 7934aeef-32df-4eb3-8f9f-d873a97d2c60 │
│ 7   │ 2019-11-16T15:10:15 │ purchase   │ 1802024    │ 2053013554415534427 │ electronics.video.tv   │ samsung │ 2573.79 │ 572245478 │ 8e4414a3-61e5-461c-a1e2-0bc9b53db381 │
.........
```

Calculate sum of prices for rows, matching condition above:

```julia
julia> using Statistics

julia> mean(v[(v.price.>2000).&(v.event_type.=="purchase").&(v.brand.=="samsung"), :price])
Time: 0:00:02 read: 109.95 MRows (51.44 MRows/sec)
2546.1417391304344
```

## Public API

```@docs
DFView
DFTable
DFColumn
create_table
open_table
empty_table
drop_table!
truncate_table!
add_column!
rename_column!
drop_column!
turnon_progress!
turnoff_progress!
insert
materialize
head
rows 
map_to_column
table_stats
ColumnTypes.serialize
ColumnTypes.Ast
```

## Future plans

Julia is my hobby, so further development depends on my free time, and, more importantly, on the community’s interest in the package. If the DataFrameDBs is interesting, then the following features are possible

- Adding NamedTuples, Vectors, Nested Vectors (i.e. Vector{Vector{Vector}}), Vectors of Strings and Tuples of Strings to stored types
- Adding CategorialArrays to stored types
- Integration with OnlineStats and aggregation functional directly on DFView without materialization
- Database infrastructure - i.e. several tables with possibility of joins, persistent join indexes and etc.
- Bloom filters as secondary indexes
- Integration with Tables.jl interfaces and DataFrames.jl interfaces
- Bulk updates possibility
- Primary key, aka stored sort order with possibility of resort stored data
