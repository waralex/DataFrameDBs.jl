Base.broadcastable(c::DFColumn) = c

struct DFColumnStyle <: Base.Broadcast.BroadcastStyle end


Base.Broadcast.BroadcastStyle(::Type{<:DFColumn}) = DFColumnStyle()


Base.Broadcast.BroadcastStyle(::DFColumnStyle, ::DFColumnStyle) = DFColumnStyle()
Base.Broadcast.BroadcastStyle(::DFColumnStyle, ::T) where {T <: Base.Broadcast.BroadcastStyle} = DFColumnStyle()
Base.Broadcast.BroadcastStyle(::T, ::DFColumnStyle) where {T <: Base.Broadcast.BroadcastStyle} = DFColumnStyle()

Base.Broadcast.BroadcastStyle(::DFColumnStyle, style::T) where {T <: Base.Broadcast.AbstractArrayStyle{0}} = DFColumnStyle()
Base.Broadcast.BroadcastStyle(style::T, ::DFColumnStyle) where {T <: Base.Broadcast.AbstractArrayStyle{0}} = DFColumnStyle()

Base.Broadcast.BroadcastStyle(::DFColumnStyle, style::T) where {T <: Base.Broadcast.AbstractArrayStyle} = style
Base.Broadcast.BroadcastStyle(style::T, ::DFColumnStyle) where {T <: Base.Broadcast.AbstractArrayStyle} = style

function _check_same_selection(arg::DFColumn, table_selection)
    isnothing(table_selection) && return (arg.view.table, arg.view.selection)
    
    ((arg.view.table, arg.view.selection) != table_selection) && throw(ArgumentError("All columns in broadcast must have same selection and table"))
    return table_selection
end

function _check_same_selection(arg::Base.Broadcast.Broadcasted{DFColumnStyle}, table_selection)
    for arg in arg.args
        table_selection = _check_same_selection(arg, table_selection)
    end
    return table_selection
end

_check_same_selection(arg, table_selection) = table_selection

function _bc_convert_arg(arg::Base.Broadcast.Broadcasted{DFColumnStyle})
    bcargs = map(_bc_convert_arg, arg.args)
    return BlockBroadcasting(arg.f, bcargs)
end

_bc_convert_arg(arg::DFColumn) = arg.view.projection.cols[1]

_bc_convert_arg(arg) = arg    

function Base.copy(bc::Base.Broadcast.Broadcasted{DFColumnStyle})
    table_selection = nothing
    for arg in bc.args
        table_selection = _check_same_selection(arg, table_selection)
    end
    
    res_block_broad = _bc_convert_arg(bc)
    
    return DFColumn(
        DFView(
            table_selection[1],
            Projection((a = res_block_broad,)),
            table_selection[2]
        )
    )
end

function Base.copyto!(dest::AbstractArray,bc::Base.Broadcast.Broadcasted{DFColumnStyle})
    
    col = Base.Broadcast.materialize(bc)
    offset = 1
    for block in BlocksIterator(col.view)        
        view(dest, offset:(offset + length(block[1]) - 1)) .= block[1]
        offset += length(block[1])
    end   
end
