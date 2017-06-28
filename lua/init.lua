dofile('tarantool_lastsnaplsn.lua')
box.once('func:lastsnaplsn', function()
    box.schema.func.create('lastsnaplsn', {if_not_exists = true})
    end)
