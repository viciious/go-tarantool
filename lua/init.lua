local tarantool_lastsnaplsn = require("tarantool_lastsnaplsn")
lastsnaplsn = tarantool_lastsnaplsn.lastsnaplsn
box.once('func:lastsnaplsn', function()
    box.schema.func.create('lastsnaplsn', {if_not_exists = true})
    end)
