local tarantool_lastsnapvclock = require("tarantool_lastsnapvclock")
lastsnapvclock = tarantool_lastsnapvclock.lastsnapvclock
box.once('func:lastsnapvclock', function()
    box.schema.func.create('lastsnapvclock', {if_not_exists = true})
end)

lastsnapfilename = tarantool_lastsnapvclock.lastsnapfilename
box.once('func:lastsnapfilename', function()
    box.schema.func.create('lastsnapfilename', {if_not_exists = true})
end)

readfile = tarantool_lastsnapvclock.readfile
box.once('func:readfile', function()
    box.schema.func.create('readfile', {if_not_exists = true})
end)

parsevclock = tarantool_lastsnapvclock.parsevclock
box.once('func:parsevclock', function()
    box.schema.func.create('parsevclock', {if_not_exists = true})
end)
