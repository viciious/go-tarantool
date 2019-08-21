-- version comment below is used for system.d spec file
-- VERSION = '0.3.3'

local box = require('box')
local fio = require('fio')
local errno = require('errno')

-- lastsnapfilename will be returned in case of success and nil otherwise.
local function lastsnapfilename()
    local lastsnapfn = ""
    local snap_dir = box.cfg.memtx_dir or box.cfg.snap_dir
    for _, fname in ipairs(fio.glob(fio.pathjoin(snap_dir, '*.snap'))) do
        if fname > lastsnapfn then
            lastsnapfn = fname
        end
    end
    -- a ? b : c
    return (lastsnapfn ~= "") and lastsnapfn or nil
end

-- readfile with the byte limit.
-- It returns result data and nil in case of success and nil with error message otherwise.
local function readfile(filename, limit)

    local snapfile = fio.open(filename, {'O_RDONLY'})
    if not snapfile then
        return nil, "failed to open file " .. filename .. ": " .. errno.strerror()
    end

    local data = snapfile:read(limit)
    snapfile:close()
    if not data then
        return nil, "failed to read file " .. filename
    end
    return data, nil
end

-- parsevclock in the given data.
-- Returns vector clock table and nil if succeed and nil with error message otherwise.
local function parsevclock(data)
    local vectorpattern = "VClock: {([%s%d:,]*)}"
    local clockspattern = "%s*(%d+)%s*:%s*(%d+)"

    _, _, data = string.find(data, vectorpattern)
    if data == nil then
        return nil
    end

    local vc = {}
    for id, lsn in string.gmatch(data, clockspattern) do
        vc[tonumber(id)] = tonumber64(lsn)
    end

    return vc
end

-- lastsnapvclock returns vector clock of the latest snapshot file.
-- In case of any errors there will be raised box.error.
local function lastsnapvclock()
    local err
    local data
    local vclock
    local limit = 1024

    local snapfilename = lastsnapfilename()
    if not snapfilename then
        box.error(box.error.PROC_LUA, "last snapshot file hasn't been found")
    end

    data, err = readfile(snapfilename, limit)
    if err then
        box.error(box.error.PROC_LUA, err)
    end
    if data == "" then
        box.error(box.error.PROC_LUA, "empty file " .. snapfilename)
    end

    vclock = parsevclock(data)
    if not vclock then
        box.error(box.error.PROC_LUA, "there is no vector clock in file " .. snapfilename)
    end
    return vclock
end

return {
    lastsnapvclock = lastsnapvclock,
    lastsnapfilename = lastsnapfilename,
    readfile = readfile,
    parsevclock = parsevclock,
}