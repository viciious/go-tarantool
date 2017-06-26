-- version comment below is used for system.d spec file
-- VERSION = '0.1.1'

local box = require('box')
local fio = require('fio')

function lastsnaplsn()
	local lsn = 0
	for _, fname in ipairs(fio.glob(fio.pathjoin(box.cfg.snap_dir, '*.snap'))) do
		local curlsn = tonumber64(fio.basename(fname, '.snap'))
		if curlsn > lsn then
			lsn = curlsn
		end
	end
	return lsn
end
