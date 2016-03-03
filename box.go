package tnt

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
)

// Box is tarantool instance. For start/stop tarantool in tests
type Box struct {
	Root     string
	Port     uint
	cmd      *exec.Cmd
	stopOnce sync.Once
	stopped  chan bool
}

type BoxOptions struct {
	Listen  uint
	PortMin uint
	PortMax uint
}

func NewBox(config string, options *BoxOptions) (*Box, error) {
	if options == nil {
		options = &BoxOptions{}
	}

	if options.PortMin == 0 {
		options.PortMin = 8000
	}

	if options.PortMax == 0 {
		options.PortMax = 9000
	}

	if options.Listen != 0 {
		options.PortMin = options.Listen
		options.PortMax = options.Listen
	}

	var box *Box

START_LOOP:
	for port := options.PortMin; port <= options.PortMax; port++ {

		tmpDir, err := ioutil.TempDir("", "") //os.RemoveAll(tmpDir);
		if err != nil {
			return nil, err
		}

		initLua := `
        box.cfg{
            listen = {port},
            snap_dir = "{root}/snap/",
            sophia_dir = "{root}/sophia/",
            wal_dir = "{root}/wal/"
        }
        `

		initLua = strings.Replace(initLua, "{port}", fmt.Sprintf("%d", port), -1)
		initLua = strings.Replace(initLua, "{root}", tmpDir, -1)

		initLua = fmt.Sprintf("%s\n%s", initLua, config)

		initLuaFile := path.Join(tmpDir, "init.lua")
		err = ioutil.WriteFile(initLuaFile, []byte(initLua), 0644)
		if err != nil {
			return nil, err
		}

		for _, subDir := range []string{"sophia", "snap", "wal"} {
			err = os.Mkdir(path.Join(tmpDir, subDir), 0755)
			if err != nil {
				return nil, err
			}
		}

		cmd := exec.Command("tarantool", initLuaFile)
		boxStderr, err := cmd.StderrPipe()
		if err != nil {
			return nil, err
		}

		err = cmd.Start()
		if err != nil {
			return nil, err
		}

		var boxStderrBuffer bytes.Buffer

		p := make([]byte, 1024)

		box = &Box{
			Root:    tmpDir,
			Port:    port,
			cmd:     cmd,
			stopped: make(chan bool),
		}

	WAIT_LOOP:
		for {
			if strings.Contains(boxStderrBuffer.String(), "entering the event loop") {
				break START_LOOP
			}

			if strings.Contains(boxStderrBuffer.String(), "is already in use, will retry binding after") {
				cmd.Process.Kill()
				cmd.Process.Wait()
				break WAIT_LOOP
			}

			n, err := boxStderr.Read(p)
			if err != nil {
				return nil, err
			}

			boxStderrBuffer.Write(p[:n])
		}

		os.RemoveAll(box.Root)
		box = nil
	}

	if box == nil {
		return nil, fmt.Errorf("Can't bind any port from %d to %d", options.PortMin, options.PortMax)
	}

	return box, nil
}

func (box *Box) Close() {
	box.stopOnce.Do(func() {
		box.cmd.Process.Kill()
		box.cmd.Process.Wait()
		os.RemoveAll(box.Root)
		close(box.stopped)
	})
	<-box.stopped
}
