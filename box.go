package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Box is tarantool instance. For start/stop tarantool in tests
type Box struct {
	Root       string
	WorkDir    string
	Port       uint
	Listen     string
	cmd        *exec.Cmd
	stopOnce   sync.Once
	stopped    chan bool
	initLua    string
	notifySock string
	version    string
}

type BoxOptions struct {
	Host    string
	Port    uint
	PortMin uint
	PortMax uint
	WorkDir string

	LogDir        string
	LogNamePrefix string
}

var (
	ErrPortAlreadyInUse = errors.New("port already in use")
)

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

	if options.Port != 0 {
		options.PortMin = options.Port
		options.PortMax = options.Port
	}

	if options.Host == "" {
		options.Host = "127.0.0.1"
	}
	if !strings.HasSuffix(options.Host, ":") {
		options.Host += ":"
	}

	var box *Box

	for port := options.PortMin; port <= options.PortMax; port++ {
		tmpDir, err := os.MkdirTemp("", options.LogNamePrefix)
		if err != nil {
			return nil, err
		}

		notifySock := filepath.Join(tmpDir, "notify.sock")

		logDir := options.LogDir
		if logDir == "" {
			logDir = os.Getenv("TNT_LOG_DIR")
		}

		logPath := "stderr"
		if logDir != "" {
			_, fName := filepath.Split(tmpDir)
			logPath = fmt.Sprintf(`"%s"`, filepath.Join(logDir, fName))
			fmt.Println("Tarantool log path:", logPath)
		}

		initLua := strings.Replace(`
			box.cfg{
				memtx_dir = "{root}/snap/",
				wal_dir = "{root}/wal/",
				log = {log},
			}
		`, "{log}", logPath, -1)

		initLua += `
			sendstatus("STARTING")

			box.once('guest:read_universe', function()
				box.schema.user.grant('guest', 'read', 'universe')
			end)

			sendstatus("BINDING")

			box.cfg{
				listen = "{host}{port}",
			}

			sendstatus("READY")
		`
		readyLua := `
			sendstatus("RUNNING")
		`

		initLua = fmt.Sprintf("%s\n%s\n%s\n", initLua, config, readyLua)
		initLua = strings.Replace(initLua, "{host}", options.Host, -1)
		initLua = strings.Replace(initLua, "{port}", fmt.Sprintf("%d", port), -1)
		initLua = strings.Replace(initLua, "{root}", tmpDir, -1)

		initLua = fmt.Sprintf(`
			local sendstatus = function(status)
				local path = "{notify_sock_path}"
				if path ~= "" and path ~= "{" .. "notify_sock_path" .. "}" then
					local socket = require('socket')
					local sock = socket("AF_UNIX", "SOCK_DGRAM", 0)
					sock:sysconnect("unix/", path)
					if sock ~= nil then
						sock:write(status)
						sock:close()
					end
				end
			end

			%s
		`, initLua)

		initLua = strings.Replace(initLua, "{notify_sock_path}", notifySock, -1)

		for _, subDir := range []string{"snap", "wal"} {
			err = os.Mkdir(path.Join(tmpDir, subDir), 0755)
			if err != nil {
				return nil, err
			}
		}

		box = &Box{
			Root:       tmpDir,
			WorkDir:    options.WorkDir,
			Listen:     fmt.Sprintf("%s%d", options.Host, port),
			Port:       port,
			cmd:        nil,
			stopped:    make(chan bool),
			initLua:    initLua,
			notifySock: notifySock,
		}
		close(box.stopped)

		ver, err := box.Version()
		if err != nil {
			return nil, err
		}

		if strings.HasPrefix(ver, "1.6") {
			box.initLua = strings.Replace(box.initLua, "memtx_dir =", "snap_dir =", -1)
			box.initLua = strings.Replace(box.initLua, "log =", "logger =", -1)
		}

		err = box.Start()
		if err == nil {
			break
		}
		if err != ErrPortAlreadyInUse {
			return nil, err
		}
		os.RemoveAll(box.Root)
		box = nil
	}

	if box == nil {
		return nil, fmt.Errorf("can't bind any port from %d to %d", options.PortMin, options.PortMax)
	}

	return box, nil
}

func (box *Box) StartWithLua(luaTransform func(string) string) error {
	if !box.IsStopped() {
		return nil
	}

	box.stopped = make(chan bool)

	initLua := box.initLua
	if luaTransform != nil {
		initLua = luaTransform(initLua)
	}

	initLuaFile := path.Join(box.Root, "init.lua")
	err := os.WriteFile(initLuaFile, []byte(initLua), 0644)
	if err != nil {
		return err
	}

	if box.WorkDir != "" {
		oldwd, err := os.Getwd()
		if err != nil {
			return err
		}

		err = os.Chdir(box.WorkDir)
		if err != nil {
			return err
		}
		defer os.Chdir(oldwd)
	}

	statusCh := make(chan string, 10)
	u, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: box.notifySock, Net: "unix"})
	if err != nil {
		return err
	}
	defer os.Remove(box.notifySock)

	go func() {
		for {
			pck := make([]byte, 128)
			nr, err := u.Read(pck)
			if err != nil {
				close(statusCh)
				return
			}
			msg := string(pck[0:nr])
			statusCh <- msg
			if msg == "RUNNING" {
				close(statusCh)
				return
			}
		}
	}()

	cmd := exec.Command("tarantool", initLuaFile)
	box.cmd = cmd

	err = cmd.Start()
	if err != nil {
		return err
	}

	for status := range statusCh {
		if status == "RUNNING" {
			return nil
		}
		if status == "BINDING" {
			select {
			case status = <-statusCh:
				if status != "READY" {
					box.Close()
					if strings.Contains(status, "failed to bind, called on fd -1") {
						return ErrPortAlreadyInUse
					}
					return fmt.Errorf("Box status is '%s', not READY", status)
				}
			case <-time.After(time.Millisecond * 50):
				box.Close()
				return ErrPortAlreadyInUse
			}
		}
	}

	box.Close()
	return ErrPortAlreadyInUse
}

func (box *Box) Start() error {
	return box.StartWithLua(nil)
}

func (box *Box) Stop() {
	go func() {
		select {
		case <-box.stopped:
			return
		default:
			if box.cmd != nil {
				box.cmd.Process.Signal(syscall.SIGTERM)
				//box.cmd.Process.Kill()
				box.cmd.Process.Wait()
				box.cmd = nil
			}
			close(box.stopped)
		}
	}()
	<-box.stopped
}

func (box *Box) IsStopped() bool {
	select {
	case <-box.stopped:
		return true
	default:
		return false
	}
}

func (box *Box) Close() {
	box.stopOnce.Do(func() {
		box.Stop()
		os.RemoveAll(box.Root)
	})
}

func (box *Box) Addr() string {
	return box.Listen
}

func (box *Box) Connect(options *Options) (*Connection, error) {
	return Connect(box.Addr(), options)
}

func (box *Box) Version() (string, error) {
	verPrefix := "Tarantool "

	if box.version != "" {
		return box.version, nil
	}

	var out bytes.Buffer
	cmd := exec.Command("tarantool", "--version")
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(&out)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		t := scanner.Text()
		if !strings.HasPrefix(t, verPrefix) {
			continue
		}

		var major, minor, patch uint32
		ver := string(t[len(verPrefix):])
		if n, _ := fmt.Sscanf(ver, "%d.%d.%d", &major, &minor, &patch); n != 3 {
			continue
		}

		box.version = fmt.Sprintf("%d.%d.%d", major, minor, patch)
		break
	}

	if box.version == "" {
		return "", errors.New("unknown Tarantool version")
	}
	return box.version, nil
}
