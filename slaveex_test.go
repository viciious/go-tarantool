package tarantool_test

import (
	"log"
	"strings"

	"sync"

	tnt16 "github.com/viciious/go-tarantool"
)

func ExampleSlave_subscribeExisted() {
	// Subscribe for master's changes synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password",
		// UUID of the instance in replica set. Required
		UUID: "7c025e42-2394-11e7-aacf-0242ac110002",
		// UUID of the Replica Set. Required
		ReplicaSetUUID: "3b39c6a4-f2da-4d81-a43b-103e5b1c16a1"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// let's start from the beginning
	var lsn int64 = 0
	it, err := s.Subscribe(lsn)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// iterate over master's changes permanently
	for {
		p, err = it.Next()
		if err != nil {
			log.Printf("Tnt Slave iterating error:%v", err)
			return
		}
		log.Println(p)
		log.Println(hr)
	}
}

func ExampleSlave_subscribeNew() {
	// Silently join slave to Replica Set and consume master's changes synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000")
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// let's start from the beginning
	it, err := s.Attach()
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// iterate over master's changes permanently
	for {
		p, err = it.Next()
		if err != nil {
			log.Printf("Tnt Slave iterating error:%v", err)
			return
		}
		log.Println(p)
		log.Println(hr)
	}
}

func ExampleSlave_Join() {
	// Silently join slave to Replica Set

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000")
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	if err = s.Join(); err != nil {
		log.Printf("Tnt Slave joining error:%v", err)
		return
	}

	log.Printf("UUID=%#v Replica Set UUID=%#v\n", s.UUID, s.ReplicaSet.UUID)
}

func ExampleSlave_JoinWithSnap_sync() {
	// Join slave to Replica Set with iterating snapshot synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000")
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// skip returned iterator; will be using self bufio.scanner-style iterator instead
	_, err = s.JoinWithSnap()
	if err != nil {
		log.Printf("Tnt Slave joining error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	for s.HasNext() {
		p = s.Packet()
		// print request
		log.Println(hr)
		switch q := p.Request.(type) {
		case *tnt16.Insert:
			switch q.Space {
			case tnt16.SpaceIndex, tnt16.SpaceSpace:
				// short default format
				log.Printf("Insert LSN:%v, Space:%v InstanceID:%v\n",
					p.LSN, q.Space, p.InstanceID)
			default:
				log.Printf("%v", p)
			}
		default:
			log.Printf("%v", p)
		}
	}
	// always checks for errors after iteration cycle
	if s.Err() != nil {
		log.Printf("Tnt Slave joining error:%v", err)
		return
	}

	log.Printf("UUID=%#v Replica Set UUID=%#v\n", s.UUID, s.ReplicaSet.UUID)
}

func ExampleSlave_JoinWithSnap_async() {
	// Join slave to Replica Set with iterating snapshot asynchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000")
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// chan for snapshot's packets
	snapChan := make(chan *tnt16.Packet, 128)
	wg := &sync.WaitGroup{}

	// run snapshot printer before join command
	wg.Add(1)
	go func(in <-chan *tnt16.Packet, wg *sync.WaitGroup) {
		defer wg.Done()

		var hr = strings.Repeat("-", 80)

		for p := range in {
			log.Println(hr)
			switch q := p.Request.(type) {
			case *tnt16.Insert:
				switch q.Space {
				case tnt16.SpaceIndex, tnt16.SpaceSpace:
					// short default format
					log.Printf("Insert LSN:%v, Space:%v InstanceID:%v\n",
						p.LSN, q.Space, p.InstanceID)
				default:
					log.Printf("%v", p)
				}
			default:
				log.Printf("%v", p)
			}
		}
	}(snapChan, wg)

	_, err = s.JoinWithSnap(snapChan)
	if err != nil {
		log.Printf("Tnt Slave joining error:%v", err)
		return
	}

	wg.Wait()

	log.Printf("UUID=%#v Replica Set UUID=%#v\n", s.UUID, s.ReplicaSet.UUID)
}

func ExampleSlave_Subscribe_sync() {
	// Subscribe for master's changes synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password",
		// UUID of the instance in replica set. Required
		UUID: "7c025e42-2394-11e7-aacf-0242ac110002",
		// UUID of the Replica Set. Required
		ReplicaSetUUID: "3b39c6a4-f2da-4d81-a43b-103e5b1c16a1"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// let's start from the beginning
	var lsn int64 = 0
	it, err := s.Subscribe(lsn)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// consume master's changes permanently
	for {
		p, err = it.Next()
		if err != nil {
			log.Printf("Tnt Slave consuming error:%v", err)
			return
		}
		log.Println(hr)
		switch q := p.Request.(type) {
		case *tnt16.Insert:
			switch q.Space {
			case tnt16.SpaceIndex, tnt16.SpaceSpace:
				// short default format
				log.Printf("Insert LSN:%v, Space:%v InstanceID:%v\n",
					p.LSN, q.Space, p.InstanceID)
			default:
				log.Printf("%v", p)
			}
		default:
			log.Printf("%v", p)
		}
	}
}

func ExampleSlave_Subscribe_async() {
	// Subscribe for master's changes asynchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password",
		// UUID of the instance in replica set. Required
		UUID: "7c025e42-2394-11e7-aacf-0242ac110002",
		// UUID of the Replica Set. Required
		ReplicaSetUUID: "3b39c6a4-f2da-4d81-a43b-103e5b1c16a1"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// chan for snapshot's packets
	xlogChan := make(chan *tnt16.Packet, 128)

	// run xlog printer before subscribing command
	go func(in <-chan *tnt16.Packet) {
		var hr = strings.Repeat("-", 80)

		for p := range in {
			log.Println(hr)
			switch q := p.Request.(type) {
			case *tnt16.Insert:
				switch q.Space {
				case tnt16.SpaceIndex, tnt16.SpaceSpace:
					// short default format
					log.Printf("Insert LSN:%v, Space:%v InstanceID:%v\n",
						p.LSN, q.Space, p.InstanceID)
				default:
					log.Printf("%v", p)
				}
			default:
				log.Printf("%v", p)
			}
		}
	}(xlogChan)

	// let's start from the beginning
	var lsn int64 = 0
	it, err := s.Subscribe(lsn)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// consume requests infinitely
	var p *tnt16.Packet
	for {
		p, err = it.Next()
		if err != nil {
			close(xlogChan)
			log.Printf("Tnt Slave consuming error:%v", err)
			return
		}
		xlogChan <- p
	}
}

func ExampleSlave_Attach_sync() {
	// Silently join slave to Replica Set and consume master's changes synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000")
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// let's start from the beginning
	it, err := s.Attach()
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// consume master's changes permanently
	for {
		p, err = it.Next()
		if err != nil {
			log.Printf("Tnt Slave consuming error:%v", err)
			return
		}
		log.Println(hr)
		switch q := p.Request.(type) {
		case *tnt16.Insert:
			switch q.Space {
			case tnt16.SpaceIndex, tnt16.SpaceSpace:
				// short default format
				log.Printf("Insert LSN:%v, Space:%v InstanceID:%v\n",
					p.LSN, q.Space, p.InstanceID)
			default:
				log.Printf("%v", p)
			}
		default:
			log.Printf("%v", p)
		}
	}
}

func ExampleSlave_Attach_async() {
	// Silently join slave to Replica Set and consume master's changes asynchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000")
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always close slave to preserve socket descriptor
	defer s.Close()

	// chan for snapshot's packets
	xlogChan := make(chan *tnt16.Packet, 128)
	wg := &sync.WaitGroup{}

	// run xlog printer before subscribing command
	wg.Add(1)
	go func(in <-chan *tnt16.Packet, wg *sync.WaitGroup) {
		defer wg.Done()

		var hr = strings.Repeat("-", 80)

		for p := range in {
			log.Println(hr)
			switch q := p.Request.(type) {
			case *tnt16.Insert:
				switch q.Space {
				case tnt16.SpaceIndex, tnt16.SpaceSpace:
					// short default format
					log.Printf("Insert LSN:%v, Space:%v InstanceID:%v\n",
						p.LSN, q.Space, p.InstanceID)
				default:
					log.Printf("%v", p)
				}
			default:
				log.Printf("%v", p)
			}
		}
	}(xlogChan, wg)

	// let's start from the beginning
	_, err = s.Attach(xlogChan)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// consume master's changes permanently
	wg.Wait()
}
