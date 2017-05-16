package tarantool_test

import (
	"log"
	"strings"

	"sync"

	tnt16 "gitlab.corp.mail.ru/rb/go/helper/tarantool"
)

func ExampleSlave_subscribeExisted() {
	// Subscribe for master's changes synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
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
	// always detach slave to preserve socket descriptor
	defer s.Detach()

	// let's start from the beginning
	var lsn int64 = 0
	err = s.Subscribe(lsn)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// consume master's changes permanently
	for s.Consume() {
		p = s.Packet()
		log.Println(p)
		log.Println(hr)
	}
	if s.Err() != nil {
		log.Printf("Tnt Slave consuming error:%v", err)
	}
}

func ExampleSlave_subscribeNew() {
	// Silently join slave to Replica Set and consume master's changes synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always detach slave to preserve socket descriptor
	defer s.Detach()

	// let's start from the beginning
	var lsn int64 = 0
	err = s.Attach(lsn)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// consume master's changes permanently
	for s.Consume() {
		p = s.Packet()
		log.Println(p)
		log.Println(hr)
	}
	if s.Err() != nil {
		log.Printf("Tnt Slave consuming error:%v", err)
	}
}

func ExampleSlave_Join() {
	// Silently join slave to Replica Set

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always detach slave to preserve socket descriptor
	defer s.Detach()

	if err = s.Join(); err != nil {
		log.Printf("Tnt Slave joining error:%v", err)
		return
	}

	log.Printf("UUID=%#v Replica Set UUID=%#v\n", s.UUID, s.ReplicaSet.UUID)
}

func ExampleSlave_JoinWithSnap_sync() {
	// Join slave to Replica Set with iterating snapshot synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always detach slave to preserve socket descriptor
	defer s.Detach()

	// get iterator on snapshot
	it, err := s.JoinWithSnap()
	if err != nil {
		log.Printf("Tnt Slave joining error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	for it.NextSnap() {
		p = it.Packet()
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
	// always check error after iterator!
	if it.Err() != nil {
		log.Printf("Tnt Slave joining error:%v", err)
		return
	}

	log.Printf("UUID=%#v Replica Set UUID=%#v\n", s.UUID, s.ReplicaSet.UUID)
}

func ExampleSlave_JoinWithSnap_async() {
	// Join slave to Replica Set with iterating snapshot asynchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always detach slave to preserve socket descriptor
	defer s.Detach()

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
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
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
	// always detach slave to preserve socket descriptor
	defer s.Detach()

	// let's start from the beginning
	var lsn int64 = 0
	err = s.Subscribe(lsn)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// consume master's changes permanently
	for s.Consume() {
		p = s.Packet()
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
	if s.Err() != nil {
		log.Printf("Tnt Slave consuming error:%v", err)
		return
	}
}

func ExampleSlave_Subscribe_async() {
	// Subscribe for master's changes asynchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
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
	// always detach slave to preserve socket descriptor
	defer s.Detach()

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
	var lsn int64 = 0
	err = s.Subscribe(lsn, xlogChan)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// consume master's changes permanently
	wg.Wait()
}

func ExampleSlave_Attach_sync() {
	// Silently join slave to Replica Set and consume master's changes synchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always detach slave to preserve socket descriptor
	defer s.Detach()

	// let's start from the beginning
	var lsn int64 = 0
	err = s.Attach(lsn)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// print snapshot
	var p *tnt16.Packet
	var hr = strings.Repeat("-", 80)
	// consume master's changes permanently
	for s.Consume() {
		p = s.Packet()
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
	if s.Err() != nil {
		log.Printf("Tnt Slave consuming error:%v", err)
		return
	}
}

func ExampleSlave_Attach_async() {
	// Silently join slave to Replica Set and consume master's changes asynchronously

	// new slave instance connects to provided dsn instantly
	s, err := tnt16.NewSlave("username:password@127.0.0.1:8000", tnt16.Options{
		User:     "username",
		Password: "password"})
	if err != nil {
		log.Printf("Tnt Slave creating error:%v", err)
		return
	}
	// always detach slave to preserve socket descriptor
	defer s.Detach()

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
	var lsn int64 = 0
	err = s.Attach(lsn, xlogChan)
	if err != nil {
		log.Printf("Tnt Slave subscribing error:%v", err)
		return
	}

	// consume master's changes permanently
	wg.Wait()
}
