package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	fout, err := os.CreateTemp("", "maelstrom-brodcast")
	if err != nil {
		log.Fatal(err)
	}
	l := log.New(fout, fmt.Sprintf("[%v]", n.ID()), log.LstdFlags)

	seen := make(map[int]struct{})
	var neighbors []string
	var mu sync.Mutex

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		l.Printf("recv broadcast: %s", msg.Body)

		var body broadcastRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if _, ok := seen[body.Message]; !ok {
			seen[body.Message] = struct{}{}
			l.Printf("%d is new", body.Message)
			for _, neighbor := range neighbors {
				l.Printf("mirroring to %v: %+v", neighbor, body)
				n.Send(neighbor, body)
			}
		} else {
			l.Printf("already seen %d", body.Message)
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		// Our partner nodes will ack our broadcasts, and right now we're just ignoring that.
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		l.Printf("recv read: %s", msg.Body)
		mu.Lock()
		defer mu.Unlock()
		messages := make([]int, 0, len(seen))
		for m := range seen {
			messages = append(messages, m)
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": messages})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		l.Printf("recv topology: %s", msg.Body)
		var body topologyRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		neighbors = body.Topology[n.ID()]
		l.Printf("set neighbors=%v", neighbors)

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type broadcastRequest struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type topologyRequest struct {
	Topology map[string][]string `json:"topology"`
}
