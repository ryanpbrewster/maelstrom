package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	seen := make(map[int]struct{})
	known := make(map[string]map[int]struct{})
	var neighbors []string
	var mu sync.Mutex

	kick := make(chan struct{}, 1)
	go func() error {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-kick:
			case <-ticker.C:
			}

			mu.Lock()
			for _, neighbor := range neighbors {
				var missing []int
				for m := range seen {
					if _, ok := known[neighbor][m]; !ok {
						missing = append(missing, m)
					}
				}
				if len(missing) > 0 {
					n.RPC(neighbor, gossipRequest{Type: "gossip", Messages: missing}, func(msg maelstrom.Message) error {
						mu.Lock()
						for _, m := range missing {
							known[neighbor][m] = struct{}{}
						}
						mu.Unlock()
						return nil
					})
				}
			}
			mu.Unlock()
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()

		learned := false
		if _, ok := seen[body.Message]; !ok {
			seen[body.Message] = struct{}{}
			learned = true
		}
		if learned {
			select {
			case kick <- struct{}{}:
			default:
			}
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		// Our partner nodes will ack our broadcasts, and right now we're just ignoring that.
		return nil
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body gossipRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()

		learned := false
		for _, m := range body.Messages {
			known[msg.Src][m] = struct{}{}
			if _, ok := seen[m]; !ok {
				seen[m] = struct{}{}
				learned = true
			}
		}
		if learned {
			select {
			case kick <- struct{}{}:
			default:
			}
		}
		return n.Reply(msg, map[string]any{"type": "gossip_ok"})
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		// Our partner nodes will ack our gossips, and right now we're just ignoring that.
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		messages := make([]int, 0, len(seen))
		for m := range seen {
			messages = append(messages, m)
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": messages})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		neighbors = body.Topology[n.ID()]
		for _, neighbor := range neighbors {
			known[neighbor] = make(map[int]struct{})
		}

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

type gossipRequest struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type topologyRequest struct {
	Topology map[string][]string `json:"topology"`
}
