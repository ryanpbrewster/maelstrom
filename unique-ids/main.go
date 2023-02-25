package main

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// To generate unique ids, each node will serve sequential values that are
	// congruent to its id modulo the number of nodes.
	// That is, node `n` (out of `m` total) will serve values `i` such that `i % m == n`.

	var count uint64
	var nextId atomic.Uint64
	init := func() {
		ids := n.NodeIDs()
		count = uint64(len(ids))
		for idx, id := range ids {
			if id == n.ID() {
				nextId.Add(uint64(idx))
			}
		}

	}

	var initOnce sync.Once
	n.Handle("generate", func(msg maelstrom.Message) error {
		initOnce.Do(init)
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = nextId.Add(count)

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
