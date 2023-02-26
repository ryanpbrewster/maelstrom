package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body addReqeuest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			cur, err := kv.ReadInt(context.TODO(), counterKey)
			keyExists := true
			if err != nil {
				var rpcerr *maelstrom.RPCError
				if errors.As(err, &rpcerr) && rpcerr.Code == maelstrom.KeyDoesNotExist {
					cur = 0
					keyExists = false
				} else {
					return err
				}
			}
			if err := kv.CompareAndSwap(context.TODO(), counterKey, cur, cur+int(body.Delta), !keyExists); err != nil {
				var rpcerr *maelstrom.RPCError
				if errors.As(err, &rpcerr) && rpcerr.Code == maelstrom.PreconditionFailed {
					continue
				} else {
					return err
				}
			}
			break
		}
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		for {
			// Do a random garbage write just to ensure we're up-to-date
			// Note that without this write, the underlying KV store is allowed to serve us stale reads.
			if err := kv.Write(context.TODO(), "garbage", rand.Int63()); err != nil {
				return err
			}
			cur, err := kv.ReadInt(context.TODO(), counterKey)
			if err != nil {
				var rpcerr *maelstrom.RPCError
				if errors.As(err, &rpcerr) && rpcerr.Code == maelstrom.KeyDoesNotExist {
					cur = 0
				} else {
					return err
				}
			}
			return n.Reply(msg, map[string]any{"type": "read_ok", "value": cur})
		}
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

const counterKey = "mykey"

type addReqeuest struct {
	Type  string `json:"type"`
	Delta int64  `json:"delta"`
}
