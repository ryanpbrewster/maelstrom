package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s := newSrv(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset, err := s.send(body)
		if err != nil {
			return err
		}
		return n.Reply(msg, sendResponse{
			Type:   "send_ok",
			Offset: offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body pollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		msgs, err := s.poll(body)
		if err != nil {
			return err
		}
		return n.Reply(msg, pollResponse{
			Type: "poll_ok",
			Msgs: msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body commitOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.commitOffsets(body)
		return n.Reply(msg, commitOffsetsResponse{
			Type: "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listCommittedOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets, err := s.listCommittedOffsets(body)
		if err != nil {
			return err
		}
		return n.Reply(msg, listCommittedOffsetsResponse{
			Type:    "list_committed_offsets_ok",
			Offsets: offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type sendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type sendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type pollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type pollResponse struct {
	Type string              `json:"type"`
	Msgs map[string][]record `json:"msgs"`
}

type record struct {
	offset int
	msg    int
}

func (r record) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int{r.offset, r.msg})
}

type commitOffsetsRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type commitOffsetsResponse struct {
	Type string `json:"type"`
}

type listCommittedOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type listCommittedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type srv struct {
	kv *maelstrom.KV
}

func newSrv(n *maelstrom.Node) *srv {
	return &srv{
		kv: maelstrom.NewLinKV(n),
	}
}

func (s *srv) send(req sendRequest) (int, error) {
	offset, err := s.incrementAndGetOffer(pendingPrefix, req.Key)
	if err != nil {
		return 0, err
	}

	if err := s.appendLogEntry(req.Key, record{msg: req.Msg, offset: offset}); err != nil {
		return 0, err
	}
	return offset, nil
}

func (s *srv) poll(req pollRequest) (map[string][]record, error) {
	result := make(map[string][]record)
	for key, offset := range req.Offsets {
		for i := 0; i < 10; i++ {
			if r, err := s.readLogEntry(key, offset+i); err != nil {
				return nil, err
			} else if r != nil {
				result[key] = append(result[key], *r)
			}
		}
	}
	return result, nil
}

func (s *srv) commitOffsets(req commitOffsetsRequest) error {
	for key, offset := range req.Offsets {
		if err := s.maxOffset(committedPrefix, key, offset); err != nil {
			return err
		}
	}
	return nil
}

func (s *srv) listCommittedOffsets(req listCommittedOffsetsRequest) (map[string]int, error) {
	result := make(map[string]int)
	for _, key := range req.Keys {
		offset, ok, err := s.getOffset(committedPrefix, key)
		if err != nil {
			return nil, err
		}
		if ok {
			result[key] = offset
		}
	}
	return result, nil
}

const pendingPrefix = "pending"
const committedPrefix = "committed"

func (s *srv) incrementAndGetOffer(prefix, key string) (int, error) {
	loc := prefix + "/" + key
	for {
		cur, keyExists, err := s.getOffset(prefix, key)
		if err != nil {
			return 0, err
		}

		next := cur + 1
		if err := s.kv.CompareAndSwap(context.TODO(), loc, cur, next, !keyExists); err != nil {
			var rpcerr *maelstrom.RPCError
			if errors.As(err, &rpcerr) && rpcerr.Code == maelstrom.PreconditionFailed {
				continue
			} else {
				return 0, err
			}
		}
		return next, nil
	}
}

func (s *srv) maxOffset(prefix, key string, target int) error {
	loc := prefix + "/" + key
	for {
		cur, keyExists, err := s.getOffset(prefix, key)
		if err != nil {
			return err
		}

		next := target
		if keyExists && next < cur {
			next = cur
		}

		if err := s.kv.CompareAndSwap(context.TODO(), loc, cur, next, !keyExists); err != nil {
			var rpcerr *maelstrom.RPCError
			if errors.As(err, &rpcerr) && rpcerr.Code == maelstrom.PreconditionFailed {
				continue
			} else {
				return err
			}
		}
		return nil
	}
}

func (s *srv) getOffset(prefix, key string) (int, bool, error) {
	loc := prefix + "/" + key
	cur, err := s.kv.ReadInt(context.TODO(), loc)
	if err != nil {
		var rpcerr *maelstrom.RPCError
		if errors.As(err, &rpcerr) && rpcerr.Code == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, err
	}
	return cur, true, nil
}

const recordsPrefix = "records"

func (s *srv) appendLogEntry(key string, r record) error {
	loc := fmt.Sprintf("%s/%s/%d", recordsPrefix, key, r.offset)
	return s.kv.Write(context.TODO(), loc, r.msg)
}

func (s *srv) readLogEntry(key string, offset int) (*record, error) {
	loc := fmt.Sprintf("%s/%s/%d", recordsPrefix, key, offset)
	msg, err := s.kv.ReadInt(context.TODO(), loc)
	if err != nil {
		var rpcerr *maelstrom.RPCError
		if errors.As(err, &rpcerr) && rpcerr.Code == maelstrom.KeyDoesNotExist {
			return nil, nil
		}
		return nil, err
	}
	return &record{offset: offset, msg: msg}, nil
}
