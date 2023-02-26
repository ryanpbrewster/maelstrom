package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s := newSrv()

	n.Handle("send", func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset := s.send(body)
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
		return n.Reply(msg, pollResponse{
			Type: "poll_ok",
			Msgs: s.poll(body),
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
		return n.Reply(msg, listCommittedOffsetsResponse{
			Type:    "list_committed_offsets_ok",
			Offsets: s.listCommittedOffsets(body),
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
	mu        sync.Mutex
	topics    map[string][]record
	committed map[string]int
}

func newSrv() *srv {
	return &srv{
		topics:    make(map[string][]record),
		committed: make(map[string]int),
	}
}

func (s *srv) send(req sendRequest) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic := s.topics[req.Key]
	offset := 0
	if len(topic) > 0 {
		offset = topic[len(topic)-1].offset + 1
	}
	s.topics[req.Key] = append(topic, record{offset: offset, msg: req.Msg})
	return offset
}

func (s *srv) poll(req pollRequest) map[string][]record {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string][]record)
	for key, offset := range req.Offsets {
		for _, record := range s.topics[key] {
			if record.offset >= offset {
				result[key] = append(result[key], record)
			}
		}
	}
	return result
}

func (s *srv) commitOffsets(req commitOffsetsRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, offset := range req.Offsets {
		if cur, ok := s.committed[key]; ok && cur > offset {
			continue
		}
		s.committed[key] = offset
	}
}

func (s *srv) listCommittedOffsets(req listCommittedOffsetsRequest) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]int)
	for _, key := range req.Keys {
		if offset, ok := s.committed[key]; ok {
			result[key] = offset
		}
	}
	return result
}
