inspect:
	cd maelstrom && ./maelstrom serve

echo:
	cd echo && go build main.go
	cd maelstrom && ./maelstrom test -w echo --bin ../echo/main --node-count 1 --time-limit 10

unique-ids:
	cd unique-ids && go build main.go
	cd maelstrom && ./maelstrom test -w unique-ids --bin ../unique-ids/main --time-limit 10 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast-single:
	cd broadcast && go build main.go
	cd maelstrom && ./maelstrom test -w broadcast --bin ../broadcast/main --node-count 1 --time-limit 20 --rate 10

broadcast-multi:
	cd broadcast && go build main.go
	cd maelstrom && ./maelstrom test -w broadcast --bin ../broadcast/main --node-count 3 --time-limit 20 --rate 10

broadcast-faulty:
	cd broadcast && go build main.go
	cd maelstrom && ./maelstrom test -w broadcast --bin ../broadcast/main --node-count 5 --time-limit 20 --rate 10 --nemesis partition

broadcast-lag:
	cd broadcast && go build main.go
	cd maelstrom && ./maelstrom test -w broadcast --bin ../broadcast/main --node-count 25 --time-limit 20 --rate 100 --latency 100

g-counter:
	cd g-counter && go build main.go
	cd maelstrom && ./maelstrom test -w g-counter --bin ../g-counter/main --node-count 3 --rate 100 --time-limit 10 --nemesis partition

kafka-single:
	cd kafka && go build main.go
	cd maelstrom && ./maelstrom test -w kafka --bin ../kafka/main --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

kafka-multi:
	cd kafka && go build main.go
	cd maelstrom && ./maelstrom test -w kafka --bin ../kafka/main --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
