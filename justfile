inspect:
	cd maelstrom && ./maelstrom serve

echo:
	cd echo && go build main.go
	cd maelstrom && ./maelstrom test -w echo --bin ../echo/main --node-count 1 --time-limit 10

unique-ids:
	cd unique-ids && go build main.go
	cd maelstrom && ./maelstrom test -w unique-ids --bin ../unique-ids/main --time-limit 10 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast:
	cd broadcast && go build main.go
	cd maelstrom && ./maelstrom test -w broadcast --bin ../broadcast/main --node-count 1 --time-limit 20 --rate 10
