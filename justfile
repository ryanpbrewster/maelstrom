echo:
	cd echo && go build main.go
	cd maelstrom && ./maelstrom test -w echo --bin ../echo/main --node-count 1 --time-limit 10
