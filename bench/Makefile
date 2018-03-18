all: write write-large write-hurry write-large-hurry write-quick write-large-quick

write:
	go test -bench PutSmall -benchtime 20s
write-large:
	go test -bench PutLarge -benchtime 20s
write-hurry:
	go test -bench PutSmall -benchtime 10s
write-large-hurry:
	go test -bench PutLarge -benchtime 10s
write-quick:
	go test -bench PutSmall -benchtime 5s
write-large-quick:
	go test -bench PutLarge -benchtime 5s
read:
	go test -bench GetSmall -benchtime 10s
read-large:
	go test -bench GetLarge -benchtime 10s

clean:
	rm *.wdb
