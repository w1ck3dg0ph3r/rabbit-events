default:
	@echo "noop"

test: test-unit test-integration

test-unit:
	go test -v ./...

test-integration:
	go test -tags integration -v ./...

docs:
	GOPATH=$(pwd) GOMOD="$(pwd)/go.mod" godoc -http=:6060