.PHONY: all apiserver bucketserver client
all: apiserver bucketserver client

generate-proto:
	protoc --go_out=./internal/proto/ --go-grpc_out=./internal/proto/ ./internal/proto/bucket.proto

generate-test-file:
	dd of=test-file-src.bin bs=100M count=1 if=/dev/urandom

apiserver:
	go build -o bin/apiserver ./cmd/apiserver/

bucketserver:
	go build -o bin/bucketserver ./cmd/bucketserver/

client:
	go build -o bin/client ./cmd/client/

update-deps: 
	go get -u ./...
	go mod tidy

up:
	docker-compose -p docker-compose.yml up -d

down:
	docker-compose -p docker-compose.yml down

images:
	docker build -t karma8-apiserver -f ./cmd/apiserver/Dockerfile .
	docker build -t karma8-bucketserver -f ./cmd/bucketserver/Dockerfile .

