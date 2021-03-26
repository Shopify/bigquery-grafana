.PHONY: build-go build-node release run-go run-node build dev

dev: build run
	@echo " > building and running..."

build-go:
	env GOOS=linux GOARCH=amd64 go build -o dist/doitintl-bigquery-datasource_linux_amd64 ./cmd/query/main.go 

build-node:
	yarn run build:dev

run:
	docker run -it -v $(PWD)/dist:/var/lib/grafana/plugins/bq-plugin -v $(PWD)/data:/var/lib/grafana:cached -p 3000:3000 grafana-bq

run-go: build-go run
	@echo " > run-go..."

run-node: build-node run
	@echo " > run-node..."

build: build-go build-node
	@echo " > building..."

release:
	./build-dist.sh
