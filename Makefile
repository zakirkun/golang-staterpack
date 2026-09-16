.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- local development --------------------------------------------------------

.PHONY: setup
setup: ## Copy .env.example to .env if missing
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env"; else echo ".env exists"; fi

.PHONY: run
run: ## Run the API locally
	go run ./cmd/api

.PHONY: build
build: ## Build the binary into bin/api
	go build -trimpath -o bin/api ./cmd/api

.PHONY: tidy
tidy: ## Sync go.mod / go.sum
	go mod tidy

.PHONY: fmt
fmt: ## Format all Go source
	gofmt -w .

.PHONY: lint
lint: ## Run go vet
	go vet ./...

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-cover
test-cover: ## Run tests and write coverage.txt
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...

.PHONY: check
check: fmt lint test ## Format, vet and test

.PHONY: check-manifests
check-manifests: ## Validate k8s YAML parses and config keys match the code
	python scripts/validate_yaml.py
	python scripts/check_env_keys.py

.PHONY: check-all
check-all: check check-manifests ## Everything

# --- docker -------------------------------------------------------------------

.PHONY: up
up: ## Start the full stack in the background
	docker compose up -d --build

.PHONY: down
down: ## Stop the stack
	docker compose down

.PHONY: clean
clean: ## Stop the stack and delete volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail API logs
	docker compose logs -f api

.PHONY: ps
ps: ## Show running services
	docker compose ps

# --- kubernetes ---------------------------------------------------------------

.PHONY: k8s-apply
k8s-apply: ## Apply the configmap/secret/deployment manifests
	kubectl apply -f deploy/k8s/00-namespace.yaml
	kubectl apply -f deploy/k8s/01-configmap.yaml
	kubectl apply -f deploy/k8s/02-secret.yaml
	kubectl apply -f deploy/k8s/10-deployment.yaml
	kubectl apply -f deploy/k8s/11-service.yaml

.PHONY: k8s-migrate
k8s-migrate: ## Run the schema migration Job and wait for completion
	kubectl apply -f deploy/k8s/20-migrate-job.yaml
	kubectl wait --for=condition=complete job/api-migrate -n staterpack --timeout=120s

.PHONY: k8s-delete
k8s-delete: ## Delete all app resources
	kubectl delete -f deploy/k8s/ --ignore-not-found
