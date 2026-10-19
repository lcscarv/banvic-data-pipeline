NAMESPACE := banvic
ZIP ?= ../banvic_data2026.zip

.PHONY: cluster deploy data secrets clean

cluster:
	kind create cluster --config infra/kind/kind-config.yaml

secrets:
	@set -a; . ./.env; set +a; \
	kubectl -n $(NAMESPACE) create secret generic banvic-db \
	  --from-literal=POSTGRES_USER=$$POSTGRES_USER \
	  --from-literal=POSTGRES_PASSWORD=$$POSTGRES_PASSWORD \
	  --from-literal=POSTGRES_DB=$$POSTGRES_DB \
	  --dry-run=client -o yaml | kubectl apply -f -

deploy:
	kubectl apply -f infra/k8s/namespace.yaml
	$(MAKE) secrets
	kubectl apply -f infra/k8s/postgres.yaml
	# TODO(você): instalar o Airflow via helm (chart oficial apache-airflow) com infra/k8s/airflow-values.yaml

data:
	mkdir -p data/raw
	unzip -oj $(ZIP) -d data/raw

clean:
	kind delete cluster --name banvic
