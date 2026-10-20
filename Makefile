NAMESPACE := banvic
ZIP ?= ../banvic_data2026.zip
IMAGE := banvic-airflow:0.2.0
AIRFLOW_CHART_VERSION := 1.22.0

.PHONY: cluster deploy data secrets image airflow clean

cluster:
	mkdir -p data/raw
	sed "s|__DATA_DIR__|$(CURDIR)/data/raw|" infra/kind/kind-config.yaml | kind create cluster --config=-

# banvic-db: lido pelo container do Postgres.
# banvic-meltano: mesmas credenciais com os nomes que o target-postgres espera,
#   mais a Airflow Connection `banvic_dw` (AIRFLOW_CONN_*) usada na validação pós-carga.
secrets:
	@set -a; . ./.env; set +a; \
	kubectl -n $(NAMESPACE) create secret generic banvic-db \
	  --from-literal=POSTGRES_USER=$$POSTGRES_USER \
	  --from-literal=POSTGRES_PASSWORD=$$POSTGRES_PASSWORD \
	  --from-literal=POSTGRES_DB=$$POSTGRES_DB \
	  --dry-run=client -o yaml | kubectl apply -f -; \
	kubectl -n $(NAMESPACE) create secret generic banvic-meltano \
	  --from-literal=TARGET_POSTGRES_HOST=$$POSTGRES_HOST \
	  --from-literal=TARGET_POSTGRES_PORT=$$POSTGRES_PORT \
	  --from-literal=TARGET_POSTGRES_USER=$$POSTGRES_USER \
	  --from-literal=TARGET_POSTGRES_PASSWORD=$$POSTGRES_PASSWORD \
	  --from-literal=TARGET_POSTGRES_DATABASE=$$POSTGRES_DB \
	  --from-literal=AIRFLOW_CONN_BANVIC_DW=postgres://$$POSTGRES_USER:$$POSTGRES_PASSWORD@$$POSTGRES_HOST:$$POSTGRES_PORT/$$POSTGRES_DB \
	  --dry-run=client -o yaml | kubectl apply -f -

image:
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name banvic

airflow:
	helm repo add apache-airflow https://airflow.apache.org --force-update >/dev/null
	@set -a; . ./.env; set +a; \
	helm upgrade --install airflow apache-airflow/airflow \
	  --version $(AIRFLOW_CHART_VERSION) \
	  --namespace $(NAMESPACE) \
	  -f infra/k8s/airflow-values.yaml \
	  --set-string createUserJob.defaultUser.username=$$AIRFLOW_ADMIN_USER \
	  --set-string createUserJob.defaultUser.password=$$AIRFLOW_ADMIN_PASSWORD \
	  --timeout 15m

deploy:
	kubectl apply -f infra/k8s/namespace.yaml
	$(MAKE) secrets
	kubectl apply -f infra/k8s/postgres.yaml
	kubectl -n $(NAMESPACE) rollout status statefulset/postgres --timeout=180s
	$(MAKE) image
	$(MAKE) airflow

data:
	mkdir -p data/raw
	unzip -oj $(ZIP) -d data/raw

clean:
	kind delete cluster --name banvic
