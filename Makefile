NAMESPACE := banvic
ZIP ?= banvic_data2026.zip
IMAGE := banvic-airflow:0.3.0
AIRFLOW_CHART_VERSION := 1.22.0
AIRFLOW_METADATA_HOST := airflow-postgresql.$(NAMESPACE).svc.cluster.local

.PHONY: env cluster deploy data secrets db-roles image airflow clean

# Cria o .env a partir do .env.example com credenciais aleatórias; não altera valores existentes.
env:
	@./scripts/gen-env.sh

cluster:
	mkdir -p data/raw
	sed "s|__DATA_DIR__|$(CURDIR)/data/raw|" infra/kind/kind-config.yaml | kind create cluster --config=-

# Todos os Secrets nascem do .env; nenhum valor sensível fica em arquivo versionado.
# banvic-db:      superusuário do warehouse, lido só pelo container do Postgres.
# banvic-meltano: role `meltano` (escrita no raw) com os nomes que o target-postgres espera,
#                 salt do hash LGPD e a Connection `banvic_dw` com a role `analyst` (somente leitura).
# airflow-core:   senha do banco de metadados e chaves fernet/API/JWT do Airflow.
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
	  --from-literal=TARGET_POSTGRES_USER=meltano \
	  --from-literal=TARGET_POSTGRES_PASSWORD=$$MELTANO_DB_PASSWORD \
	  --from-literal=TARGET_POSTGRES_DATABASE=$$POSTGRES_DB \
	  --from-literal=TAP_CSV_STREAM_MAP_CONFIG="{\"hash_seed\": \"$$MELTANO_HASH_SEED\"}" \
	  --from-literal=AIRFLOW_CONN_BANVIC_DW=postgres://analyst:$$ANALYST_DB_PASSWORD@$$POSTGRES_HOST:$$POSTGRES_PORT/$$POSTGRES_DB \
	  --dry-run=client -o yaml | kubectl apply -f -; \
	kubectl -n $(NAMESPACE) create secret generic airflow-core \
	  --from-literal=postgres-password=$$AIRFLOW_METADATA_DB_PASSWORD \
	  --from-literal=connection=postgresql://postgres:$$AIRFLOW_METADATA_DB_PASSWORD@$(AIRFLOW_METADATA_HOST):5432/postgres?sslmode=disable \
	  --from-literal=fernet-key=$$AIRFLOW_FERNET_KEY \
	  --from-literal=api-secret-key=$$AIRFLOW_API_SECRET_KEY \
	  --from-literal=jwt-secret=$$AIRFLOW_JWT_SECRET \
	  --dry-run=client -o yaml | kubectl apply -f -

# Roles `meltano` e `analyst` no warehouse (idempotente).
db-roles:
	@set -a; . ./.env; set +a; \
	kubectl -n $(NAMESPACE) exec -i postgres-0 -- psql -q -U $$POSTGRES_USER -d $$POSTGRES_DB \
	  -v meltano_password=$$MELTANO_DB_PASSWORD \
	  -v analyst_password=$$ANALYST_DB_PASSWORD \
	  < infra/sql/roles.sql

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
	  --set-string images.airflow.tag=$(lastword $(subst :, ,$(IMAGE))) \
	  --set-string createUserJob.defaultUser.username=$$AIRFLOW_ADMIN_USER \
	  --set-string createUserJob.defaultUser.password=$$AIRFLOW_ADMIN_PASSWORD \
	  --timeout 15m

deploy:
	kubectl apply -f infra/k8s/namespace.yaml
	$(MAKE) secrets
	kubectl apply -f infra/k8s/postgres.yaml
	kubectl -n $(NAMESPACE) rollout status statefulset/postgres --timeout=180s
	$(MAKE) db-roles
	$(MAKE) image
	$(MAKE) airflow

data:
	@test -f $(ZIP) || { echo "Arquivo $(ZIP) não encontrado. Coloque o zip na raiz do projeto ou use: make data ZIP=/caminho/do/arquivo.zip"; exit 1; }
	mkdir -p data/raw
	unzip -oj $(ZIP) -d data/raw

clean:
	kind delete cluster --name banvic
