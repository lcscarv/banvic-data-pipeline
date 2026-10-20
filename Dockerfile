# Imagem do Airflow com o Meltano e o projeto de ingestão embutidos.
# Construída por `make image` e carregada no kind (sem registry).
FROM apache/airflow:3.2.2-python3.11

# git é necessário para instalar o tap-csv, que vem direto do GitHub.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Meltano em um venv próprio para não conflitar com as dependências do Airflow.
# Vai para o fim do PATH, para o python do Airflow continuar sendo o padrão.
RUN python -m venv /opt/airflow/meltano-venv \
    && /opt/airflow/meltano-venv/bin/pip install --no-cache-dir meltano==4.4.1
ENV PATH="${PATH}:/opt/airflow/meltano-venv/bin" \
    MELTANO_SEND_ANONYMOUS_USAGE_STATS=false

# Plugins instalados no build: a execução não depende de rede nem de versões novas.
COPY --chown=airflow:root meltano/ /opt/airflow/meltano/
RUN cd /opt/airflow/meltano && meltano install

# DAGs embutidas na imagem: cada tag é um snapshot coerente de código e dependências.
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
