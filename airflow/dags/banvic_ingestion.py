"""DAG de ingestão do ERP do BanVic: valida arquivos, executa o Meltano e confere a carga."""
import csv
import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, task

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("BANVIC_DATA_DIR", "/data/raw")
WAREHOUSE_CONN_ID = "banvic_dw"
RAW_SCHEMA = "raw"

# Tabela -> chave primária. Espelha as `keys` do meltano.yml e é usada na validação pós-carga.
TABLES = {
    "agencias": ["cod_agencia"],
    "clientes": ["cod_cliente"],
    "colaborador_agencia": ["cod_colaborador", "cod_agencia"],
    "colaboradores": ["cod_colaborador"],
    "contas": ["num_conta"],
    "propostas_credito": ["cod_proposta"],
    "transacoes": ["cod_transacao"],
}


def notify_failure(context):
    """Monitoramento básico: registra a falha em log estruturado.

    Em produção, este é o ponto para enviar alerta (Slack, e-mail, PagerDuty).
    """
    ti = context["ti"]
    log.error(
        "Falha na ingestão BanVic: dag=%s task=%s run=%s tentativa=%s erro=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
        context.get("exception"),
    )


default_args = {
    "owner": "dados",
    # Falhas transitórias (banco reiniciando, rede): até 3 novas tentativas,
    # com espera dobrando a cada uma (2, 4, 8 min) e teto de 10 min.
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=10),
    # Nenhuma task pode segurar o slot indefinidamente.
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="banvic_ingestion",
    description="CSV do ERP -> Postgres (raw) via Meltano",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    # O ERP exporta uma vez por dia e o dashboard comercial não exige tempo real.
    schedule="@daily",
    # Não reprocessa os dias anteriores ao deploy; a carga é full table de qualquer forma.
    catchup=False,
    # Evita duas cargas concorrentes escrevendo nas mesmas tabelas.
    max_active_runs=1,
    default_args=default_args,
    tags=["banvic", "ingestao"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Um sensor por arquivo garante que a fonte existe antes de ingerir.
    # mode="reschedule" libera o slot entre as verificações em vez de ficar bloqueado.
    wait_files = [
        FileSensor(
            task_id=f"wait_{table}",
            filepath=f"{DATA_DIR}/{table}.csv",
            fs_conn_id="fs_default",
            poke_interval=30,
            timeout=60 * 10,
            mode="reschedule",
        )
        for table in TABLES
    ]

    # BashOperator na própria imagem do Airflow, que já traz o Meltano e os plugins
    # instalados no build: menos peças móveis que um KubernetesPodOperator neste ambiente local.
    # Credenciais chegam por TARGET_POSTGRES_* (Secret), nunca pelo comando.
    ingest = BashOperator(
        task_id="meltano_ingest",
        bash_command="cd /opt/airflow/meltano && meltano run ingest_erp",
    )

    # Sem retries: divergência de contagem é determinística, tentar de novo só atrasaria o alerta.
    @task(retries=0)
    def validate_load(table: str) -> dict:
        """Compara a tabela carregada com o CSV de origem e checa a chave primária."""
        keys = TABLES[table]
        with open(f"{DATA_DIR}/{table}.csv", newline="", encoding="utf-8") as f:
            source_rows = sum(1 for _ in csv.DictReader(f))

        key_cols = ", ".join(keys)
        null_filter = " or ".join(f"{k} is null" for k in keys)
        sql = f"""
            select
                count(*),
                count(*) filter (where {null_filter}),
                count(distinct ({key_cols}))
            from {RAW_SCHEMA}.{table}
        """
        loaded_rows, null_keys, distinct_keys = PostgresHook(
            postgres_conn_id=WAREHOUSE_CONN_ID
        ).get_first(sql)

        result = {
            "table": table,
            "source_rows": source_rows,
            "loaded_rows": loaded_rows,
            "null_keys": null_keys,
            "duplicate_keys": loaded_rows - distinct_keys,
        }
        log.info("Validação %s", result)

        problems = []
        if loaded_rows != source_rows:
            problems.append(f"{loaded_rows} linhas no Postgres vs {source_rows} no CSV")
        if null_keys:
            problems.append(f"{null_keys} linhas com chave nula")
        if result["duplicate_keys"]:
            problems.append(f"{result['duplicate_keys']} chaves duplicadas")
        if problems:
            raise ValueError(f"Validação de {table} falhou: " + "; ".join(problems))
        return result

    # Uma task por tabela (dynamic task mapping): a UI mostra exatamente qual tabela divergiu.
    validate = validate_load.expand(table=list(TABLES))

    end = EmptyOperator(task_id="end")

    start >> wait_files >> ingest >> validate >> end
