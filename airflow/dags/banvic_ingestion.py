"""DAG de ingestão do ERP do BanVic: valida arquivos, executa o Meltano e confere a carga."""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

DATA_DIR = "/data/raw"
TABLES = [
    "agencias",
    "clientes",
    "colaborador_agencia",
    "colaboradores",
    "contas",
    "propostas_credito",
    "transacoes",
]

default_args = {
    "owner": "dados",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    # TODO(você): retry_exponential_backoff, execution_timeout, on_failure_callback (monitoramento)
}

with DAG(
    dag_id="banvic_ingestion",
    description="CSV do ERP -> Postgres (raw) via Meltano",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # TODO(você): qual agenda faz sentido? Justifique (ex.: @daily)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["banvic", "ingestao"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Um sensor por arquivo garante que a fonte existe antes de ingerir.
    wait_files = [
        FileSensor(
            task_id=f"wait_{table}",
            filepath=f"{DATA_DIR}/{table}.csv",
            poke_interval=30,
            timeout=60 * 10,
            mode="reschedule",
        )
        for table in TABLES
    ]

    # TODO(você): rodar o Meltano. Opções: BashOperator (imagem Airflow com Meltano)
    # ou KubernetesPodOperator (imagem do Meltano). Qual escolheu e por quê?
    ingest = BashOperator(
        task_id="meltano_ingest",
        bash_command="cd /opt/airflow/meltano && meltano run tap-csv target-postgres",
    )

    # TODO(você): validação pós-carga (contagem de linhas por tabela vs. CSV, sem nulos em chaves)
    validate = EmptyOperator(task_id="validate_load")

    end = EmptyOperator(task_id="end")

    start >> wait_files >> ingest >> validate >> end
