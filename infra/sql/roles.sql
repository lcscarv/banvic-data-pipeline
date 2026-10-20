-- Roles do warehouse com menor privilégio. Idempotente: pode rodar a cada deploy.
-- Executado por `make db-roles`, que passa as senhas como variáveis do psql (vindas do .env).
\set ON_ERROR_STOP on

-- meltano: carrega dados. Dono do schema raw, sem acesso a nada fora dele.
SELECT format('CREATE ROLE meltano LOGIN PASSWORD %L', :'meltano_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'meltano') \gexec
ALTER ROLE meltano PASSWORD :'meltano_password';

-- analyst: leitura do raw. Usado pelos analistas e pela validação pós-carga da DAG.
SELECT format('CREATE ROLE analyst LOGIN PASSWORD %L', :'analyst_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst') \gexec
ALTER ROLE analyst PASSWORD :'analyst_password';

SELECT format('GRANT CONNECT ON DATABASE %I TO meltano, analyst', current_database()) \gexec

CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION meltano;
ALTER SCHEMA raw OWNER TO meltano;

-- Tabelas criadas antes desta configuração passam a pertencer ao meltano (o upsert precisa ser dono).
DO $$
DECLARE t record;
BEGIN
  FOR t IN SELECT tablename FROM pg_tables WHERE schemaname = 'raw' LOOP
    EXECUTE format('ALTER TABLE raw.%I OWNER TO meltano', t.tablename);
  END LOOP;
END $$;

GRANT USAGE ON SCHEMA raw TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO analyst;
-- Tabelas que o meltano criar no futuro já nascem legíveis pelo analyst.
ALTER DEFAULT PRIVILEGES FOR ROLE meltano IN SCHEMA raw GRANT SELECT ON TABLES TO analyst;
