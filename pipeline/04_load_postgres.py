"""
04_load_postgres.py
--------------------
Carrega os dados limpos no PostgreSQL.

PRÉ-REQUISITOS:
    1. PostgreSQL rodando (local ou Docker)
    2. Banco criado: createdb ritalinagap
    3. Schema aplicado: psql -U postgres -d ritalinagap -f sql/schema.sql
    4. Arquivo .env configurado (veja abaixo)

ARQUIVO .env na raiz do projeto:
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=ritalinagap
    DB_USER=postgres
    DB_PASSWORD=sua_senha

PARA RODAR:
    python pipeline/04_load_postgres.py
"""

import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"

load_dotenv(r"C:\Users\kruge\Desktop\ritalina_gap\.env", override=True)
print(f"DEBUG senha: {os.getenv('DB_PASSWORD')}")


def get_engine():
    """Cria conexão com PostgreSQL via SQLAlchemy."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "ritalinagap")
    user = os.getenv("DB_USER", "postgres")
    pwd  = os.getenv("DB_PASSWORD", "postgres")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
    engine = create_engine(url, echo=False)
    return engine


def carregar_tabela(engine, arquivo: Path, tabela: str, truncate: bool = True):
    """Carrega um CSV para uma tabela PostgreSQL."""
    if not arquivo.exists():
        print(f"  ⚠️  Arquivo não encontrado: {arquivo}")
        return

    df = pd.read_csv(arquivo)
    print(f"  Carregando {len(df):,} registros em '{tabela}'...")

    with engine.begin() as conn:
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {tabela} RESTART IDENTITY CASCADE"))

    df.to_sql(
        tabela,
        engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )
    print(f"  ✓ {tabela} carregada com sucesso")


def verificar_carga(engine):
    """Verifica contagens após a carga."""
    print("\n→ Verificando carga...")
    tabelas = ["dispensacao_metilfenidato", "atendimentos_tdah"]

    with engine.connect() as conn:
        for tabela in tabelas:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {tabela}"))
                count = result.scalar()
                print(f"  {tabela}: {count:,} registros")
            except Exception as e:
                print(f"  ✗ Erro em {tabela}: {e}")

        # Preview do gap via view
        try:
            result = conn.execute(text("""
                SELECT ano, mes, uf,
                       unidades_dispensadas,
                       atendimentos_tdah,
                       unidades_por_atendimento
                FROM vw_gap_mensal
                WHERE unidades_por_atendimento IS NOT NULL
                ORDER BY unidades_por_atendimento DESC
                LIMIT 5
            """))
            rows = result.fetchall()
            print(f"\n  Top 5 UF/mês por unidades por atendimento:")
            print(f"  {'ANO':>4} {'MES':>3} {'UF':>3} {'UNIDADES':>10} {'ATEND':>8} {'RATIO':>8}")
            for row in rows:
                print(f"  {row[0]:>4} {row[1]:>3} {row[2]:>3} {row[3]:>10,.0f} {row[4]:>8,.0f} {row[5]:>8.1f}")
        except Exception as e:
            print(f"  ✗ Erro ao consultar view: {e}")


def main():
    print("=" * 60)
    print("CARGA NO POSTGRESQL")
    print("=" * 60)

    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Conexão com PostgreSQL estabelecida")
    except Exception as e:
        print(f"✗ Não foi possível conectar ao PostgreSQL: {e}")
        print("\nVerifique seu arquivo .env:")
        print("  DB_HOST=localhost")
        print("  DB_PORT=5432")
        print("  DB_NAME=ritalinagap")
        print("  DB_USER=postgres")
        print("  DB_PASSWORD=sua_senha")
        print("\nE certifique-se que o schema foi aplicado:")
        print("  psql -U postgres -d ritalinagap -f sql/schema.sql")
        return

    carregar_tabela(
        engine,
        PROCESSED_DIR / "metilfenidato_limpo.csv",
        "dispensacao_metilfenidato"
    )

    carregar_tabela(
        engine,
        PROCESSED_DIR / "atendimentos_tdah_limpo.csv",
        "atendimentos_tdah"
    )

    verificar_carga(engine)
    print("\n✓ Carga concluída! Agora configure o Superset apontando para este banco.")


if __name__ == "__main__":
    main()
