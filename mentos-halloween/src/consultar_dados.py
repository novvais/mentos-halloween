import sqlite3
import pandas as pd

conn = sqlite3.connect('database/mentos_halloween.db')

with conn:
    conn.execute("DROP TABLE IF EXISTS candy_production;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candy_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            produto_nome TEXT,
            quantidade_produzida INTEGER,
            mes TEXT,
            ano INTEGER
        );
    """)

candy_production = pd.read_csv('data/candy_production.csv')

candy_production.rename(columns={
    'observation_date': 'produto_nome',  
    'IPG3113N': 'quantidade_produzida',
}, inplace=True)

candy_production.to_sql('candy_production', conn, if_exists='append', index=False)

def executar_consulta(query, descricao):
    print(f"\n{descricao}")
    resultado = conn.execute(query).fetchall()
    
    if resultado:
        for row in resultado:
            for i, value in enumerate(row):
                print(f"Coluna {i+1}: {value}")
            print("-" * 50)
    else:
        print("Nenhum resultado encontrado.")
        print("-" * 50)

executar_consulta(
    "SELECT COUNT(*) AS total_referencia_desenho FROM Produtos WHERE referencia_desenho = 1;",
    "1. Quantos produtos possuem formato com referência a desenhos?"
)

executar_consulta(
    "SELECT COUNT(*) AS total_sazonal FROM Produtos WHERE sazonal = 1 AND fruity = 1;",
    "2. Quantos produtos possuem sabor de frutas sazonais?"
)

executar_consulta(
    "SELECT COUNT(*) AS total_multibalas FROM Pacotes WHERE quantidade_balas > 1;",
    "3. Quantos pacotes possuem mais de uma bala?"
)

executar_consulta(
    """
    SELECT ano, SUM(quantidade_produzida) AS total_produzido
    FROM candy_production
    GROUP BY ano
    ORDER BY ano DESC;
    """,
    "4. Produção total por ano:"
)

executar_consulta(
    """
    SELECT produto_nome, COUNT(*) AS total_favoritos
    FROM favorite_candy
    GROUP BY produto_nome
    ORDER BY total_favoritos DESC
    LIMIT 5;
    """,
    "5. Top 5 doces mais favoritos:"
)

conn.close()
