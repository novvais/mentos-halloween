import sqlite3
import pandas as pd

conn = sqlite3.connect('database/mentos_halloween.db')

candy_data = pd.read_csv('data/candy-data.csv')
candy_crush = pd.read_csv('data/candy_crush.csv')
candy_production = pd.read_csv('data/candy_production.csv')
favorite_candy = pd.read_csv('data/FavoriteCandy.csv')

produtos = candy_data[['competitorname', 'chocolate', 'fruity', 'caramel', 'peanutyalmondy', 
                       'nougat', 'crispedricewafer', 'hard', 'bar', 'pluribus', 'sugarpercent', 
                       'pricepercent', 'winpercent']].copy()

produtos.rename(columns={
    'competitorname': 'nome',
    'chocolate': 'chocolate',
    'fruity': 'fruity',
    'caramel': 'caramel',
    'peanutyalmondy': 'peanutyalmondy',
    'nougat': 'nougat',
    'crispedricewafer': 'crispedricewafer',
    'hard': 'hard',
    'bar': 'bar',
    'pluribus': 'pluribus',
    'sugarpercent': 'sugarpercent',
    'pricepercent': 'pricepercent',
    'winpercent': 'winpercent'
}, inplace=True)

produtos['referencia_desenho'] = produtos['nome'].apply(lambda x: 'desenho' in x.lower())
produtos['sazonal'] = produtos['fruity'].apply(lambda x: x == True)

produtos.to_sql('Produtos', conn, if_exists='append', index=False)

pacotes = candy_crush[['player_id', 'level', 'num_attempts', 'num_success']].copy()

pacotes.rename(columns={'player_id': 'produto_nome', 'num_attempts': 'quantidade_balas', 'num_success': 'preco'}, inplace=True)

produto_ids = {row[1]: row[0] for row in conn.execute("SELECT id, nome FROM Produtos").fetchall()}

pacotes['produto_id'] = pacotes['produto_nome'].map(produto_ids)

if pacotes['produto_id'].isnull().any():
    print("Atenção: Alguns pacotes não puderam ser associados a produtos.")
    pacotes = pacotes.dropna(subset=['produto_id'])

pacotes.drop(columns=['produto_nome'], inplace=True)
pacotes.to_sql('Pacotes', conn, if_exists='append', index=False)

with conn:
    conn.execute("DROP TABLE IF EXISTS candy_production;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candy_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_observacao TEXT,
            quantidade_produzida REAL
        );
    """)

if 'observation_date' in candy_production.columns and 'IPG3113N' in candy_production.columns:
    candy_production = candy_production[['observation_date', 'IPG3113N']].copy()
    candy_production.rename(columns={
        'observation_date': 'data_observacao',
        'IPG3113N': 'quantidade_produzida'
    }, inplace=True)

    candy_production.to_sql('candy_production', conn, if_exists='append', index=False)
else:
    print("Erro: O arquivo 'candy_production.csv' não contém as colunas esperadas.")

favorite_candy_clean = favorite_candy[['Timestamp', 'Score', 'Snickers', 'KitKat', 'Twix', 'Ghirardelli Squares', 
                                      'Reeses', 'JollyRancher', 'Twizzlers/ Red Vines', 'Lollipops', 'SourPatch', 
                                      'Skittles', 'Apples / Fruits / Floss', 'M&M\'s']].copy()

favorite_candy_clean = favorite_candy_clean.melt(id_vars=['Timestamp', 'Score'], value_vars=favorite_candy_clean.columns[2:],
                                                var_name='produto_nome', value_name='preferencia_nivel')

favorite_candy_clean['produto_nome'] = favorite_candy_clean['produto_nome'].str.strip()  

favorite_candy_clean.to_sql('favorite_candy', conn, if_exists='append', index=False)

print("Todos os dados foram inseridos com sucesso!")

conn.close()
