import sqlite3

conn = sqlite3.connect('database/mentos_halloween.db')

with conn:
    conn.execute("DROP TABLE IF EXISTS favorite_candy;")

    conn.executescript("""
        -- Tabela Produtos
        CREATE TABLE IF NOT EXISTS Produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            chocolate BOOLEAN,
            fruity BOOLEAN,
            caramel BOOLEAN,
            peanutyalmondy BOOLEAN,
            nougat BOOLEAN,
            crispedricewafer BOOLEAN,
            hard BOOLEAN,
            bar BOOLEAN,
            pluribus BOOLEAN,
            sugarpercent REAL,
            pricepercent REAL,
            winpercent REAL,
            referencia_desenho BOOLEAN DEFAULT FALSE,
            sazonal BOOLEAN DEFAULT FALSE
        );

        -- Tabela Pacotes
        CREATE TABLE IF NOT EXISTS Pacotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            produto_id INTEGER NOT NULL,
            quantidade_balas INTEGER,
            preco REAL,
            FOREIGN KEY (produto_id) REFERENCES Produtos(id)
        );

        -- Tabela de Produção de Doces
        CREATE TABLE IF NOT EXISTS candy_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            produto_nome TEXT,
            quantidade_produzida INTEGER,
            mes TEXT,
            ano INTEGER
        );

        -- Tabela de Doces Favoritos
        CREATE TABLE IF NOT EXISTS favorite_candy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pessoa_id INTEGER,
            produto_nome TEXT,
            preferencia_nivel INTEGER,
            timestamp TEXT,        -- Adicionada a coluna Timestamp
            score INTEGER          -- Adicionada a coluna Score
        );
    """)

print("Banco de dados e tabelas criados com sucesso!")

conn.close()
