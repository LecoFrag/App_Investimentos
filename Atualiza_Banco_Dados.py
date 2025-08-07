import os
import pandas as pd
import yfinance as yf
import sqlite3
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import requests
from pandas.tseries.offsets import MonthEnd


# ========== CONFIGURAÇÕES ==========
DB_PATH = 'dados_investimentos.db'
CSV_TICKERS_ACOES = 'acoes_listadas_b3.csv'
EXCEL_TICKERS_FII = 'fundos_listados.xlsx'
CSV_DADOS_FUNDAMENTAIS = 'dados_fundamentais_b3.csv'
COLUNA_TICKERS_FII = 'Código'

# ========== CONEXÃO ==========
engine = create_engine(f'sqlite:///' + DB_PATH)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# ========== CRIA TABELAS ==========
def criar_tabelas():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dados_historicos (
        date TEXT,
        ticker TEXT,
        close REAL,
        adj_close REAL,
        dividends REAL,
        high REAL,
        low REAL,
        open REAL,
        stock_splits REAL,
        volume REAL,
        market_cap REAL,
        enterprise_value REAL,
        price_to_book REAL,
        trailing_pe REAL,
        dividend_yield REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dados_fii (
        date TEXT,
        ticker TEXT,
        close REAL,
        dividend_yield REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dados_fundamentais (
        ticker TEXT,
        nome TEXT,
        setor TEXT,
        industria TEXT,
        pais TEXT,
        site TEXT,
        funcionarios INTEGER,
        payout_ratio REAL,
        receita REAL,
        lucro_bruto REAL,
        ebitda REAL,
        margem_lucro REAL,
        caixa_total REAL,
        divida_total REAL,
        roe REAL,
        roa REAL,
        liquidez_corrente REAL,
        beta REAL
    )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dados_indicadores (
            data TEXT PRIMARY KEY,
            cdi REAL,
            ipca REAL,
            ibov REAL
        )
        """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS carteiras (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT,
            ticker TEXT,
            quantidade INTEGER,
            valor_compra REAL,
            data_compra TEXT
        )
        """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dados_noticias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_publicacao TEXT,
        fonte TEXT,
        titulo TEXT,
        conteudo_resumido TEXT,
        ticker TEXT,
        sentimento_score REAL
    )
    """)

    conn.commit()

# ========== FUNÇÕES DE ATUALIZAÇÃO ==========
def ultima_data_global(tabela):
    query = f"SELECT MAX(date) FROM {tabela}"
    result = cursor.execute(query).fetchone()
    return result[0] if result and result[0] else "1990-01-01"

def ultima_data_indicador():
    cursor.execute("SELECT MAX(data) FROM dados_indicadores")
    resultado = cursor.fetchone()
    return resultado[0] if resultado and resultado[0] else "2000-01-01"

def normalizar_colunas(df):
    if isinstance(df.columns[0], tuple):
        df.columns = [col[0].lower().replace(" ", "_") for col in df.columns]
    else:
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df

def atualizar_historicos(df_tickers, data_inicio, data_fim):
    dfs = []
    total = len(df_tickers)
    for i, ticker in enumerate(df_tickers, 1):
        ticker_yf = ticker + ".SA"
        df = yf.download(ticker_yf, start=data_inicio, end=data_fim, auto_adjust=False, actions=True, progress=False)
        if df.empty:
            continue

        df.reset_index(inplace=True)
        df = normalizar_colunas(df)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['ticker'] = ticker

        try:
            info = yf.Ticker(ticker_yf).info
            df['market_cap'] = info.get('marketCap')
            df['enterprise_value'] = info.get('enterpriseValue')
            df['price_to_book'] = info.get('priceToBook')
            df['trailing_pe'] = info.get('trailingPE')
            df['dividend_yield'] = info.get('dividendYield')
        except:
            df['market_cap'] = df['enterprise_value'] = df['price_to_book'] = df['trailing_pe'] = df['dividend_yield'] = None

        colunas = ['date', 'ticker', 'close', 'adj_close', 'dividends', 'high', 'low', 'open', 'stock_splits',
                   'volume', 'market_cap', 'enterprise_value', 'price_to_book', 'trailing_pe', 'dividend_yield']
        df = df[[col for col in colunas if col in df.columns]]
        dfs.append(df)
        print(f"📊 Atualizando dados históricos: {i} de {total}", end='\r')

    if dfs:
        df_total = pd.concat(dfs, ignore_index=True)
        df_total.to_sql('dados_historicos', con=engine, index=False, if_exists='append')

    print("\n✅ Concluída atualização de dados históricos.")

def atualizar_fiis(df_tickers, data_inicio, data_fim):
    dfs = []
    total = len(df_tickers)
    for i, ticker in enumerate(df_tickers, 1):
        ticker_yf = ticker + ".SA"
        df = yf.download(ticker_yf, start=data_inicio, end=data_fim, auto_adjust=False, actions=True, progress=False)
        if df.empty:
            continue

        df.reset_index(inplace=True)
        df = normalizar_colunas(df)
        df['date'] = pd.to_datetime(df['date']).dt.date

        try:
            info = yf.Ticker(ticker_yf).info
            dy = info.get('dividendYield', None)
        except:
            dy = None

        df_final = pd.DataFrame()
        df_final['date'] = df['date']
        df_final['ticker'] = ticker
        df_final['close'] = df['close']
        df_final['dividend_yield'] = dy

        dfs.append(df_final)
        print(f"🏢 Atualizando dados FII: {i} de {total}", end='\r')

    if dfs:
        df_total = pd.concat(dfs, ignore_index=True)
        df_total.to_sql('dados_fii', con=engine, index=False, if_exists='append')

    print("\n✅ Concluída atualização de dados de FIIs.")

def importar_dados_fundamentais():
    if not os.path.exists(CSV_DADOS_FUNDAMENTAIS):
        print("⚠️ Arquivo de dados fundamentais não encontrado.")
        return
    df_fund = pd.read_csv(CSV_DADOS_FUNDAMENTAIS)
    df_fund.to_sql('dados_fundamentais', con=engine, index=False, if_exists='replace')

# ========== CONSULTA API BCB ==========
def consultar_bcb(codigo_serie, data_ini="01/01/2000", data_fim=None):
    if data_fim is None:
        data_fim = pd.Timestamp.today() + pd.DateOffset(days=1)  # inclui hoje
    else:
        data_fim = pd.to_datetime(data_fim, dayfirst=True)

    data_ini = pd.to_datetime(data_ini, dayfirst=True)
    headers = {"User-Agent": "Mozilla/5.0"}
    df_total = pd.DataFrame()

    while data_ini < data_fim:
        data_limite = min(data_ini + pd.DateOffset(years=10), data_fim)
        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados"
            f"?formato=json&dataInicial={data_ini.strftime('%d/%m/%Y')}"
            f"&dataFinal={data_limite.strftime('%d/%m/%Y')}"
        )
        r = requests.get(url, headers=headers)

        try:
            dados = r.json()
        except ValueError:
            raise ValueError(f"Erro ao interpretar JSON. Status {r.status_code}. Conteúdo: {r.text[:200]}")

        if isinstance(dados, list) and dados:
            df = pd.DataFrame(dados)
            df['data'] = pd.to_datetime(df['data'], dayfirst=True)
            df['valor'] = df['valor'].str.replace(",", ".").astype(float) / 100  # ✅ Corrige taxa CDI
            df_total = pd.concat([df_total, df])

        data_ini = data_limite + pd.DateOffset(days=1)

    if df_total.empty:
        return pd.DataFrame(columns=["data", "valor"])

    df_total = df_total.sort_values('data').reset_index(drop=True)
    return df_total


def inserir_ou_atualizar_indicadores(df_para_inserir):
    """
    Insere ou atualiza os dados na tabela dados_indicadores de forma robusta.
    Usa o comando 'INSERT OR REPLACE' do SQLite para evitar erros de chave única.
    """
    conn_local = sqlite3.connect(DB_PATH)
    cursor_local = conn_local.cursor()
    
    # Prepara os dados para inserção
    dados_tuplas = []
    for _, row in df_para_inserir.iterrows():
        # Garante que os valores nulos sejam tratados corretamente pelo SQLite
        cdi = row['cdi'] if pd.notna(row['cdi']) else None
        ipca = row['ipca'] if pd.notna(row['ipca']) else None
        ibov = row['ibov'] if pd.notna(row['ibov']) else None
        dados_tuplas.append((row['data'], cdi, ipca, ibov))
        
    # Comando SQL "UPSERT"
    comando_sql = "INSERT OR REPLACE INTO dados_indicadores (data, cdi, ipca, ibov) VALUES (?, ?, ?, ?)"
    
    try:
        # 'executemany' é muito mais eficiente para inserir múltiplos registros
        cursor_local.executemany(comando_sql, dados_tuplas)
        conn_local.commit()
        print(f"✅ Inseridos ou atualizados {len(dados_tuplas)} registros na tabela dados_indicadores.")
    except Exception as e:
        print(f"❌ Erro ao tentar inserir/atualizar dados: {e}")
        conn_local.rollback()
    finally:
        conn_local.close()

def atualizar_indicadores():
    hoje = datetime.today().date()
    
    ultima_data_df = pd.read_sql("SELECT MAX(data) as data FROM dados_indicadores", con=engine)
    ultima_data_str = ultima_data_df.iloc[0]['data'] if not ultima_data_df.empty and ultima_data_df.iloc[0]['data'] else '2000-01-01'
    
    ultima_data = pd.to_datetime(ultima_data_str).date()

    if ultima_data >= hoje:
        print("✅ Indicadores já atualizados. Nenhuma atualização necessária.")
        return

    data_inicio = ultima_data + timedelta(days=1)
    data_fim = hoje + timedelta(days=1)

    print(f"Buscando indicadores de {data_inicio.strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')}...")

    # Baixa CDI, IPCA e IBOV
    df_cdi = consultar_bcb(12, data_ini=data_inicio.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y'))
    df_ipca_mensal = consultar_bcb(433, data_ini=data_inicio.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y'))
    df_ibov = yf.download("^BVSP", start=data_inicio.strftime('%Y-%m-%d'), end=data_fim.strftime('%Y-%m-%d'), progress=False)

    # Processa CDI
    df_cdi.rename(columns={'valor': 'cdi'}, inplace=True)
    df_cdi = df_cdi[['data', 'cdi']]
    df_cdi['data'] = pd.to_datetime(df_cdi['data']).dt.strftime('%Y-%m-%d')
    df_cdi = df_cdi.set_index('data')

    # Processa IPCA
    df_ipca_mensal['data'] = pd.to_datetime(df_ipca_mensal['data'])
    df_ipca_mensal['data_fim'] = df_ipca_mensal['data'] + MonthEnd(0)
    df_ipca_mensal['n_dias'] = (df_ipca_mensal['data_fim'] - df_ipca_mensal['data']).dt.days + 1
    df_ipca_mensal['taxa_diaria'] = (1 + df_ipca_mensal['valor']) ** (1 / df_ipca_mensal['n_dias']) - 1
    datas, taxas = [], []
    for _, row in df_ipca_mensal.iterrows():
        for i in range(row['n_dias']):
            dia = row['data'] + timedelta(days=i)
            if dia.date() <= hoje:
                datas.append(dia.strftime('%Y-%m-%d'))
                taxas.append(row['taxa_diaria'])
    df_ipca_diario = pd.DataFrame({'data': datas, 'ipca': taxas}).set_index('data')

    # Processa IBOV
    if not df_ibov.empty:
        df_ibov = df_ibov[['Close']].copy()
        df_ibov.reset_index(inplace=True)
        df_ibov.columns = ['data', 'ibov']
        df_ibov['data'] = pd.to_datetime(df_ibov['data']).dt.strftime('%Y-%m-%d')
        df_ibov = df_ibov.set_index('data')
    else:
        df_ibov = pd.DataFrame(columns=['data', 'ibov']).set_index('data')

    # Merge geral dos indicadores
    df_merge = df_cdi.join(df_ipca_diario, how='outer')
    df_merge = df_merge.join(df_ibov, how='outer').reset_index()

    # <--- MUDANÇA PRINCIPAL AQUI --- >
    # Em vez de tentar filtrar e usar to_sql, chamamos nossa nova função robusta
    if df_merge.empty:
        print("✅ Nenhum novo dado de indicadores para inserir.")
    else:
        inserir_ou_atualizar_indicadores(df_merge)

def ultima_data_noticia():
    """
    Retorna a data da notícia mais recente na tabela 'dados_noticias'
    ou uma data de início padrão se a tabela estiver vazia.
    """
    query = "SELECT MAX(data_publicacao) FROM dados_noticias"
    cursor.execute(query)
    resultado = cursor.fetchone()
    # Se a tabela estiver vazia, retorna uma data inicial para a primeira grande atualização
    if resultado and resultado[0]:
        return datetime.strptime(resultado[0], "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        # Data de início para a primeira grande atualização (ex: 1º de janeiro de 2024)
        return datetime(2024, 1, 1)

def atualizar_noticias(df_tickers):
    hoje = datetime.now()
    data_inicio_busca = ultima_data_noticia()
    
    # Chave adquirida em: 
    API_KEY = "08cfca0b968f4b34802ac3147d0319c2"
    
    if data_inicio_busca.date() < hoje.date():
        data_inicio_busca = data_inicio_busca + timedelta(days=1)
        print(f"\n📰 Buscando notícias a partir de {data_inicio_busca.strftime('%d/%m/%Y')}...")
    else:
        print("\n✅ As notícias já estão atualizadas para hoje.")
        return

    total = len(df_tickers)

    for i, ticker in enumerate(df_tickers, 1):
        query_term = ticker.split('.')[0]
        
        # A API de notícias aceita a data de início da busca com o parâmetro 'from'
        url = (
            f"https://newsapi.org/v2/everything?q={query_term}&language=pt"
            f"&from={data_inicio_busca.strftime('%Y-%m-%d')}"
            f"&sortBy=publishedAt&apiKey={API_KEY}"
        )
        
        try:
            response = requests.get(url)
            dados_noticias = response.json()
            
            if 'articles' in dados_noticias:
                for artigo in dados_noticias['articles']:
                    # Verificar se a notícia já existe para evitar duplicatas, usando título e data
                    cursor.execute(
                        "SELECT id FROM dados_noticias WHERE data_publicacao = ? AND titulo = ?", 
                        (artigo['publishedAt'], artigo['title'])
                    )
                    if cursor.fetchone() is None:
                        cursor.execute("""
                            INSERT INTO dados_noticias (data_publicacao, fonte, titulo, conteudo_resumido, ticker)
                            VALUES (?, ?, ?, ?, ?)
                        """, (artigo['publishedAt'], artigo['source']['name'], artigo['title'], artigo['description'], ticker))
                conn.commit()
                print(f"✅ Notícias para {ticker} salvas. ({i}/{total})", end='\r')
            
        except Exception as e:
            print(f"\n❌ Erro ao buscar notícias para {ticker}: {e}")

    print("\n✅ Concluída a atualização de notícias.")


# ========== CRIA ÍNDICES E VIEW ==========
def criar_indices_views():
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_hist ON dados_historicos(ticker);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_hist ON dados_historicos(date);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_date_hist ON dados_historicos(ticker, date);')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_fii ON dados_fii(ticker);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_fii ON dados_fii(date);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_date_fii ON dados_fii(ticker, date);')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_fund ON dados_fundamentais(ticker);')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_indicadores ON dados_indicadores(data);')

    engine.connect().execute(text("""
        CREATE VIEW IF NOT EXISTS view_dados_completos AS
        SELECT h.*, 
               f.nome, f.setor, f.industria, f.pais, f.site,
               f.funcionarios, f.payout_ratio, f.receita, f.lucro_bruto,
               f.ebitda, f.margem_lucro, f.caixa_total, f.divida_total,
               f.roe, f.roa, f.liquidez_corrente, f.beta
        FROM dados_historicos h
        LEFT JOIN dados_fundamentais f ON h.ticker = f.ticker
    """))

    conn.commit()

# ========== EXECUÇÃO ==========
def main():
    print("Iniciando o processo de atualização...")

    # Criação das tabelas
    criar_tabelas()
    print("Tabelas verificadas/criadas.")

    # Atualização dos dados históricos
    hoje = datetime.today().date()
    data_hist = pd.to_datetime(ultima_data_global('dados_historicos')).date()
    if data_hist >= hoje:
        print("✅ Dados históricos já atualizados.")
    else:
        print(f"🕒 Atualizando dados históricos de {data_hist} a {hoje}.")
        df_acoes = pd.read_csv(CSV_TICKERS_ACOES)
        lista_tickers = df_acoes['Ticker'].dropna().astype(str).str.upper().tolist()
        atualizar_historicos(lista_tickers, data_hist + timedelta(days=1), hoje + timedelta(days=1))

    # Adicione mensagens para as outras seções também
    print("Iniciando atualização de notícias...")
    df_acoes = pd.read_csv(CSV_TICKERS_ACOES)
    lista_tickers = df_acoes['Ticker'].dropna().astype(str).str.upper().tolist()
    atualizar_noticias(lista_tickers)
    print("Finalizada atualização de notícias.")

    print("Iniciando atualização de indicadores...")
    atualizar_indicadores()
    print("Finalizada atualização de indicadores.")

    # ... (restante da função main) ...

    print("Processo de atualização finalizado.")

if __name__ == '__main__':
    main()
    conn.close()