import pandas as pd
import os
from config import SILVER_DIR, GOLD_DIR, setup_directories

def get_latest_silver_file():
    """Encontra o arquivo .csv mais recente na pasta SILVER."""
    try:
        # Busca por arquivos .csv (mudamos o formato do Silver para CSV)
        files = [f for f in os.listdir(SILVER_DIR) if f.endswith('_silver.csv')]
        if not files:
            return None
        files.sort(reverse=True)
        return os.path.join(SILVER_DIR, files[0])
    except FileNotFoundError:
        return None

def aggregate_and_save_gold():
    """Lê o Silver (DataFrame), agrega e salva na camada GOLD (Parquet)."""
    
    setup_directories()
    print("--- INICIANDO AGREGAÇÃO GOLD (Parquet) ---")

    # 1. Encontrar e Ler o arquivo SILVER
    silver_path = get_latest_silver_file()
    if not silver_path:
        print("[ERRO] Não foi encontrado nenhum arquivo na camada SILVER. Execute 'process_silver.py' primeiro.")
        return
    
    print(f"   Lendo dados de: {silver_path}")
    try:
        # Lê o CSV normalizado para um DataFrame
        df_silver = pd.read_csv(silver_path)
    except Exception as e:
        print(f"[ERRO] Falha ao ler o arquivo SILVER: {e}")
        return

    # 2. AGREGAÇÃO / PREPARAÇÃO PARA ANÁLISE (GOLD LÓGICA)
    
    # O PIVOT transforma o formato longo (normalizado) em formato largo (analítico)
    # Por exemplo: cria colunas USD, EUR, etc.
    df_gold = df_silver.pivot(
        index=['collected_date', 'base_currency'],
        columns='currency',
        values='rate'
    ).reset_index()

    # Exemplo de cálculo Gold: Taxa de USD para EUR
    if 'USD' in df_gold.columns and 'EUR' in df_gold.columns:
        df_gold['USD_to_EUR'] = df_gold['EUR'] / df_gold['USD']
        
    # Renomeia as colunas para o formato final de relatório
    df_gold.rename(columns={'USD': 'BRL_to_USD', 'EUR': 'BRL_to_EUR'}, inplace=True)
    
    # Seleciona apenas as colunas do relatório final
    gold_columns = ['collected_date', 'base_currency', 'BRL_to_USD', 'BRL_to_EUR', 'USD_to_EUR']
    df_gold = df_gold[[col for col in gold_columns if col in df_gold.columns]]

    # 3. Salva na camada GOLD em formato PARQUET
    filename_from_silver = os.path.basename(silver_path).replace("_silver.csv", "")
    gold_file_path = os.path.join(GOLD_DIR, f"{filename_from_silver}_gold.parquet")
    
    # O Parquet é o formato mais eficiente para Engenharia de Dados
    df_gold.to_parquet(gold_file_path, index=False)
        
    print(f"   [SUCESSO] Dados agregados salvos em: {gold_file_path} (Formato PARQUET)")
    print("--- AGREGAÇÃO GOLD CONCLUÍDA ---")

if __name__ == "__main__":
    aggregate_and_save_gold()