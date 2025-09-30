import pandas as pd
import os

def read_gold_parquet():
    """Lê o arquivo Parquet mais recente da camada GOLD e imprime o conteúdo."""
    
    GOLD_DIR = "data_layers/gold"
    
    # Encontra o arquivo .parquet mais recente na pasta GOLD
    try:
        files = [f for f in os.listdir(GOLD_DIR) if f.endswith('.parquet')]
        if not files:
            print(f"[ERRO] Nenhum arquivo .parquet encontrado em {GOLD_DIR}")
            return
            
        files.sort(reverse=True)
        latest_file = os.path.join(GOLD_DIR, files[0])
        
    except FileNotFoundError:
        print(f"[ERRO] O diretório {GOLD_DIR} não existe. Execute o aggregate_gold.py primeiro.")
        return

    print(f"--- Lendo o arquivo Parquet mais recente: {latest_file} ---")

    try:
        # Lê o Parquet (o pandas usa o pyarrow nos bastidores)
        df_gold = pd.read_parquet(latest_file)
        
        # Imprime as primeiras 5 linhas e as informações sobre as colunas
        print("\nPrimeiras 5 linhas do DataFrame GOLD:")
        print(df_gold.head())
        print("\nInformações sobre as colunas (Tipos de Dados):")
        df_gold.info()

    except Exception as e:
        print(f"[ERRO] Falha ao ler o arquivo Parquet: {e}")

if __name__ == "__main__":
    read_gold_parquet()