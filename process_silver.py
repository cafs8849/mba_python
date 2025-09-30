import json
import os
import pandas as pd
import re 
from datetime import datetime 
import logging
import sys 

# Adicione a importação do config.py AQUI para ter acesso às variáveis de diretório e à função setup_directories
# IMPORTAÇÃO CORRIGIDA:
from config import BRONZE_DIR, SILVER_DIR, setup_directories 

# Configuração do logging estruturado e de nível
logging.basicConfig(
    # Garante que o log vai para a saída padrão (terminal)
    stream=sys.stdout, 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
)
logger = logging.getLogger(__name__)

def get_latest_bronze_file():
    """Encontra o arquivo .json mais recente na pasta BRONZE."""
    try:
        # A variável BRONZE_DIR está disponível graças à importação acima
        files = [f for f in os.listdir(BRONZE_DIR) if f.endswith('.json')]
        if not files:
            return None
        # Ordena em ordem decrescente (o mais recente com o maior timestamp)
        files.sort(reverse=True)
        return os.path.join(BRONZE_DIR, files[0])
    except FileNotFoundError:
        return None

def process_and_save_silver():
    """Lê o Bronze, valida, normaliza (Pandas DataFrame) e salva na camada SILVER."""
    
    # A função setup_directories() está disponível graças à importação
    setup_directories()
    logger.info("--- INICIANDO PROCESSAMENTO SILVER ---") # Log de INFO
    
    # 1. Encontrar e Ler o arquivo BRONZE
    bronze_path = get_latest_bronze_file()
    if not bronze_path:
        logger.error("[ERRO] Não foi encontrado nenhum arquivo na camada BRONZE. Execute 'collect_bronze.py' primeiro.") # Log de ERROR
        return
    
    logger.info(f"Lendo dados de: {bronze_path}")
    
    try:
        with open(bronze_path, 'r', encoding='utf-8') as f:
            bronze_data = json.load(f)
    except Exception as e:
        logger.error(f"Falha ao ler o arquivo BRONZE: {e}") # Log de ERROR
        return

    # --- INÍCIO DA VALIDAÇÃO E NORMALIZAÇÃO ---
    
    if bronze_data.get("result") != "success" or "conversion_rates" not in bronze_data:
        logger.error("[FALHA] Dados BRONZE inválidos ou incompletos.")
        return

    # 1. Normalizar
    rates = bronze_data["conversion_rates"]
    data_list = [(currency, rate) for currency, rate in rates.items()]
    df = pd.DataFrame(data_list, columns=['currency', 'rate'])
    
    # 2. Adicionar colunas de contexto (base_currency)
    df['base_currency'] = bronze_data["base_code"]
    
    # 3. Extrair a data do nome do arquivo (YYYY-MM-DD)
    filename = os.path.basename(bronze_path)
    
    try:
        # Pega os 10 primeiros caracteres (ex: 2025-09-30)
        timestamp_str = filename[:10] 
        df['collected_date'] = pd.to_datetime(timestamp_str)
    except Exception as e:
        logger.error(f"[ERRO FATAL DE DATA] Não foi possível converter '{timestamp_str}' em data. Detalhes: {e}")
        return # Aborta, pois a data é crucial para o join histórico
    
    # 4. GARANTIR QUALIDADE (Taxas não negativas ou nulas)
    initial_count = len(df)
    df['rate'] = pd.to_numeric(df['rate'], errors='coerce') 
    df.dropna(subset=['rate'], inplace=True) 
    df = df[df['rate'] > 0] # Remove negativos ou zero
    
    final_count = len(df)
    # Log de INFO para observabilidade
    logger.info(f"Linhas removidas por valor nulo/negativo: {initial_count - final_count}")
    
    # --- FIM DA VALIDAÇÃO E NORMALIZAÇÃO ---

    # 5. Salva na camada SILVER (CSV, formato normalizado e limpo)
    filename_from_bronze = os.path.basename(bronze_path).replace(".json", "")
    silver_file_path = os.path.join(SILVER_DIR, f"{filename_from_bronze}_silver.csv")
    
    df.to_csv(silver_file_path, index=False)
    
    logger.info(f"[SUCESSO] Dados normalizados salvos em: {silver_file_path}")
    logger.info("--- PROCESSAMENTO SILVER CONCLUÍDO ---")

if __name__ == "__main__":
    process_and_save_silver()