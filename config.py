import os
from datetime import datetime
from dotenv import load_dotenv  # Importa a função para carregar .env

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- Configurações do LLM ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# --- Configurações da API (Lidas do .env) ---
# Usamos os.getenv() para ler as variáveis
API_KEY = os.getenv("API_KEY")
MOEDA_BASE = os.getenv("MOEDA_BASE", "BRL") # O "BRL" é um valor padrão caso não encontre
API_BASE_URL = os.getenv("API_BASE_URL")

# Monta a URL completa sem hardcode
URL_API = f"{API_BASE_URL}/{API_KEY}/latest/{MOEDA_BASE}"


# --- Configurações de Diretórios e Camadas ---
DATA_DIR = "data_layers"
# Renomeando a camada de RAW para BRONZE (se ainda não o fizemos)
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")


# --- Nomenclatura dos Arquivos (YYYY-MM-DD) ---
# O professor solicitou o formato YYYY-MM-DD no nome do arquivo
DATE_STR = datetime.now().strftime("%Y-%m-%d")

# O nome base do arquivo será apenas a data de coleta, como solicitado
FILENAME_BASE = f"{DATE_STR}"


def setup_directories():
    """Cria os diretórios das camadas de dados se não existirem."""
    # Garante que criamos o diretório BRONZE
    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)
    print("Diretórios de camadas de dados verificados.")