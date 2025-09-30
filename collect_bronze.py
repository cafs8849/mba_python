import requests
import json
import os
# ... imports
from config import URL_API, BRONZE_DIR, FILENAME_BASE, setup_directories

def collect_and_save_bronze():
    setup_directories()
    
    # 1. DEFINIÇÃO CORRETA: Defina o caminho antes do bloco try/except
    bronze_file_path = os.path.join(BRONZE_DIR, f"{FILENAME_BASE}.json")
    
    print("--- INICIANDO COLETA BRONZE ---")
    
    try:
        # ... (seu código de DEBUG da URL e Status Code aqui)
        
        response = requests.get(URL_API)
        response.raise_for_status() 
        raw_data = response.json()

        # 2. Salva na camada BRONZE (aqui apenas o comando de salvar, sem redefinição)
        with open(bronze_file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, indent=4)
        
        # 3. A impressão de sucesso agora funciona, pois a variável foi definida
        print(f"   [SUCESSO] Dados brutos salvos em: {bronze_file_path}")
        print("--- COLETA BRONZE CONCLUÍDA ---")
        return bronze_file_path

    except requests.exceptions.RequestException as e:
        # 4. Caso de erro de conexão ou HTTP (Chave errada, por exemplo)
        print(f"   [ERRO] Falha na coleta ou conexão. Chave de API pode estar incorreta.")
        print(f"   Detalhes: {e}")
        return None

if __name__ == "__main__":
    collect_and_save_bronze()