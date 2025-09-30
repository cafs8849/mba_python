import pandas as pd
import os
import json
from google import genai
from google.genai.errors import APIError
from config import GOLD_DIR, GEMINI_API_KEY, setup_directories

# --- Configuração do LLM ---
# A inicialização é feita fora das funções para reuso
try:
    if not GEMINI_API_KEY:
        raise ValueError("A chave GEMINI_API_KEY não foi encontrada no .env.")
    
    client = genai.Client(api_key=GEMINI_API_KEY)
    LLM_MODEL = 'gemini-2.5-flash' # Modelo rápido e eficiente para texto
except ValueError as e:
    print(f"[ERRO DE CONFIGURAÇÃO] {e}")
    client = None


def get_latest_gold_file():
    """Encontra o arquivo .parquet mais recente na pasta GOLD."""
    try:
        files = [f for f in os.listdir(GOLD_DIR) if f.endswith('_gold.parquet')]
        if not files:
            return None
        files.sort(reverse=True)
        return os.path.join(GOLD_DIR, files[0])
    except FileNotFoundError:
        return None

def generate_insights_with_llm():
    """Lê os dados Gold e usa o LLM para gerar insights e explicações."""
    
    if not client:
        print("[AVISO] O LLM não pode ser utilizado pois a chave de API está ausente ou inválida.")
        return

    setup_directories()
    print("--- INICIANDO ENRIQUECIMENTO COM LLM ---")

    # 1. Encontrar e Ler o arquivo GOLD (Parquet)
    gold_path = get_latest_gold_file()
    if not gold_path:
        print("[ERRO] Nenhum arquivo Parquet encontrado na camada GOLD. Execute 'aggregate_gold.py' primeiro.")
        return
    
    df_gold = pd.read_parquet(gold_path)
    print(f"   Lendo dados de: {gold_path}")

    # 2. Preparar os dados para o Prompt
    # Convertendo o DataFrame em uma string Markdown ou JSON para o LLM
    data_for_llm = df_gold.to_markdown(index=False)
    
    # 3. Definir o Prompt e enviar ao LLM
    prompt = f"""
    A tabela abaixo contém as taxas de câmbio mais recentes em relação ao Real Brasileiro (BRL).
    
    Dados da Tabela GOLD:
    {data_for_llm}
    
    Com base nesses dados, crie dois parágrafos distintos:
    1. Resumo Executivo: Uma explicação simples, em termos de negócio, sobre como está a variação das 3 principais moedas (USD, EUR, JPY) frente ao Real (BRL) HOJE, focando nas taxas de câmbio atuais. Não inclua a data na resposta.
    2. Análise Sugerida: Uma sugestão de análise que poderia ser feita se houvesse dados históricos (como volatilidade, comparação com o mês passado). Use exemplos como: "A volatilidade do JPY em relação ao USD está acima da média" ou "O Euro está 5% mais valorizado em relação ao mês passado."

    Formate sua resposta usando os cabeçalhos 'Resumo Executivo:' e 'Análise Sugerida:'.
    """
    
    print("   Enviando dados para o LLM para gerar insights...")

    try:
        # 4. Chamada da API do Gemini
        response = client.models.generate_content(
            model=LLM_MODEL,
            contents=prompt
        )

        # 5. Imprimir Resultados
        print("\n" + "="*50)
        print("          ✨ INSIGHTS GERADOS PELO LLM ✨")
        print("="*50)
        print(response.text)
        print("="*50)

    except APIError as e:
        print(f"[ERRO DE API DO LLM] Falha ao comunicar com a API Gemini: {e}")
    except Exception as e:
        print(f"[ERRO INESPERADO] {e}")


if __name__ == "__main__":
    generate_insights_with_llm()