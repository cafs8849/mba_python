import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from google import genai
from google.genai.errors import APIError
from config import GOLD_DIR, GEMINI_API_KEY, setup_directories

# Configuração de estilo do Matplotlib
plt.style.use('ggplot') 

# Função utilitária para carregar o arquivo Parquet de uma data específica
def load_gold_data(date: datetime):
    """Tenta carregar o arquivo Parquet para a data fornecida."""
    date_str = date.strftime("%Y-%m-%d")
    
    # Busca o arquivo pelo padrão de nome (ex: 2025-09-29_gold.parquet)
    # Procuramos o arquivo que começa com a data_str
    try:
        files = [f for f in os.listdir(GOLD_DIR) if f.startswith(date_str) and f.endswith('_gold.parquet')]
        if not files:
            return None
            
        file_path = os.path.join(GOLD_DIR, files[0])
        return pd.read_parquet(file_path)
    except Exception as e:
        print(f"   [AVISO] Falha ao carregar dados de {date_str}: {e}")
        return None

def generate_comparison_report():
    """Compara o câmbio de hoje com o de ontem, gera gráfico e análise LLM."""
    
    setup_directories()
    print("--- INICIANDO ANÁLISE COMPARATIVA ---")

    # 1. Definir Datas
    TODAY = datetime.now().date()
    YESTERDAY = TODAY - timedelta(days=1)
    
    # --- Configuração do LLM (Copiado do enrich_llm.py) ---
    LLM_MODEL = 'gemini-2.5-flash' # Definindo o modelo aqui

    try:
        if not GEMINI_API_KEY:
            raise ValueError("A chave GEMINI_API_KEY não foi encontrada no .env.")
    
        client = genai.Client(api_key=GEMINI_API_KEY)
    except ValueError as e:
        print(f"[ERRO DE CONFIGURAÇÃO] {e}")
        client = None

    # 2. Carregar Dados
    df_today = load_gold_data(datetime.combine(TODAY, datetime.min.time()))
    df_yesterday = load_gold_data(datetime.combine(YESTERDAY, datetime.min.time()))

    if df_today is None:
        print(f"[ERRO] Não há dados GOLD para hoje ({TODAY}). Execute a pipeline completa primeiro.")
        return
    
    if df_yesterday is None:
        print(f"[ERRO] Não há dados GOLD para ontem ({YESTERDAY}). Impossível comparar. Crie o arquivo histórico.")
        # Se não houver ontem, apenas gera o gráfico de hoje (opcional)
        df_comparison = df_today 
    else:
        # 3. Calcular Variação Percentual
        print("   Comparando dados de hoje com o dia anterior...")
        
        # Seleciona as colunas de taxas para o cálculo (exclui datas e moedas base)
        rate_cols = [col for col in df_today.columns if col not in ['collected_date', 'base_currency']]
        
        # Garante que os DataFrames têm a mesma estrutura para a comparação
        df_comp = pd.merge(df_today, df_yesterday, on='base_currency', suffixes=('_today', '_yesterday'))
        
        # Calcula a variação percentual para cada moeda
        df_comparison = pd.DataFrame()
        df_comparison['collected_date'] = df_comp['collected_date_today']
        df_comparison['base_currency'] = df_comp['base_currency']
        
        for col in rate_cols:
            col_today = f'{col}_today'
            col_yesterday = f'{col}_yesterday'
            
            # Variação % = ((Hoje - Ontem) / Ontem) * 100
            df_comparison[f'{col}_CHANGE_PCT'] = (
                (df_comp[col_today] - df_comp[col_yesterday]) / df_comp[col_yesterday]
            ) * 100
            
            # Adiciona o valor atual para referência (necessário para o LLM)
            df_comparison[col] = df_comp[col_today]
        
        print("   Cálculo de variação percentual concluído.")
        
    # --- 4. Geração do Gráfico ---
    plot_data = df_comparison.filter(regex='_CHANGE_PCT$').iloc[0] # Pega a primeira linha com as variações
    
    # Remove colunas que são NaN ou desnecessárias
    plot_data.dropna(inplace=True) 
    
    plt.figure(figsize=(10, 6))
    colors = ['g' if x >= 0 else 'r' for x in plot_data] # Verde para alta, vermelho para baixa
    plot_data.plot(kind='bar', color=colors)
    
    plt.title(f'Variação Percentual do Câmbio (Base {df_today["base_currency"].iloc[0]}): Hoje vs Ontem', fontsize=14)
    plt.ylabel('Variação %')
    plt.xlabel('Taxa de Câmbio')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    # Salva o gráfico
    report_file_name = f'exchange_report_{TODAY.strftime("%Y%m%d")}.png'
    report_path = os.path.join(os.getcwd(), report_file_name) # Salva na raiz do projeto
    plt.savefig(report_path)
    plt.close()
    
    print(f"   [SUCESSO] Gráfico de variação salvo em: {report_path}")

    # --- 5. Enriquecimento com LLM (Com dados de variação) ---
    if client:
        print("   Enviando dados de variação para o LLM...")
        
        # Converte a comparação para Markdown para o LLM
        llm_input_data = df_comparison.to_markdown(index=False)
        
        prompt = f"""
        A tabela abaixo contém as taxas de câmbio de hoje e a variação percentual (colunas com '_CHANGE_PCT') em relação ao dia anterior (Base: BRL).

        Dados para Análise:
        {llm_input_data}

        Crie uma Explicação Executiva em linguagem natural, com no máximo 5 linhas, que interprete a variação percentual das moedas (USD, EUR, JPY) e resuma se o Real se valorizou ou desvalorizou mais significativamente em relação a elas.
        """
        try:
            response = client.models.generate_content(
                model=LLM_MODEL,
                contents=prompt
            )

            print("\n" + "="*50)
            print("          ✨ ANÁLISE EXECUTIVA (LLM) ✨")
            print("="*50)
            print(response.text)
            print("="*50)

        except APIError as e:
            print(f"[ERRO DE API DO LLM] Falha ao comunicar com a API Gemini: {e}")
            
    print("--- ANÁLISE COMPARATIVA CONCLUÍDA ---")


if __name__ == "__main__":
    generate_comparison_report()