import pytest
import pandas as pd
import numpy as np

# A função de validação (se ela estivesse separada) seria testada.
# Como a validação está no process_silver.py, vamos replicar a lógica.

def clean_and_validate_rates(data: dict) -> pd.DataFrame:
    """Simula a lógica de validação da Camada Silver."""
    
    # 1. Normalizar
    data_list = [(currency, rate) for currency, rate in data["conversion_rates"].items()]
    df = pd.DataFrame(data_list, columns=['currency', 'rate'])
    
    # 2. Qualidade (A parte que precisa ser testada)
    initial_count = len(df)
    df['rate'] = pd.to_numeric(df['rate'], errors='coerce') 
    df.dropna(subset=['rate'], inplace=True) 
    df = df[df['rate'] > 0] # Remove negativos/zero
    
    # 3. Retorna o resultado
    return df

# --- TESTES UNITÁRIOS ---

def test_negative_rates_are_removed():
    """Verifica se taxas negativas ou zero são removidas."""
    mock_data = {
        "conversion_rates": {
            "USD": 5.00,
            "EUR": -0.50, # Deve ser removida
            "JPY": 0.0,   # Deve ser removida
            "GBP": 6.50
        }
    }
    df_clean = clean_and_validate_rates(mock_data)
    
    # O tamanho final deve ser 2 (USD e GBP)
    assert len(df_clean) == 2, "Taxas negativas ou zero não foram removidas corretamente."
    assert 'EUR' not in df_clean['currency'].values
    assert 'JPY' not in df_clean['currency'].values

def test_non_numeric_rates_are_removed():
    """Verifica se taxas não-numéricas são removidas (tratadas como NaN e removidas)."""
    mock_data = {
        "conversion_rates": {
            "USD": 5.00,
            "EUR": "cinco", # Deve ser removida
            "CAD": "4.50"   # Deve ser mantida (string numérica)
        }
    }
    df_clean = clean_and_validate_rates(mock_data)
    
    # O tamanho final deve ser 2 (USD e CAD)
    assert len(df_clean) == 2, "Taxas não numéricas não foram removidas corretamente."
    assert 'EUR' not in df_clean['currency'].values

def test_all_valid_rates_are_kept():
    """Verifica se todas as taxas válidas são mantidas."""
    mock_data = {
        "conversion_rates": {
            "USD": 5.00,
            "EUR": 5.50,
            "JPY": 0.03
        }
    }
    df_clean = clean_and_validate_rates(mock_data)
    assert len(df_clean) == 3, "Taxas válidas não foram mantidas."