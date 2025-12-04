import glob
import pandas as pd
import logging
from src.etl_processor import process_files_to_df

# Configuração básica de logging para ver o progresso
logging.basicConfig(level=logging.INFO)

def run_processing():
    # 1. Encontrar todos os PDFs nas pastas identificadas
    # Usando recursive=True para garantir que pegue subpastas se houver
    # Ajustando os padrões baseados na estrutura encontrada
    patterns = [
        "/Users/stwgabriel/Documents/Development/Fullstack/ETLS/data/Novas/*.pdf",
        "/Users/stwgabriel/Documents/Development/Fullstack/ETLS/data/Faturas/*.pdf"
    ]
    
    all_files = []
    for pattern in patterns:
        found = glob.glob(pattern)
        all_files.extend(found)
        
    # Remover duplicatas se houver
    all_files = sorted(list(set(all_files)))
    
    print(f"Encontrados {len(all_files)} arquivos para processar.")
    for f in all_files:
        print(f" - {f.split('/')[-1]}")
        
    if not all_files:
        print("Nenhum arquivo encontrado. Verifique os caminhos.")
        return

    # 2. Processar usando o novo método do etl_processor
    print("\nIniciando processamento...")
    df_result = process_files_to_df(all_files)
    
    if df_result.empty:
        print("O processamento não retornou dados.")
        return

    # 3. Salvar resultados
    output_csv = "resultado_faturas_consolidado.csv"
    output_excel = "resultado_faturas_consolidado.xlsx"
    
    print(f"\nSalvando resultados em {output_csv} e {output_excel}...")
    
    df_result.to_csv(output_csv, index=False)
    try:
        df_result.to_excel(output_excel, index=False)
    except ImportError:
        print("Bibliotecas de Excel não instaladas (openpyxl/xlsxwriter), salvando apenas CSV.")
    
    # 4. Exibir resumo
    print("\n--- Resumo do Processamento ---")
    print(f"Total de Transações: {len(df_result)}")
    if 'valor' in df_result.columns:
        print(f"Valor Total Processado: R$ {df_result['valor'].sum():.2f}")
    
    if 'arquivo' in df_result.columns:
        print("\nTransações por Arquivo:")
        print(df_result.groupby('arquivo')['valor'].sum())

if __name__ == "__main__":
    run_processing()
