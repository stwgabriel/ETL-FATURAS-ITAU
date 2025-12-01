# Modelo de Dados - ETL Faturas Itau

Este documento descreve a estrutura dos dados extraídos das faturas em PDF.

## Estrutura do Arquivo CSV (`faturas_consolidado.csv`)

O arquivo gerado é um CSV com separador padrão (vírgula) contendo as seguintes colunas:

| Coluna | Tipo | Descrição | Exemplo |
|---|---|---|---|
| `arquivo_origem` | String | Nome do arquivo PDF de onde a transação foi extraída. | `Fatura_Itau_20251110-121024.pdf` |
| `data_vencimento` | Date (YYYY-MM-DD) | Data de vencimento da fatura, extraída do texto ou inferida do nome do arquivo. | `2025-11-10` |
| `data_transacao` | Date (YYYY-MM-DD) | Data em que a compra foi realizada. O ano é inferido com base no vencimento da fatura. | `2025-10-11` |
| `estabelecimento` | String | Nome do estabelecimento onde a compra foi realizada. | `IFOOD*IFOOD` |
| `parcela` | String | Informação de parcelamento, se houver. | `02/03` |
| `valor` | Float | Valor da transação em Reais (BRL). Valores formatados com ponto decimal. | `12.90` |

## Chaves e Identificadores

- **Chave Primária Composta**: Não há um ID único por transação no PDF. Uma chave única pode ser composta por: `arquivo_origem` + `data_transacao` + `estabelecimento` + `valor`.
- **Relacionamentos**:
    - Cada linha representa uma transação financeira.
    - Múltiplas transações pertencem a um `arquivo_origem` (Fatura).

## Regras de Negócio e Transformações

1. **Inferência de Ano**: As transações na fatura possuem apenas Dia/Mês. O ano é calculado com base na data de vencimento da fatura:
    - Se o mês da transação for maior que o mês de vencimento (ex: Compra em Dezembro, Vencimento em Janeiro), subtrai-se 1 do ano de vencimento.
    - Caso contrário, utiliza-se o mesmo ano do vencimento.
2. **Limpeza de Valores**:
    - Símbolos de moeda e separadores de milhar são removidos.
    - Vírgula decimal é substituída por ponto.
3. **Transações em Linha**:
    - Algumas faturas apresentam múltiplas transações na mesma linha de texto devido à formatação em colunas. O ETL identifica e separa essas ocorrências.
