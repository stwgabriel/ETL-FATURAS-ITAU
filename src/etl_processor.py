import pdfplumber
import os
import re
import pandas as pd
import logging
from datetime import datetime
from io import StringIO
from typing import Union, List, Dict, Any
from itertools import combinations
import logging
# Configure basic logging if not already configured
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class InvoiceProcessor:
    def __init__(self):
        self.categories = {
            "Transporte": ["UBER", "99POP","99*","99", "99APP", "99RIDE", "99PAY", "METRO", "VELOE", "SEM PARAR", "POSTO", "SHELL", "IPIRANGA", "ESTACIONAMENTO", "LOCALIZA", "MOVIDA", "UNIDAS", "WHOOSH"],
            "Alimentação": ["IFOOD", "IFD", "RAPPI", "UBER EATS", "BURGER", "MC DONALDS", "MCDONALDS", "OUTBACK", "RESTAURANTE", "PADARIA", "MERCADO", "SUPERMERCADO", "MUNDIAL", "ZONA SUL", "PAO DE ACUCAR", "PAODEACUCAR", "PDA", "MINUTO", "MINUTOPA", "ASSAI", "CARREFOUR", "EXTRA", "HORTIFRUTI", "BEBIDAS", "BAR", "BISTRO", "DOCES", "GIGANTE", "GRUPO FARTURA", "CONFIANCA", "SODEXO", "ZIG", "COLODEMAE", "SAMBADAROSA", "SKINA", "TORTA"],
            "Saúde": ["DROGARIA", "FARMACIA", "RAIA", "PACHECO", "VENANCIO", "HOSPITAL", "CLINICA", "LABORATORIO", "CONSULTORIO", "RD SAUDE", "RDSAUDE", "VETERINARIO", "VETERINARIOSA", "WELLHUB", "GYMPASS", "SPORTCLUB"],
            "Serviços/Assinaturas": ["NETFLIX", "SPOTIFY", "AMAZON PRIME", "CLARO", "VIVO", "TIM", "OI", "INTERNET", "TV", "APPLE", "GOOGLE", "CLUBE", "LIVELO", "YELUM", "SEGURADORA", "SEGURO", "KEYDROP"],
            "Compras": ["AMAZON", "MERCADO LIVRE", "MELI", "MAGALU", "SHOPEE", "ALIEXPRESS", "SHEIN", "ZARA", "RENNER", "C&A", "RIACHUELO", "DECATHLON", "CENTAURO", "NETSHOES", "VANS", "VIVARA", "FAST SHOP", "FASTSHOP", "AZEVEDO", "LUIS FELIPPE", "LUISFELIPPE", "COMPRA DE PONTOS", "AQUINO", "OUTLET", "MODA"],
            "Viagem": ["HOTEL", "AIRBNB", "BOOKING", "CVC", "LATAM", "GOL", "AZUL", "PASSAGEM", "IBIS", "INGRESSE"],
            "Financeiro": ["IOF", "ENCARGOS", "MULTA", "JUROS", "ANUIDADE"]
        }
        
    def extract_page_text(self, page_obj, page_index):
        # Check if it's a candidate for 2-column split
        # Heuristic: If width > 500 (e.g. A4 is 595) and we are processing typical invoice pages
        if page_obj.width > 500:
            # Based on analysis, the gap is between 340 and 367.
            # Safe split point is around 355.
            split_x = 355
            
            # Left Column
            left_bbox = (0, 0, split_x, page_obj.height)
            text_left = page_obj.crop(left_bbox).extract_text(x_tolerance=3) or ""
            
            # Right Column
            right_bbox = (split_x, 0, page_obj.width, page_obj.height)
            text_right = page_obj.crop(right_bbox).extract_text(x_tolerance=3) or ""
            
            # Concatenate
            logging.debug(f"Page {page_index+1}: Applied 2-column split at x={split_x}")
            return text_left + "\n" + text_right
        
        return page_obj.extract_text(x_tolerance=3) or ""

    def categorize_transaction(self, description):
        desc_upper = description.upper()
        for category, keywords in self.categories.items():
            for keyword in keywords:
                if keyword in desc_upper:
                    return category
        return "Outros"

    def parse_money(self, value_str):
        try:
            # Remove spaces first, then handle standard PT-BR formatting
            clean_str = value_str.replace(' ', '').replace('.', '').replace(',', '.')
            return float(clean_str)
        except ValueError:
            return 0.0

    def extract_header_info(self, text):
        info = {
            "valor_total_declarado": 0.0,
            "data_emissao": None,
            "data_vencimento": None,
            "nome_cliente": "UNKNOWN",
            "cartao_principal": "UNKNOWN"
        }
        
        # Normaliza o texto removendo espaços para lidar com formatações estranhas
        tnorm = text.replace(" ", "").lower()
        
        # 1. Tentativa direta no texto original (com suporte a espaços no valor)
        re_total = re.search(r'Total\s*desta\s*fatura\s*([\d\.,\s]+)', text)
        
        if not re_total:
             re_total = re.search(r'O total da sua fatura é:[\s\S]*?R\$\s*([\d\.,\s]+)', text)
        
        if re_total:
            info["valor_total_declarado"] = self.parse_money(re_total.group(1))
        else:
            # 2. Tentativas no texto normalizado (sem espaços)
            m = None
            # Padrão: totaldestafatura...
            if not m:
                m = re.search(r'totaldestafatura.*?([\d\.,]+)', tnorm)
            # Padrão: ototaldasuafaturaé...R$
            if not m:
                m = re.search(r'ototaldasuafaturaé.*?r\$\s*([\d\.,]+)', tnorm)
            # Padrões antigos de lançamentos
            if not m:
                m = re.search(r'(?:l)?lançamentosatuais\s*([\d\.,]+)', tnorm)
            if not m:
                m = re.search(r'(?:l)?lancamentosatuais\s*([\d\.,]+)', tnorm)
            if not m:
                m = re.search(r'totaldoslançamentosatuais\s*([\d\.,]+)', tnorm)
            if not m:
                m = re.search(r'totaldoslancamentosatuais\s*([\d\.,]+)', tnorm)
                
            if m:
                info["valor_total_declarado"] = self.parse_money(m.group(1))
        
        re_vencimento = re.search(r'Vencimento:\s*(\d{2}/\d{2}/\d{4})', text)
        re_emissao = re.search(r'Emissão:\s*(\d{2}/\d{2}/\d{4})', text)
        re_cliente = re.search(r'Titular\s+(.+)', text)
        re_cartao = re.search(r'Cartão\s+(\d{4}\.XXXX\.XXXX\.\d{4})', text)
        
        if re_vencimento:
            info["data_vencimento"] = re_vencimento.group(1)
        if re_emissao:
            info["data_emissao"] = re_emissao.group(1)
        if re_cliente:
            info["nome_cliente"] = re_cliente.group(1).strip()
        if re_cartao:
            info["cartao_principal"] = re_cartao.group(1)
            
        return info

    def extract_generic_header(self, text):
        info = {
            "valor_total_declarado": 0.0,
            "data_emissao": None,
            "data_vencimento": None,
            "nome_cliente": "GENERIC CLIENT",
            "cartao_principal": "UNKNOWN"
        }
        
        # Tentar encontrar Valor Total
        re_total = re.search(r'(?:Total|Valor)\s*(?:da\s*fatura|a\s*pagar|total)?\s*(?:R\$)?\s*([\d\.,]+)', text, re.IGNORECASE)
        if re_total:
            info["valor_total_declarado"] = self.parse_money(re_total.group(1))

        # Tentar encontrar Vencimento
        re_vencimento = re.search(r'Vencimento\s*:?\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
        if re_vencimento:
            info["data_vencimento"] = re_vencimento.group(1)
            
        # Tentar encontrar Nome
        re_ola = re.search(r'Olá,\s*([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)', text)
        if re_ola:
            info["nome_cliente"] = re_ola.group(1)

        return info

    def extract_generic_transactions(self, page_texts, filename, header_info):
        transactions = []
        
        pattern_a = r'(\d{2}/\d{2})\s+(.+?)\s+(-?(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2})'
        pattern_b = r'(.+?)\s+(\d{2}/\d{2})\s+(-?(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2})'

        current_year = datetime.now().year
        if header_info.get('data_vencimento'):
            try:
                current_year = datetime.strptime(header_info['data_vencimento'], "%d/%m/%Y").year
            except:
                pass

        for text in page_texts:
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if not line: continue
                
                if re.search(r'(total|saldo|pagamento|vencimento)', line, re.IGNORECASE):
                    continue

                match = None
                dt_str = None
                desc = None
                val_str = None
                
                m_a = re.search(pattern_a, line)
                if m_a:
                    dt_str, desc, val_str = m_a.groups()
                else:
                    m_b = re.search(pattern_b, line)
                    if m_b:
                        desc, dt_str, val_str = m_b.groups()
                
                if dt_str and val_str:
                    try:
                        valor = self.parse_money(val_str)
                        day, month = map(int, dt_str.split('/'))
                        data_transacao = f"{current_year}-{month:02d}-{day:02d}"
                        
                        transactions.append({
                            "arquivo": filename,
                            "data_emissao": header_info.get("data_emissao"),
                            "data_vencimento": header_info.get("data_vencimento"),
                            "valor_total_declarado": header_info.get("valor_total_declarado"),
                            "nome_cliente": header_info.get("nome_cliente"),
                            "cartao_principal": "GENERIC",
                            "titular_cartao": header_info.get("nome_cliente"),
                            "final_cartao": "XXXX",
                            "internacional": False,
                            "data_transacao": data_transacao,
                            "estabelecimento": desc.strip(),
                            "categoria": self.categorize_transaction(desc),
                            "parcela": None,
                            "valor": valor,
                            "extraction_method": "Generic"
                        })
                    except Exception as e:
                        logging.debug(f"Generic extract error line '{line}': {e}")
                        
        return transactions

    def reconcile_discrepancies(self, transactions: List[Dict], header_info: Dict, page_texts: List[str]) -> List[Dict]:
        """
        Attempts to fix discrepancies between declared total and extracted total
        by searching for common missing charges (Encargos, IOF, etc.) or credits (Saldo Anterior, Descontos) in the summary text.
        """
        if not transactions:
            return transactions

        declared = header_info.get("valor_total_declarado", 0.0)
        if declared == 0.0:
            return transactions

        # Calculate current extracted total
        extracted = sum(t['valor'] for t in transactions)
        diff = declared - extracted

        if abs(diff) < 0.05:
            return transactions

        logging.info(f"Discrepancy detected: Declared={declared:.2f}, Extracted={extracted:.2f}, Diff={diff:.2f}. Attempting reconciliation...")

        # Combine text from first few pages (summary usually on page 1 or 2)
        summary_text = "\n".join(page_texts[:3]) if page_texts else ""
        
        # Case 1: Missing Positive Charges (Diff > 0)
        if diff > 0:
            # Potential missing charges to look for
            patterns = [
                (r'Encargos\s*(?:R\$)?\s*([\d\.,]+)', "Encargos de Financiamento"),
                (r'IOF\s*(?:R\$)?\s*([\d\.,]+)', "IOF de Financiamento"),
                (r'Juros\s*(?:R\$)?\s*([\d\.,]+)', "Juros"),
                (r'Multa\s*(?:R\$)?\s*([\d\.,]+)', "Multa"),
                (r'Tarifa\s*(?:R\$)?\s*([\d\.,]+)', "Tarifa")
            ]
            
            candidates = []
            for pat, cat in patterns:
                matches = re.finditer(pat, summary_text, re.IGNORECASE)
                for m in matches:
                    val_str = m.group(1)
                    val = self.parse_money(val_str)
                    if val > 0:
                        candidates.append({'category': cat, 'value': val})

            # Strategy 1: Check if any single candidate matches the diff
            for cand in candidates:
                if abs(diff - cand['value']) < 0.05:
                    if not self._is_duplicate(transactions, cand['value'], cand['category']):
                        logging.info(f"Reconciliation: Found missing {cand['category']} of {cand['value']}")
                        self._add_reconciled_transaction(transactions, header_info, cand['category'], cand['value'])
                        return transactions
            
            # Strategy 2: Check combinations
            unique_candidates = {f"{c['category']}_{c['value']}": c for c in candidates}.values()
            for r in range(2, len(unique_candidates) + 1):
                for combo in combinations(unique_candidates, r):
                    combo_sum = sum(c['value'] for c in combo)
                    if abs(diff - combo_sum) < 0.05:
                        logging.info(f"Reconciliation: Found missing combination matching {diff:.2f}")
                        for item in combo:
                            if not self._is_duplicate(transactions, item['value'], item['category']):
                                self._add_reconciled_transaction(transactions, header_info, item['category'], item['value'])
                        return transactions

        # Case 2: Missing Credits/Discounts (Diff < 0)
        elif diff < 0:
            # We are looking for a credit that explains the negative difference
            # The missing transaction should have a value of 'diff' (negative).
            # But in the text, it might appear as positive (e.g. "Crédito: 100,00") or negative ("-100,00")
            target_val = abs(diff)
            
            patterns = [
                (r'Saldo\s*(?:Financiado|Anterior)\s*(?:R\$)?\s*(-?[\d\.,]+)', "Saldo Anterior"),
                (r'Crédito\s*(?:R\$)?\s*(-?[\d\.,]+)', "Crédito Fatura"),
                (r'Desconto\s*(?:R\$)?\s*(-?[\d\.,]+)', "Desconto"),
                (r'Pagamento\s*(?:a\s*maior)?\s*(?:R\$)?\s*(-?[\d\.,]+)', "Pagamento Antecipado")
            ]
            
            for pat, cat in patterns:
                matches = re.finditer(pat, summary_text, re.IGNORECASE)
                for m in matches:
                    val_str = m.group(1)
                    val = self.parse_money(val_str)
                    # The extracted value might be positive (1099.00) or negative (-1099.00)
                    # We check if abs(val) matches abs(diff)
                    
                    if abs(abs(val) - target_val) < 0.05:
                        # Found it! We need to add a NEGATIVE transaction
                        final_val = -abs(val) # Ensure it's negative
                        
                        if not self._is_duplicate(transactions, final_val, cat):
                             logging.info(f"Reconciliation: Found missing credit {cat} of {final_val}")
                             self._add_reconciled_transaction(transactions, header_info, cat, final_val)
                             return transactions

        logging.warning(f"Reconciliation failed. Remaining Diff: {diff:.2f}")
        return transactions

    def _is_duplicate(self, transactions, value, category_snippet):
        return any(
            abs(t['valor'] - value) < 0.01 and 
            (category_snippet.lower() in t['estabelecimento'].lower() or category_snippet.lower() in t['categoria'].lower())
            for t in transactions
        )

    def _add_reconciled_transaction(self, transactions, header_info, category, value):
        new_trans = {
            "arquivo": transactions[0]['arquivo'] if transactions else "UNKNOWN",
            "data_emissao": header_info.get("data_emissao"),
            "data_vencimento": header_info.get("data_vencimento"),
            "valor_total_declarado": header_info.get("valor_total_declarado"),
            "nome_cliente": header_info.get("nome_cliente"),
            "cartao_principal": header_info.get("cartao_principal"),
            "titular_cartao": header_info.get("nome_cliente"),
            "final_cartao": "XXXX",
            "internacional": False,
            "data_transacao": header_info.get("data_emissao"), # Default to invoice date
            "estabelecimento": f"RECONCILIATION - {category}",
            "categoria": "Financeiro",
            "parcela": None,
            "valor": value
        }
        # Convert date format if needed
        try:
            if new_trans["data_transacao"]:
                dt_obj = datetime.strptime(new_trans["data_transacao"], "%d/%m/%Y")
                new_trans["data_transacao"] = dt_obj.strftime("%Y-%m-%d")
        except:
            pass
            
        transactions.append(new_trans)

    def process_pdf(self, pdf_path: str) -> pd.DataFrame:
        """
        Process a PDF file and return a pandas DataFrame with the transactions.

        Returns:
            pd.DataFrame: DataFrame with columns:
                - arquivo
                - data_emissao
                - data_vencimento
                - valor_total_declarado
                - nome_cliente
                - cartao_principal
                - titular_cartao
                - final_cartao
                - internacional
                - data_transacao
                - estabelecimento
                - categoria
                - parcela
                - valor

        Example Output:
            | arquivo | data_emissao | ... | estabelecimento | valor |
            |---------|--------------|-----|-----------------|-------|
            | Fatura..| 26/06/2025   | ... | IFD*BARRESTAUR..| 30.92 |
        """
        filename = os.path.basename(pdf_path)
        logging.info(f"Iniciando processamento (TEXT): {filename}")
        
        transactions = []
        header_info = {
            "valor_total_declarado": 0.0,
            "data_emissao": None,
            "data_vencimento": None,
            "nome_cliente": "UNKNOWN",
            "cartao_principal": "UNKNOWN"
        }
        
        current_card_holder = "Unknown"
        current_card_number = "Unknown"
        current_card_is_international = False
        is_international_section = False
        ignore_section = False
        in_summary_section = False
        in_launches_section = False
        seen_total_section_full = False
        seen_total_section_partial = False
        after_partial_total = False
        block_card_number = None
        block_target = None
        block_sum = 0.0
        block_ps_index = None
        ps_total_agg = 0.0
        in_ps_section = False
        last_seen_date_str = None
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if len(pdf.pages) > 0:
                    first_page_text = self.extract_page_text(pdf.pages[0], 0)
                    header_info = self.extract_header_info(first_page_text)
                    
                    # Check for Saldo Financiado / Previous Balance in Header text
                    tnorm_hdr = first_page_text.replace(" ", "").lower()
                    ms = re.search(r'(?:saldofinanciado|saldoanterior).*?(-?[\d\.,]+)', tnorm_hdr)
                    if ms:
                        saldo_financiado = self.parse_money(ms.group(1))
                        if saldo_financiado != 0:
                            # Determine date (use header info)
                            dt_trans = header_info.get("data_vencimento")
                            if dt_trans:
                                try:
                                    dt_obj = datetime.strptime(dt_trans, "%d/%m/%Y")
                                    dt_trans = dt_obj.strftime("%Y-%m-%d")
                                except:
                                    pass
                            
                            transactions.append({
                                "arquivo": filename,
                                "data_emissao": header_info.get("data_emissao"),
                                "data_vencimento": header_info.get("data_vencimento"),
                                "valor_total_declarado": header_info.get("valor_total_declarado"),
                                "nome_cliente": header_info.get("nome_cliente"),
                                "cartao_principal": header_info.get("cartao_principal"),
                                "titular_cartao": header_info.get("nome_cliente"),
                                "final_cartao": header_info.get("cartao_principal")[-4:] if header_info.get("cartao_principal") != "UNKNOWN" else "XXXX",
                                "internacional": False,
                                "data_transacao": dt_trans,
                                "estabelecimento": "Saldo Financiado Anterior",
                                "categoria": "Financeiro",
                                "parcela": None,
                                "valor": saldo_financiado
                            })
                            logging.info(f"Extracted Saldo Financiado Anterior: {saldo_financiado}")

                    if header_info["nome_cliente"] != "UNKNOWN":
                        current_card_holder = header_info["nome_cliente"]
                    if header_info["cartao_principal"] != "UNKNOWN":
                        current_card_number = header_info["cartao_principal"][-4:]

                page_texts = []
                for page_num, page in enumerate(pdf.pages):
                    ignore_section = False
                    in_summary_section = False
                    in_launches_section = False
                    after_partial_total = False
                    
                    text = self.extract_page_text(page, page_num)
                    if not text:
                        continue
                    page_texts.append(text)
                    
                    lines = text.split('\n')
                    for line in lines:
                        line_norm = line.replace(" ", "").lower()
                        # Check for Repasse de IOF (International)
                        if "repassedeiof" in line_norm:
                            match_iof_rep = re.search(r'repassedeiof.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line_norm)
                            if match_iof_rep:
                                val_str = match_iof_rep.group(1)
                                valor = self.parse_money(val_str)
                                
                                # Use emission date or None if not available
                                dt_trans = header_info.get("data_emissao")
                                if dt_trans:
                                    try:
                                        dt_obj = datetime.strptime(dt_trans, "%d/%m/%Y")
                                        dt_trans = dt_obj.strftime("%Y-%m-%d")
                                    except:
                                        pass

                                transactions.append({
                                    "arquivo": filename,
                                    "data_emissao": header_info.get("data_emissao"),
                                    "data_vencimento": header_info.get("data_vencimento"),
                                    "valor_total_declarado": header_info.get("valor_total_declarado"),
                                    "nome_cliente": header_info.get("nome_cliente"),
                                    "cartao_principal": current_card_number,
                                    "titular_cartao": current_card_holder,
                                    "final_cartao": current_card_number,
                                    "internacional": True,
                                    "data_transacao": dt_trans,
                                    "estabelecimento": "IOF INTERNACIONAL",
                                    "categoria": "IOF",
                                    "parcela": None,
                                    "valor": valor
                                })
                                logging.info(f"Extracted IOF Repasse: {valor}")
                                continue

                        if "lançamentos" in line_norm or "lancamentos" in line_norm or "transações" in line_norm or "transacoes" in line_norm or "minhasdespesas" in line_norm:
                            ignore_section = False
                            in_summary_section = False
                            in_launches_section = True
                            after_partial_total = False

                        if any(term in line_norm for term in ["resumodafatura", "demonstrativodeencargos", "resumodespesas"]):
                            in_summary_section = True
                            in_ps_section = False

                        current_line_is_total = False
                        if "produtoseservicos" in line_norm or "produtoseserviços" in line_norm:
                            is_international_section = False
                            current_card_is_international = False
                            in_ps_section = True
                            
                            if header_info.get("cartao_principal") != "UNKNOWN":
                                current_card_number = header_info["cartao_principal"][-4:]
                            if header_info.get("nome_cliente") != "UNKNOWN":
                                current_card_holder = header_info["nome_cliente"]

                        if any(term in line_norm for term in [
                            "preparamosoutrasopções", "opçõesdepagamento", "pagamentomínimo", "paguesuafatura",
                            "limitesdecrédito", "simulação", "totaldoslançamentosatuais", "totalparapróximasfaturas",
                            "lançamentosfuturos", "comprasparceladas", "demaisfaturas", "parcelasfuturas"
                        ]):
                            ignore_section = True
                            in_ps_section = False
                            if "totaldoslançamentosatuais" in line_norm:
                                current_line_is_total = True
                                match_idx = line_norm.find("totaldoslançamentosatuais")
                                if match_idx < 5:
                                    seen_total_section_full = True
                                else:
                                    seen_total_section_partial = True
                                    after_partial_total = True
                                in_launches_section = False
                        
                        if seen_total_section_full and not current_line_is_total:
                            continue
                        if after_partial_total:
                            if "lançamentos" in line_norm or "lancamentos" in line_norm or "transações" in line_norm or "transacoes" in line_norm or "minhasdespesas" in line_norm:
                                after_partial_total = False
                                in_launches_section = True
                            else:
                                continue

                        is_trans_line = re.search(r'^\s*(\d{2}/\d{2})\b', line) or re.search(r'^\s*(IOF|TAR)\b', line)

                        if ignore_section:
                            if "totaldoslançamentosatuais" in line_norm:
                                pass
                            else:
                                continue
                        
                        if not in_launches_section and is_trans_line and not after_partial_total:
                            in_launches_section = True
                        
                        if in_summary_section:
                            # Summary logic skipped for simplicity in API ETL unless needed
                            continue

                        # Transaction Extraction Logic
                        if in_launches_section or is_trans_line:
                            event = {'type': None}
                            
                            # Match logic
                            match_card = re.search(r'(?:cartão|final)\s*(?:xxxx\s*xxxx\s*xxxx\s*)?(\d{4})', line, re.IGNORECASE)
                            if match_card:
                                event = {'type': 'card_header', 'match': match_card}
                            elif "internacional" in line_norm:
                                event = {'type': 'international_header'}
                            else:
                                match_trans = re.search(r'(\d{2}/\d{2})\s+(.*?)\s+(-?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2})(?!\s*%)', line)
                                match_iof = None
                                if not match_trans:
                                    match_iof = re.search(r'(IOF\s+.*?|TAR\s+.*?)\s+(-?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2})(?!\s*%)', line)

                                if match_trans:
                                    event = {'type': 'transaction', 'match': match_trans, 'has_date': True}
                                elif match_iof:
                                    event = {'type': 'transaction', 'match': match_iof, 'has_date': False}

                            if event['type'] == 'international_header':
                                is_international_section = True
                                current_card_is_international = True
                            
                            elif event['type'] == 'card_header':
                                candidate_card = event['match'].group(1)
                                candidate_name = line[:event['match'].start()].strip()
                                
                                if ("LANÇAMENTOS" in candidate_name.upper() or "CARTÃO" in candidate_name.upper()) and len(candidate_name) < 25:
                                    pass
                                elif len(candidate_name) < 2:
                                    pass
                                else:
                                    in_ps_section = False
                                    clean_name = re.sub(r'^.*[:;,]\s*', '', candidate_name)
                                    clean_name = re.sub(r'.*\d{2}/\d{2}.*?\d+[,.]\d+\s*', '', clean_name)
                                    
                                    if len(clean_name) > 2:
                                        # Close previous block logic
                                        if block_card_number is not None and block_target is not None and block_ps_index is not None:
                                            try:
                                                ps_val = transactions[block_ps_index]["valor"]
                                                sum_without_ps = block_sum - ps_val
                                                if abs(sum_without_ps - block_target) < abs(block_sum - block_target) - 0.001:
                                                    transactions.pop(block_ps_index)
                                                    block_sum = sum_without_ps
                                            except Exception:
                                                pass

                                        current_card_holder = clean_name.strip()
                                        current_card_number = candidate_card
                                        block_card_number = current_card_number
                                        block_target = None
                                        block_sum = 0.0
                                        block_ps_index = None
                                        
                                        m_sub = re.search(r'final\s*' + candidate_card + r'[^\d]*(-?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2})(?!\s*%)', line)
                                        if m_sub:
                                            block_target = self.parse_money(m_sub.group(1))

                                        if is_international_section:
                                            current_card_is_international = True
                                            is_international_section = False
                                        else:
                                            current_card_is_international = False
                                        
                                        seen_total_section_full = False 
                                        seen_total_section_partial = False

                            elif event['type'] == 'transaction':
                                match = event['match']
                                if event.get('has_date', True):
                                    dt_str, desc, val_str = match.groups()
                                    last_seen_date_str = dt_str
                                else:
                                    desc, val_str = match.groups()
                                    dt_str = last_seen_date_str if 'last_seen_date_str' in locals() and last_seen_date_str else None
                                    if not dt_str and header_info.get("data_emissao"):
                                        try:
                                            dt_str = datetime.strptime(header_info["data_emissao"], "%d/%m/%Y").strftime("%d/%m")
                                        except:
                                            pass

                                desc = desc.strip()
                                
                                if not re.search(r'\d', val_str):
                                    continue
                                
                                val_str_clean = val_str.replace(" ", "")
                                valor = self.parse_money(val_str_clean)
                                
                                if valor < 0 and ("PAGAMENTO" in desc.upper() or "DEBITO AUT" in desc.upper()):
                                    continue
                                
                                if current_line_is_total and abs(valor - header_info.get("valor_total_declarado", 0)) < 1.0:
                                     continue

                                parcela = None
                                match_parc = re.search(r'(\d{2}/\d{2})$', desc)
                                if match_parc:
                                    parcela = match_parc.group(1)

                                data_transacao = dt_str
                                is_future = False
                                
                                if header_info.get("data_vencimento"):
                                    try:
                                        data_vencimento_dt = datetime.strptime(header_info["data_vencimento"], "%d/%m/%Y")
                                        day, month = map(int, dt_str.split('/'))
                                        year = data_vencimento_dt.year
                                        
                                        delta_month = month - data_vencimento_dt.month
                                        
                                        if delta_month > 0:
                                            if delta_month < 6:
                                                is_future = True
                                            else:
                                                year -= 1
                                        elif delta_month == 0:
                                            if day > data_vencimento_dt.day:
                                                is_future = True
                                        
                                        if not is_future:
                                            data_transacao_obj = datetime(year, month, day)
                                            data_transacao = data_transacao_obj.strftime("%Y-%m-%d")
                                            
                                    except ValueError:
                                        pass
                                
                                if is_future:
                                    continue

                                is_iof = "IOF" in desc.upper()
                                
                                if in_ps_section:
                                    ps_total_agg += valor
                                
                                transactions.append({
                                    "arquivo": filename,
                                    "data_emissao": header_info.get("data_emissao"),
                                    "data_vencimento": header_info.get("data_vencimento"),
                                    "valor_total_declarado": header_info.get("valor_total_declarado"),
                                    "nome_cliente": header_info.get("nome_cliente"),
                                    "cartao_principal": header_info.get("cartao_principal"),
                                    "titular_cartao": current_card_holder,
                                    "final_cartao": current_card_number,
                                    "internacional": current_card_is_international or is_iof,
                                    "data_transacao": data_transacao,
                                    "estabelecimento": desc,
                                    "categoria": self.categorize_transaction(desc),
                                    "parcela": parcela,
                                    "valor": valor
                                })
                                try:
                                    if block_card_number == current_card_number:
                                        block_sum += valor
                                except:
                                    pass

            if block_card_number is not None and block_target is not None and block_ps_index is not None:
                try:
                    ps_val = transactions[block_ps_index]["valor"]
                    sum_without_ps = block_sum - ps_val
                    if abs(sum_without_ps - block_target) < abs(block_sum - block_target) - 0.001:
                        transactions.pop(block_ps_index)
                except Exception:
                    pass

        except Exception as e:
            logging.error(f"Erro ao processar {filename}: {str(e)}")
            return pd.DataFrame()

        # Fallback para Extração Genérica
        if not transactions or header_info["valor_total_declarado"] == 0:
            try:
                if not page_texts:
                    with pdfplumber.open(pdf_path) as pdf:
                        page_texts = [p.extract_text() or "" for p in pdf.pages]

                if page_texts:
                    header_info = self.extract_generic_header(page_texts[0])
                    transactions = self.extract_generic_transactions(page_texts, filename, header_info)
            except Exception as e:
                logging.error(f"Erro no fallback genérico: {e}")

        # Final Reconciliation Step
        transactions = self.reconcile_discrepancies(transactions, header_info, page_texts)

        return pd.DataFrame(transactions)

def process_files_to_csv(file_paths: Union[str, List[str]]) -> Dict[str, str]:
    """
    Process one or more PDF files and return their extracted data as CSV strings.
    
    Args:
        file_paths: A single file path string or a list of file path strings.
        
    Returns:
        A dictionary where keys are filenames and values are CSV content strings.
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]
        
    processor = InvoiceProcessor()
    results = {}
    
    for path in file_paths:
        if not os.path.exists(path):
            logging.warning(f"File not found: {path}")
            continue
            
        df = processor.process_pdf(path)
        if not df.empty:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            results[os.path.basename(path)] = csv_buffer.getvalue()
        else:
            results[os.path.basename(path)] = ""
            
    return results

if __name__ == "__main__":
    # Exemplo de uso e verificação
    import sys
    
    # Caminho padrão para teste local
    default_path = "/Users/stwgabriel/Documents/Development/Fullstack/ETLS/data/Faturas/Fatura_MASTERCARD_100482993583_07-2025_unlocked.pdf"
    
    target_path = default_path
    if len(sys.argv) > 1:
        target_path = sys.argv[1]
        
    if os.path.exists(target_path):
        print(f"Processando arquivo: {target_path}")
        processor = InvoiceProcessor()
        df_result = processor.process_pdf(target_path)
        
        print("\n--- Informações do DataFrame ---")
        print(df_result.info())
        
        print("\n--- Primeiras 5 linhas ---")
        print(df_result.head())
        
        print("\n--- Exemplo de Transação Específica (IFD*BARRESTAURANTE) ---")
        filtered = df_result[df_result['estabelecimento'].str.contains("IFD*BARRESTAURANTE", regex=False, na=False)]
        if not filtered.empty:
            print(filtered[['data_transacao', 'estabelecimento', 'valor', 'final_cartao']])
        else:
            print("Transação 'IFD*BARRESTAURANTE' não encontrada.")
    else:
        print(f"Arquivo não encontrado: {target_path}")
