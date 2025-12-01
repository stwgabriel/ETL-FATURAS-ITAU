import pdfplumber
import os
import re
import pandas as pd
import sqlite3
import logging
from datetime import datetime
import shutil

# Configuração de Logging
logging.basicConfig(
    filename='build/logs/etl_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s: %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class InvoiceProcessor:
    def __init__(self, output_dir="build/output/faturas_processadas"):
        self.output_dir = output_dir
        self.categories = {
            "Transporte": ["UBER", "99POP", "99APP", "99RIDE", "99PAY", "METRO", "VELOE", "SEM PARAR", "POSTO", "SHELL", "IPIRANGA", "ESTACIONAMENTO", "LOCALIZA", "MOVIDA", "UNIDAS", "WHOOSH"],
            "Alimentação": ["IFOOD", "IFD", "RAPPI", "UBER EATS", "BURGER", "MC DONALDS", "MCDONALDS", "OUTBACK", "RESTAURANTE", "PADARIA", "MERCADO", "SUPERMERCADO", "MUNDIAL", "ZONA SUL", "PAO DE ACUCAR", "PAODEACUCAR", "PDA", "MINUTO", "MINUTOPA", "ASSAI", "CARREFOUR", "EXTRA", "HORTIFRUTI", "BEBIDAS", "BAR", "BISTRO", "DOCES", "GIGANTE", "GRUPO FARTURA", "CONFIANCA", "SODEXO", "ZIG", "COLODEMAE", "SAMBADAROSA", "SKINA", "TORTA"],
            "Saúde": ["DROGARIA", "FARMACIA", "RAIA", "PACHECO", "VENANCIO", "HOSPITAL", "CLINICA", "LABORATORIO", "CONSULTORIO", "RD SAUDE", "RDSAUDE", "VETERINARIO", "VETERINARIOSA", "WELLHUB", "GYMPASS", "SPORTCLUB"],
            "Serviços/Assinaturas": ["NETFLIX", "SPOTIFY", "AMAZON PRIME", "CLARO", "VIVO", "TIM", "OI", "INTERNET", "TV", "APPLE", "GOOGLE", "CLUBE", "LIVELO", "YELUM", "SEGURADORA", "SEGURO", "KEYDROP"],
            "Compras": ["AMAZON", "MERCADO LIVRE", "MELI", "MAGALU", "SHOPEE", "ALIEXPRESS", "SHEIN", "ZARA", "RENNER", "C&A", "RIACHUELO", "DECATHLON", "CENTAURO", "NETSHOES", "VANS", "VIVARA", "FAST SHOP", "FASTSHOP", "AZEVEDO", "LUIS FELIPPE", "LUISFELIPPE", "COMPRA DE PONTOS", "AQUINO", "OUTLET", "MODA"],
            "Viagem": ["HOTEL", "AIRBNB", "BOOKING", "CVC", "LATAM", "GOL", "AZUL", "PASSAGEM", "IBIS", "INGRESSE"],
            "Financeiro": ["IOF", "ENCARGOS", "MULTA", "JUROS", "ANUIDADE"]
        }
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.md_output_dir = "build/output/faturas_md"
        if not os.path.exists(self.md_output_dir):
            os.makedirs(self.md_output_dir)

        self.ocr_available = False
        try:
            import importlib
            self.pytesseract = importlib.import_module('pytesseract')
            self.pdf2image = importlib.import_module('pdf2image')
            self.PIL_Image = importlib.import_module('PIL.Image')
            self.ocr_available = shutil.which('tesseract') is not None
            if not self.ocr_available:
                logging.warning('Tesseract não encontrado no PATH; OCR fallback indisponível')
        except Exception:
            logging.info('Bibliotecas de OCR não disponíveis; prosseguindo sem OCR')

    def extract_page_text(self, pdf_path, page_index, page_obj, use_ocr=False):
        text = page_obj.extract_text() or ""
        if use_ocr and self.ocr_available:
            try:
                images = self.pdf2image.convert_from_path(pdf_path, first_page=page_index + 1, last_page=page_index + 1, dpi=300)
                if images:
                    ocr_text = self.pytesseract.image_to_string(images[0], lang='por+eng')
                    if ocr_text and len(ocr_text.strip()) > len(text.strip()):
                        return ocr_text
                    # Caso OCR seja menor, mescla para capturar tokens perdidos
                    return text + ("\n" + ocr_text if ocr_text else "")
            except Exception as e:
                logging.debug(f"Falha no OCR da página {page_index+1}: {str(e)}")
        return text

    def categorize_transaction(self, description):
        desc_upper = description.upper()
        for category, keywords in self.categories.items():
            for keyword in keywords:
                if keyword in desc_upper:
                    return category
        return "Outros"

    def parse_money(self, value_str):
        try:
            clean_str = value_str.replace('.', '').replace(',', '.')
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
        
        # Pattern 1: Standard "Total desta fatura" line (handling spaces)
        re_total = re.search(r'Total\s*desta\s*fatura\s*([\d\.,]+)', text)
        
        # Pattern 2: Header block "O total da sua fatura é:" followed by value (potentially on next line)
        if not re_total:
             re_total = re.search(r'O total da sua fatura é:[\s\S]*?R\$\s*([\d\.,]+)', text)
        
        if re_total:
            info["valor_total_declarado"] = self.parse_money(re_total.group(1))
        else:
            tnorm = text.replace(" ", "").lower()
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

    def process_pdf(self, pdf_path, use_ocr=False):
        filename = os.path.basename(pdf_path)
        logging.info(f"Iniciando processamento: {filename}")
        
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
        card_subtotals = {}
        outros_agg = 0.0
        intern_trans_agg = 0.0
        intern_lanc_agg = 0.0
        pagamento_efetuado = 0.0
        ps_total_agg = 0.0
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Extração do Cabeçalho (Página 1)
                if len(pdf.pages) > 0:
                    first_page_text = self.extract_page_text(pdf_path, 0, pdf.pages[0], use_ocr) or ""
                    header_info = self.extract_header_info(first_page_text)
                    try:
                        tnorm_hdr = first_page_text.replace(" ", "").lower()
                        mp = re.search(r'pagamentoefetuado.*?-([\d\.,]+)', tnorm_hdr)
                        if mp:
                            pagamento_efetuado = self.parse_money(mp.group(1))
                    except:
                        pass
                    
                    if header_info["nome_cliente"] != "UNKNOWN":
                        current_card_holder = header_info["nome_cliente"]
                    if header_info["cartao_principal"] != "UNKNOWN":
                        current_card_number = header_info["cartao_principal"][-4:]

                # Processamento das páginas
                page_texts = []
                for page_num, page in enumerate(pdf.pages):
                    # Resetar flag de ignorar seção a cada nova página para evitar ignorar conteúdo válido indevidamente
                    ignore_section = False
                    in_summary_section = False
                    in_launches_section = False
                    after_partial_total = False
                    
                    text = self.extract_page_text(pdf_path, page_num, page, use_ocr)
                    if not text:
                        continue
                    page_texts.append(text)
                    
                    lines = text.split('\n')
                    for line in lines:
                        # Normalizar linha para verificação de keywords (remover espaços e lowercase) para lidar com texto mergeado
                        line_norm = line.replace(" ", "").lower()
                        
                        # Identificação de Blocos de Cartão (MOVIDO PARA O INÍCIO)
                        match_card_header = re.search(r'(?:^|\s)(?<!\d)(?:Lançamentos\s*no\s*cartão\s*)?([A-Z\s\.]+?)\(?final\s*(\d{4})\)?', line, re.IGNORECASE)
                        if match_card_header:
                            # Finaliza bloco anterior (se houver) removendo Produtos e serviços se piorou o erro
                            if block_card_number is not None and block_target is not None and block_ps_index is not None:
                                try:
                                    ps_val = transactions[block_ps_index]["valor"]
                                    sum_without_ps = block_sum - ps_val
                                    if abs(sum_without_ps - block_target) < abs(block_sum - block_target) - 0.001:
                                        transactions.pop(block_ps_index)
                                        block_sum = sum_without_ps
                                        logging.info(f"Removido 'Produtos e serviços' do bloco {block_card_number} para reconciliar subtotal")
                                except Exception as e:
                                    logging.debug(f"Falha ao ajustar bloco {block_card_number}: {e}")
                            candidate_name = match_card_header.group(1).strip()
                            candidate_card = match_card_header.group(2)
                            
                            if "LANÇAMENTOS" in candidate_name.upper() or len(candidate_name) < 3:
                                pass
                            else:
                                clean_name = re.sub(r'.*\d{2}/\d{2}.*?\d+[,.]\d+\s*', '', candidate_name)
                                current_card_holder = clean_name.strip()
                                current_card_number = candidate_card
                                block_card_number = current_card_number
                                m_sub = re.search(r'final\s*\d{4}[^\d]*(-?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2})(?!\s*%)', line)
                                block_target = self.parse_money(m_sub.group(1)) if m_sub else None
                                block_sum = 0.0
                                block_ps_index = None
                                if block_target is not None:
                                    card_subtotals[block_card_number] = block_target
                                is_international_section = False
                                seen_total_section_full = False 
                                seen_total_section_partial = False
                                logging.info(f"Novo bloco de cartão: {current_card_holder} (resetando seen_total_section)")

                        # Reativar leitura se encontrar cabeçalho de lançamentos
                        if "lançamentos" in line_norm or "lancamentos" in line_norm or "transações" in line_norm or "transacoes" in line_norm or "minhasdespesas" in line_norm:
                            ignore_section = False
                            in_summary_section = False
                            in_launches_section = True
                            after_partial_total = False
                            logging.info(f"Reativando leitura na página {page_num+1}: {line.strip()}")

                        # Detecção de Seções de Resumo (evitar duplicidade de encargos)
                        if any(term in line_norm for term in ["resumodafatura", "encargoscobrados", "demonstrativodeencargos", "resumodespesas"]):
                            in_summary_section = True
                            logging.debug(f"Entrou em seção de resumo na página {page_num+1}: {line.strip()}")

                        current_line_is_total = False
                        # Verificação de seções a ignorar
                        if ("lancamentosprodutoseservicos" in line_norm or "lançamentosprodutoseserviços" in line_norm):
                            mv = re.search(r'(-?[\d\.,]+)(?!\s*%)', line)
                            if mv:
                                valor_ps = self.parse_money(mv.group(1))
                                already_ps = any(
                                    t.get("final_cartao") == current_card_number and t.get("estabelecimento") == "Produtos e serviços"
                                    for t in transactions
                                )
                                if not already_ps:
                                    transactions.append({
                                        "arquivo": filename,
                                        "data_emissao": header_info.get("data_emissao"),
                                        "data_vencimento": header_info.get("data_vencimento"),
                                        "valor_total_declarado": header_info.get("valor_total_declarado"),
                                        "nome_cliente": header_info.get("nome_cliente"),
                                        "cartao_principal": header_info.get("cartao_principal"),
                                        "titular_cartao": current_card_holder,
                                        "final_cartao": current_card_number,
                                        "internacional": False,
                                        "data_transacao": header_info.get("data_vencimento"),
                                        "estabelecimento": "Produtos e serviços",
                                        "categoria": "Outros",
                                        "parcela": None,
                                        "valor": valor_ps
                                    })

                        if any(term in line_norm for term in [
                            "preparamosoutrasopções",
                            "opçõesdepagamento",
                            "pagamentomínimo",
                            "paguesuafatura",
                            "limitesdecrédito",
                            "simulação",
                            "totaldoslançamentosatuais",
                            "totalparapróximasfaturas",
                            "lançamentosfuturos",
                            "comprasparceladas",
                            "demaisfaturas"
                        ]):
                            ignore_section = True
                            if "totaldoslançamentosatuais" in line_norm:
                                current_line_is_total = True
                                # Verifica se é FULL (início da linha) ou PARTIAL (meio da linha)
                                match_idx = line_norm.find("totaldoslançamentosatuais")
                                if match_idx < 5: # Permite pequenos artefatos no início
                                    seen_total_section_full = True
                                    logging.info(f"Fim da seção de lançamentos (FULL) detectado na página {page_num+1}")
                                else:
                                    seen_total_section_partial = True
                                    after_partial_total = True
                                    logging.info(f"Fim da seção de lançamentos (PARTIAL) detectado na página {page_num+1}")
                                in_launches_section = False
                                

                            logging.info(f"Iniciando seção ignorada na página {page_num+1}: {line.strip()}")
                        
                        # Se a seção de lançamentos acabou (FULL), ignorar transações
                        if seen_total_section_full and not current_line_is_total:
                            logging.debug(f"Ignorando linha pós-total (FULL): {line.strip()}")
                            continue
                        # Se detectamos TOTAL parcial, só retomamos após novo cabeçalho de lançamentos (não por trans_line)
                        if after_partial_total:
                            if "lançamentos" in line_norm or "lancamentos" in line_norm or "transações" in line_norm or "transacoes" in line_norm or "minhasdespesas" in line_norm:
                                after_partial_total = False
                                in_launches_section = True
                            else:
                                continue

                        # Detecta se a linha parece transação (data no início da linha)
                        is_trans_line = re.search(r'^\s*(\d{2}/\d{2})\b', line)

                        if ignore_section:
                            if "totaldoslançamentosatuais" in line_norm:
                                pass
                            else:
                                if any(term in line_norm for term in ["comprasparceladas", "demaisfaturas", "totalparapróximasfaturas"]):
                                    continue
                                if not is_trans_line:
                                    continue
                        
                        # Ativa seção de lançamentos ao encontrar uma linha de transação (apenas se não estivermos após total parcial)
                        if not in_launches_section and is_trans_line and not after_partial_total:
                            in_launches_section = True
                        
                        # Identificação de Seção Internacional
                        if "Transações Internacionais" in line or "Lançamentos internacionais" in line:
                            is_international_section = True

                        # Ignorar linhas irrelevantes
                        if "Saldofinanciado" in line or "Pagamento" in line or ("totalapagar" in line_norm):
                            continue

                        # Fora da seção de lançamentos, pular
                        if not in_launches_section:
                            continue

                        # Regex de Transação (data em qualquer posição; evita capturar percentuais)
                        matches = list(re.finditer(r'(\d{2}/\d{2})\s+(.*?)\s+(-?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2})(?!\s*%)', line))
                        
                        # Se estivermos em seção PARTIAL (coluna da direita é Futuro), limitamos a 1 match por linha
                        # para evitar pegar a coluna da direita
                        if seen_total_section_partial and len(matches) > 1:
                             logging.debug(f"Limitando matches em seção PARTIAL: {len(matches)} -> 1")
                             matches = matches[:1]

                        for match in matches:
                            dt_str, desc, val_str = match.groups()
                            desc = desc.strip()
                            
                            if not re.search(r'\d', val_str):
                                continue
                            
                            # Normalizar valor (remover espaços após o menos)
                            val_str_clean = val_str.replace(" ", "")
                            valor = self.parse_money(val_str_clean)
                            
                            # Ignorar pagamentos de fatura
                            if valor < 0 and ("PAGAMENTO" in desc.upper() or "DEBITO AUT" in desc.upper()):
                                logging.info(f"Ignorando pagamento de fatura: {desc} {valor}")
                                continue
                            
                            # Ignorar o próprio valor do TOTAL se for capturado (raro, pois regex exige data, mas vai que...)
                            if current_line_is_total and abs(valor - header_info.get("valor_total_declarado", 0)) < 1.0:
                                 continue

                            parcela = None
                            match_parc = re.search(r'(\d{2}/\d{2})$', desc)
                            if match_parc:
                                parcela = match_parc.group(1)

                            # Inferência de Ano e Filtragem de Futuros
                            data_transacao = dt_str
                            is_future = False
                            
                            if header_info.get("data_vencimento"):
                                try:
                                    data_vencimento_dt = datetime.strptime(header_info["data_vencimento"], "%d/%m/%Y")
                                    day, month = map(int, dt_str.split('/'))
                                    year = data_vencimento_dt.year
                                    
                                    # Heurística para Futuro vs Passado
                                    delta_month = month - data_vencimento_dt.month
                                    
                                    if delta_month > 0:
                                        if delta_month < 6:
                                            # Ex: Venc=10 (Oct), Trans=12 (Dec). Delta=2. FUTURO.
                                            is_future = True
                                        else:
                                            # Ex: Venc=01 (Jan), Trans=12 (Dec). Delta=11. PASSADO (Ano anterior).
                                            year -= 1
                                    elif delta_month == 0:
                                        if day > data_vencimento_dt.day:
                                            # Ex: Venc=10/10, Trans=15/10. FUTURO (Próxima fatura).
                                            is_future = True
                                    else:
                                        pass
                                    
                                    if not is_future:
                                        data_transacao_obj = datetime(year, month, day)
                                        data_transacao = data_transacao_obj.strftime("%Y-%m-%d")
                                        
                                except ValueError:
                                    pass
                            
                            if is_future:
                                logging.debug(f"Ignorando transação futura: {dt_str} {desc}")
                                continue

                            is_iof = "IOF" in desc.upper()
                            
                            idx_new = len(transactions)
                            transactions.append({
                                "arquivo": filename,
                                "data_emissao": header_info.get("data_emissao"),
                                "data_vencimento": header_info.get("data_vencimento"),
                                "valor_total_declarado": header_info.get("valor_total_declarado"),
                                "nome_cliente": header_info.get("nome_cliente"),
                                "cartao_principal": header_info.get("cartao_principal"),
                                "titular_cartao": current_card_holder,
                                "final_cartao": current_card_number,
                                "internacional": is_international_section or is_iof,
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

                        if not matches:
                            if "lancamentosprodutoseservicos" in line_norm or "lançamentosprodutoseserviços" in line_norm:
                                m = re.search(r'(-?[\d\.,]+)(?!\s*%)', line)
                                if m:
                                    valor = self.parse_money(m.group(1))
                                    already = any(
                                        t.get("final_cartao") == current_card_number and t.get("estabelecimento") == "Produtos e serviços"
                                        for t in transactions
                                    )
                                    if not already:
                                        idx_ps = len(transactions)
                                        transactions.append({
                                            "arquivo": filename,
                                            "data_emissao": header_info.get("data_emissao"),
                                            "data_vencimento": header_info.get("data_vencimento"),
                                            "valor_total_declarado": header_info.get("valor_total_declarado"),
                                            "nome_cliente": header_info.get("nome_cliente"),
                                            "cartao_principal": header_info.get("cartao_principal"),
                                            "titular_cartao": current_card_holder,
                                            "final_cartao": current_card_number,
                                            "internacional": False,
                                            "data_transacao": header_info.get("data_vencimento"),
                                            "estabelecimento": "Produtos e serviços",
                                            "categoria": "Outros",
                                            "parcela": None,
                                            "valor": valor
                                        })
                                        block_ps_index = idx_ps
                                        try:
                                            if block_card_number == current_card_number:
                                                block_sum += valor
                                                ps_total_agg += valor
                                        except:
                                            pass
                        

            # Finaliza último bloco
            if block_card_number is not None and block_target is not None and block_ps_index is not None:
                try:
                    ps_val = transactions[block_ps_index]["valor"]
                    sum_without_ps = block_sum - ps_val
                    if abs(sum_without_ps - block_target) < abs(block_sum - block_target) - 0.001:
                        transactions.pop(block_ps_index)
                        logging.info(f"Removido 'Produtos e serviços' do bloco final {block_card_number} para reconciliar subtotal")
                except Exception as e:
                    logging.debug(f"Falha ao ajustar bloco final {block_card_number}: {e}")

        except Exception as e:
            logging.error(f"Erro ao processar {filename}: {str(e)}")
            return None

        # Criação do DataFrame da Fatura
        df = pd.DataFrame(transactions)
        
        

        # Validação
        validation_result = self.validate_invoice(filename, df, header_info)
        
        # Salvar CSV individual
        if not df.empty:
            csv_name = os.path.splitext(filename)[0] + ".csv"
            csv_path = os.path.join(self.output_dir, csv_name)
            df.to_csv(csv_path, index=False)
            logging.info(f"Tabela salva em: {csv_path}")
        try:
            md_name = os.path.splitext(filename)[0] + ".md"
            md_path = os.path.join(self.md_output_dir, md_name)
            with open(md_path, "w") as md:
                md.write(f"# {filename}\n\n")
                md.write("## Cabeçalho\n\n")
                md.write(f"- Arquivo: {filename}\n")
                md.write(f"- Data de Emissão: {header_info.get('data_emissao')}\n")
                md.write(f"- Data de Vencimento: {header_info.get('data_vencimento')}\n")
                md.write(f"- Titular: {header_info.get('nome_cliente')}\n")
                md.write(f"- Cartão Principal: {header_info.get('cartao_principal')}\n")
                md.write(f"- Total declarado: {header_info.get('valor_total_declarado'):.2f}\n")
                md.write("\n## Auxiliares\n\n")
                md.write(f"- Pagamento efetuado (header): {pagamento_efetuado:.2f}\n")
                md.write(f"- Total 'Produtos e serviços' agregado: {ps_total_agg:.2f}\n")
                md.write(f"- Total 'Outros lançamentos' (resumo): {outros_agg:.2f}\n")
                md.write(f"- Total transações internacionais (resumo): {intern_trans_agg:.2f}\n")
                md.write(f"- Total lançamentos internacionais (resumo): {intern_lanc_agg:.2f}\n")
                if card_subtotals:
                    md.write("\n### Subtotais por cartão\n\n")
                    for k,v in card_subtotals.items():
                        md.write(f"- final {k}: {v:.2f}\n")
                md.write("\n## Transações\n\n")
                if not df.empty:
                    md.write("| data | estabelecimento | valor | categoria | final_cartao |\n")
                    md.write("|---|---|---:|---|---|\n")
                    for _,row in df.iterrows():
                        md.write(f"| {row.get('data_transacao','')} | {row.get('estabelecimento','')} | {row.get('valor'):.2f} | {row.get('categoria','')} | {row.get('final_cartao','')} |\n")
                else:
                    md.write("Sem transações extraídas\n")
                md.write("\n## Texto por páginas\n\n")
                for i,pt in enumerate(page_texts):
                    md.write(f"### Página {i+1}\n\n")
                    md.write("```\n")
                    md.write(pt)
                    md.write("\n```\n\n")
            logging.info(f"MD salvo em: {md_path}")
        except Exception as e:
            logging.debug(f"Falha ao salvar MD: {e}")

        return {
            "filename": filename,
            "dataframe": df,
            "validation": validation_result,
            "metadata": {**header_info,
                "aux_card_subtotals": card_subtotals,
                "outros_lanc_total": outros_agg,
                "intern_trans_total": intern_trans_agg,
                "intern_lanc_total": intern_lanc_agg,
                "pagamento_efetuado": pagamento_efetuado,
                "produtos_servicos_total": ps_total_agg
            }
        }

    def validate_invoice(self, filename, df, header_info):
        total_declarado = header_info.get("valor_total_declarado", 0.0)
        
        if df.empty:
            return {
                "status": "VAZIO", 
                "diff": 0, 
                "msg": "Nenhuma transação extraída",
                "total_declarado": total_declarado,
                "total_extraido": 0.0
            }
            
        total_extraido = df['valor'].sum()
        
        # Considerando tolerância para possíveis arredondamentos ou taxas não capturadas
        diff = total_extraido - total_declarado
        
        # Ignorar pagamentos (créditos) na soma simples se o total declarado for o valor a pagar
        # Mas aqui estamos somando tudo. Se houver pagamentos na fatura, eles reduzem o total.
        # Assumindo que 'valor' já vem com sinal negativo se for crédito/pagamento.
        
        status = "OK"
        if abs(diff) > 0.50: # Tolerância de 50 centavos
            status = "DISCREPANCIA"
            logging.warning(f"Discrepância em {filename}: Declarado={total_declarado:.2f}, Extraído={total_extraido:.2f}, Diff={diff:.2f}")
        else:
            logging.info(f"Validação OK para {filename}")
            
        return {
            "status": status,
            "total_declarado": total_declarado,
            "total_extraido": total_extraido,
            "diff": diff
        }

def run_etl():
    base_path = "data/Faturas"
    processor = InvoiceProcessor()
    
    dirs = []
    if os.path.exists(base_path):
        dirs.append(base_path)
    alt_path = "data/Faturas" # kept for safety but effectively same
    if os.path.exists(alt_path):
        dirs.append(alt_path)
    if not dirs:
        print("Diretórios não encontrados: data/Faturas")
        return
    files = []
    for d in dirs:
        files += [f for f in os.listdir(d) if f.lower().endswith('.pdf')]
    files = sorted(set(files))
    all_results = []
    master_df = pd.DataFrame()
    
    print(f"Iniciando processamento de {len(files)} arquivos...")
    
    for f in files:
        path = os.path.join(base_path if os.path.exists(os.path.join(base_path, f)) else alt_path, f)
        result = processor.process_pdf(path, use_ocr=True)
        if result:
            all_results.append(result)
            if not result['dataframe'].empty:
                master_df = pd.concat([master_df, result['dataframe']], ignore_index=True)
    
    # Gerar Relatório de Validação
    with open("build/logs/VALIDATION_REPORT.md", "w") as report:
        report.write("# Relatório de Validação do ETL\n\n")
        report.write(f"**Data de Execução**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        report.write("| Arquivo | Status | Total Declarado | Total Extraído | Diferença |\n")
        report.write("|---|---|---|---|---|\n")
        
        for res in all_results:
            val = res['validation']
            icon = "✅" if val['status'] == "OK" else "⚠️" if val['status'] == "DISCREPANCIA" else "❌"
            report.write(f"| {res['filename']} | {icon} {val['status']} | {val['total_declarado']:.2f} | {val['total_extraido']:.2f} | {val['diff']:.2f} |\n")

    # Sem OCR: revalidação desativada
            
    # Salvar Consolidado no SQLite e CSV Master
    if not master_df.empty:
        master_df.to_csv("build/output/faturas_consolidado.csv", index=False)
        
        conn = sqlite3.connect("build/db/faturas.db")
        master_df.to_sql("transacoes", conn, if_exists="replace", index=False)
        conn.close()
        
        print("\nProcessamento concluído!")
        print(f"Tabelas individuais salvas em: {processor.output_dir}/")
        print("Relatório de validação gerado: build/logs/VALIDATION_REPORT.md")
        print("Base consolidada atualizada: build/db/faturas.db")
        # Debug de discrepâncias
        try:
            debug_rows = []
            by_file = master_df.groupby('arquivo')
            for res in all_results:
                val = res['validation']
                if val['status'] == 'OK':
                    continue
                fn = res['filename']
                if fn in by_file.groups:
                    df_file = by_file.get_group(fn)
                    soma_pos = float(df_file[df_file['valor'] > 0]['valor'].sum())
                    soma_neg = float(df_file[df_file['valor'] < 0]['valor'].sum())
                    soma_fin = float(df_file[df_file['categoria'] == 'Financeiro']['valor'].sum())
                    soma_ps = float(df_file[df_file['estabelecimento'] == 'Produtos e serviços']['valor'].sum())
                    qtd = int(len(df_file))
                else:
                    soma_pos = soma_neg = soma_fin = soma_ps = 0.0
                    qtd = 0
                md = res.get('metadata', {})
                cs = md.get('aux_card_subtotals', {})
                debug_rows.append({
                    'arquivo': fn,
                    'status': val['status'],
                    'total_declarado': round(val['total_declarado'],2),
                    'total_extraido': round(val['total_extraido'],2),
                    'diff': round(val['diff'],2),
                    'qtd_transacoes': qtd,
                    'soma_positivos': round(soma_pos,2),
                    'soma_negativos': round(soma_neg,2),
                    'soma_financeiro': round(soma_fin,2),
                    'valor_produtos_servicos': round(soma_ps,2),
                    'pagamentos_header': round(md.get('pagamento_efetuado',0.0),2),
                    'total_outros_agg': round(md.get('outros_lanc_total',0.0),2),
                    'total_intern_trans_agg': round(md.get('intern_trans_total',0.0),2),
                    'total_intern_lanc_agg': round(md.get('intern_lanc_total',0.0),2),
                    'sub_5192': round(cs.get('5192',0.0),2),
                    'sub_5612': round(cs.get('5612',0.0),2),
                    'sub_8223': round(cs.get('8223',0.0),2)
                })
            if debug_rows:
                pd.DataFrame(debug_rows).to_csv('build/logs/debug_discrepancias.csv', index=False)
                print('Debug gerado: build/logs/debug_discrepancias.csv')
        except Exception as e:
            logging.debug(f'Falha ao gerar debug_discrepancias.csv: {e}')
    else:
        print("Nenhum dado foi extraído.")

if __name__ == "__main__":
    run_etl()
                        
