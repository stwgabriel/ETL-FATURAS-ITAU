import os
import logging
import shutil
import tempfile
from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
from src.etl_processor import InvoiceProcessor

# Configuração de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ETL Faturas Itau")

# Configurar templates
templates = Jinja2Templates(directory="src/templates")

# Diretório temporário para uploads
UPLOAD_DIR = "temp_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/info", response_class=HTMLResponse)
async def read_info(request: Request):
    return templates.TemplateResponse("docs.html", {"request": request})

@app.post("/api/extract")
async def extract_invoice(file: UploadFile = File(...)):
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Arquivo deve ser um PDF")

    temp_file_path = os.path.join(UPLOAD_DIR, file.filename)
    
    try:
        # Salvar arquivo temporariamente
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # Processar
        processor = InvoiceProcessor()
        df = processor.process_pdf(temp_file_path)
        
        if df.empty:
            raise HTTPException(status_code=500, detail="Falha ao processar PDF ou arquivo vazio")
            
        # Reconstruct validation logic
        total_declarado = df['valor_total_declarado'].iloc[0] if 'valor_total_declarado' in df.columns else 0.0
        total_extraido = df['valor'].sum()
        diff = total_declarado - total_extraido
        status = "OK" if abs(diff) < 1.0 else "DIVERGENTE"

        # Check for Saldo Financiado/Discounts
        discount_note = ""
        if not df.empty:
            saldo_tx = df[df['estabelecimento'].str.contains("Saldo Financiado|Saldo Anterior", case=False, na=False)]
            if not saldo_tx.empty:
                saldo_val = saldo_tx['valor'].sum()
                if saldo_val < 0:
                    discount_note = f" (Incl. Desc/Saldo: {saldo_val:.2f})"

        validation = {
            "total_declarado": total_declarado,
            "total_extraido": total_extraido,
            "diff": diff,
            "status": status,
            "discount_note": discount_note
        }
        
        # Converter NaN para None para JSON válido
        df_dict = df.where(pd.notnull(df), None).to_dict(orient='records')
        
        # Estatísticas para o dashboard
        
        # Convertendo colunas relevantes para numérico se necessário
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)
        
        # Cálculos específicos
        # IOF
        iof_mask = df['estabelecimento'].str.contains('IOF', case=False, na=False)
        total_iof = df[iof_mask]['valor'].sum()
        
        # Internacional (excluindo IOF se quiser separar, mas aqui vamos pegar tudo marcado como internacional)
        total_internacional = df[df['internacional'] == True]['valor'].sum()
        
        # Taxas e Juros (Multa, Juros, Encargos, Anuidade)
        taxas_mask = df['estabelecimento'].str.contains('MULTA|JUROS|ENCARGOS|ANUIDADE', case=False, regex=True, na=False)
        total_taxas_servicos = df[taxas_mask]['valor'].sum()
        
        # Net spend (transactions only, excluding taxes and IOF)
        total_compras = df[~taxas_mask & ~iof_mask]['valor'].sum()
        
        # Compras Parceladas (se parcela não for nulo)
        total_parcelado = df[df['parcela'].notna()]['valor'].sum()

        # Determinar método de extração
        extraction_method = "NATIVO ITAÚ"
        if 'extraction_method' in df.columns and not df.empty:
                if "Generic" in df['extraction_method'].values:
                    extraction_method = "GENÉRICO / OUTRO BANCO"

        stats = {
            "total_declarado": validation['total_declarado'],
            "total_extraido": validation['total_extraido'],
            "total_compras": float(total_compras),
            "diferenca": validation['diff'],
            "status": validation['status'],
            "discount_note": validation.get('discount_note', ""),
            "total_transacoes": len(df),
            "total_iof": float(total_iof),
            "total_internacional": float(total_internacional),
            "total_taxas": float(total_taxas_servicos),
            "total_parcelado": float(total_parcelado),
            "por_categoria": df.groupby('categoria')['valor'].sum().to_dict(),
            "por_titular": df.groupby('titular_cartao')['valor'].sum().to_dict(),
            "metodo_extracao": extraction_method
        }
        
        response_data = {
            "filename": file.filename,
            "statistics": stats,
            "transactions": df_dict,
            "raw_validation": validation
        }
        
        return JSONResponse(content=response_data)

    except Exception as e:
        logger.error(f"Erro ao processar arquivo: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Limpar arquivo temporário
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
