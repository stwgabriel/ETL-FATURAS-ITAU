import sys
import os
import glob

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))
from etl_faturas import InvoiceProcessor

def process_directory(directory_path):

    pdf_files = glob.glob(os.path.join(directory_path, "*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {directory_path}")
        return

    print(f"Found {len(pdf_files)} PDF files in {directory_path}")
    print("-" * 50)

    processor = InvoiceProcessor()
    summary = {
        "total": 0,
        "passed": 0,
        "failed": 0
    }
    
    report_lines = []
    report_lines.append("# Relatório de Validação do ETL (TEXT)")
    report_lines.append("")
    from datetime import datetime
    report_lines.append(f"**Data de Execução**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    report_lines.append("| Arquivo | Status | Total Declarado | Total Extraído | Diferença |")
    report_lines.append("|---|---|---|---|---|")

    for file_path in sorted(pdf_files):
        filename = os.path.basename(file_path)
        print(f"\nProcessing: {filename}")
        
        try:
            result = processor.process_pdf(file_path)
            summary["total"] += 1
            
            if result:
                validation = result["validation"]
                status = validation['status']
                
                if status == "OK":
                    summary["passed"] += 1
                    status_icon = "✅ OK"
                else:
                    summary["failed"] += 1
                    status_icon = "⚠️ DISCREPANCIA"

                print(f"Status: {status}")
                print(f"Declared: {validation['total_declarado']:.2f} | Calculated: {validation['total_extraido']:.2f} | Diff: {validation['diff']:.2f}")
                
                # Add to report line
                report_lines.append(f"| {filename} | {status_icon} | {validation['total_declarado']:.2f} | {validation['total_extraido']:.2f} | {validation['diff']:.2f} |")

                # Calculate sum per card from transactions for extra verification
                df = result["dataframe"]
                if not df.empty:
                    print("Calculated Sum per Card:")
                    card_sums = df.groupby("final_cartao")["valor"].sum()
                    for card, val in card_sums.items():
                        print(f"  - Card {card}: {val:.2f}")
            else:
                summary["failed"] += 1
                status_icon = "❌ ERRO"
                report_lines.append(f"| {filename} | {status_icon} | 0.00 | 0.00 | 0.00 |")
                print("❌ Processing failed (No result returned)")

        except Exception as e:
            summary["failed"] += 1
            status_icon = "❌ ERRO"
            report_lines.append(f"| {filename} | {status_icon} | 0.00 | 0.00 | 0.00 |")
            print(f"❌ Error processing file: {str(e)}")

    print("\n" + "=" * 50)
    print("FINAL SUMMARY")
    print("=" * 50)
    print(f"Total Files: {summary['total']}")
    print(f"Passed:      {summary['passed']} ✅")
    print(f"Failed:      {summary['failed']} ❌")
    print("=" * 50)
    
    # Write report to file
    output_report_path = os.path.join(os.getcwd(), "build", "logs", "VALIDATION_REPORT_TEXT.md")
    os.makedirs(os.path.dirname(output_report_path), exist_ok=True)
    
    with open(output_report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    
    print(f"\nReport saved to: {output_report_path}")

if __name__ == "__main__":
    # Directory containing the PDFs
    if len(sys.argv) > 1:
        INVOICES_DIR = sys.argv[1]
    else:
        INVOICES_DIR = "/Users/stwgabriel/Documents/Development/Fullstack/ETLS/data/Faturas"
    process_directory(INVOICES_DIR)
