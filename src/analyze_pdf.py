import pdfplumber
import os

base_path = "data/Faturas"
files_to_analyze = [
    "Fatura_Itau_20251110-121024.pdf",
    "Fatura_MASTERCARD_100475516765_08-2025_unlocked.pdf"
]

with open("build/logs/analysis_tables_output.txt", "w") as f:
    for file_name in files_to_analyze:
        full_path = os.path.join(base_path, file_name)
        if os.path.exists(full_path):
            f.write(f"--- Extracting tables from: {full_path} ---\n")
            try:
                with pdfplumber.open(full_path) as pdf:
                    for i, page in enumerate(pdf.pages):
                        f.write(f"--- Page {i+1} ---\n")
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                f.write(str(row) + "\n")
                            f.write("\n-- End of Table --\n")
                        f.write("\n\n")
            except Exception as e:
                f.write(f"Error extracting tables: {e}\n")
        else:
            f.write(f"File not found: {full_path}\n")
