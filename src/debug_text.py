import pdfplumber
import os
import re
import sys

def dump_text(pdf_path):
    print(f"--- DUMPING {os.path.basename(pdf_path)} ---")
    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) > 0:
            print("--- PAGE 1 (HEADER) ---")
            print(pdf.pages[0].extract_text()[:1000]) # First 1000 chars
        
        print("\n--- ALL PAGES TEXT ---")
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text:
                print(f"--- PAGE {i+1} ---")
                lines = text.split('\n')
                for line in lines:
                    # Print lines that look like transactions or totals or headers
                    if re.search(r'\d{2}/\d{2}', line) or "Total" in line or "Resumo" in line or "IOF" in line:
                        print(f"{i+1}: {line}")
    print("--------------------------------------------------\n")

def discrepant_files_from_report():
    report_path = "build/logs/VALIDATION_REPORT.md"
    files = []
    if not os.path.exists(report_path):
        return files
    with open(report_path, "r") as fh:
        for line in fh:
            if line.startswith("|") and ("DISCREPANCIA" in line or "VAZIO" in line):
                parts = [p.strip() for p in line.strip().split("|")]
                if len(parts) >= 3:
                    fname = parts[1]
                    files.append(fname)
    return files

def resolve_paths(fnames):
    resolved = []
    for name in fnames:
        p1 = os.path.join("data/Faturas", name)
        # p2 removal or update
        if os.path.exists(p1):
            resolved.append(p1)
        else:
            print(f"File not found: {name}")
    return resolved

def main():
    fnames = discrepant_files_from_report()
    paths = resolve_paths(fnames)
    for f in paths:
        dump_text(f)

if __name__ == "__main__":
    main()
