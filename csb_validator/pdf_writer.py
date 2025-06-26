from fpdf import FPDF
import os
from typing import List, Tuple, Dict

def write_report_pdf(results: List[Tuple[str, List[Dict[str, any]]]], filename: str, mode: str):
    def safe(text: str) -> str:
        return text.encode("latin-1", "ignore").decode("latin-1")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Courier", "B", 14)
    pdf.cell(200, 10, txt="CSB Validation Summary", ln=True)
    files_with_errors = [r for r in results if r[1]]
    pdf.set_font("Courier", size=10)
    pdf.ln(5)
    pdf.cell(200, 8, txt=f"Total files processed: {len(results)}", ln=True)
    pdf.cell(200, 8, txt=f"Files with errors: {len(files_with_errors)}", ln=True)
    pdf.cell(200, 8, txt=f"Total validation errors: {sum(len(r[1]) for r in results)}", ln=True)
    pdf.ln(8)
    pdf.set_font("Courier", "B", 12)
    pdf.cell(200, 8, txt="Validation Errors:", ln=True)
    pdf.ln(3)
    pdf.set_font("Courier", size=10)
    for file_path, errors in results:
        if not errors:
            continue
        base = os.path.basename(file_path)
        pdf.set_font("Courier", "B", 10)
        pdf.cell(200, 7, txt=f"{base}", ln=True)
        pdf.set_font("Courier", size=10)
        pdf.cell(10, 7, "#", border=1)
        if mode == "trusted-node":
            pdf.cell(160, 7, "Error Message", border=1, ln=True)
        else:
            pdf.cell(30, 7, "Line", border=1)
            pdf.cell(160, 7, "Error Message", border=1, ln=True)
        for idx, err in enumerate(errors, 1):
            line_info = str(err.get("line", "N/A"))
            pdf.cell(10, 6, str(idx), border=1)
            if mode == "trusted-node":
                pdf.cell(160, 6, safe(err["error"][:160]), border=1, ln=True)
            else:
                pdf.cell(30, 6, line_info, border=1)
                pdf.cell(160, 6, safe(err["error"][:160]), border=1, ln=True)
        pdf.ln(5)
    pdf.output(filename)
