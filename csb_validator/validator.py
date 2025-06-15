import argparse
import json
import os
import subprocess
from typing import List, Dict, Any, Tuple
from fpdf import FPDF
from colorama import Fore, Style, init

init(autoreset=True)

def run_custom_validation(file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
    errors = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for i, feature in enumerate(data.get("features", [])):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])

            if not coords or not isinstance(coords, list) or len(coords) < 2:
                errors.append({"file": file_path, "feature": f"Feature-{i+1}", "error": "Invalid geometry coordinates"})
            else:
                lon, lat = coords[0], coords[1]
                if lon is None or lon < -180 or lon > 180:
                    errors.append({"file": file_path, "feature": f"Feature-{i+1}", "error": f"Longitude out of bounds: {lon}"})
                if lat is None or lat < -90 or lat > 90:
                    errors.append({"file": file_path, "feature": f"Feature-{i+1}", "error": f"Latitude out of bounds: {lat}"})

            depth = props.get("depth")
            if depth is None:
                errors.append({"file": file_path, "feature": f"Feature-{i+1}", "error": "Depth is required"})
            elif depth > 0:
                errors.append({"file": file_path, "feature": f"Feature-{i+1}", "error": f"Depth must be negative: {depth}"})

    except Exception as e:
        errors.append({"file": file_path, "feature": "N/A", "error": f"Failed to parse JSON: {str(e)}"})
    return file_path, errors

def run_trusted_node_validation(file_path: str, schema_version: str = None) -> Tuple[str, List[Dict[str, Any]]]:
    cmd = ["csbschema", "validate", "-f", file_path]
    if schema_version:
        cmd.extend(["--version", schema_version])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"\n{Fore.GREEN}✅ [PASS]{Style.RESET_ALL} {file_path} passed csbschema validation\n")
            return file_path, []
        else:
            print(f"\n{Fore.RED}❌ [FAIL]{Style.RESET_ALL} {file_path} failed csbschema validation\n")
            errors = []
            for line in result.stdout.strip().splitlines():
                if "Path:" in line and "error:" in line:
                    path_part, msg_part = line.split("error:", 1)
                    errors.append({
                        "file": file_path,
                        "feature": path_part.strip().replace("Path:", "").strip(),
                        "error": msg_part.strip()
                    })
            if errors:
                print(f"{Fore.YELLOW}Detailed Errors:{Style.RESET_ALL}")
                for err in errors:
                    print(f"  - {Fore.CYAN}Feature:{Style.RESET_ALL} {err['feature']}")
                    print(f"    {Fore.MAGENTA}Error:{Style.RESET_ALL} {err['error']}")
            else:
                print("No structured error output found.")
            return file_path, errors
    except Exception as e:
        print(f"{Fore.RED}❌ Exception while validating {file_path}: {e}{Style.RESET_ALL}")
        return file_path, [{"file": file_path, "feature": "N/A", "error": f"Exception: {str(e)}"}]

def write_report_pdf(results: List[Tuple[str, List[Dict[str, Any]]]], filename: str):
    def safe(text: str) -> str:
        return text.replace("✅", "[PASS]").replace("❌", "[FAIL]").encode("latin-1", "ignore").decode("latin-1")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Courier", size=10)

    for file_path, errors in results:
        base = os.path.basename(file_path)
        pdf.set_font("Courier", "B", 10)
        pdf.cell(200, 6, txt=safe(f"{base} Validation Report"), ln=True)
        pdf.set_font("Courier", size=10)
        if not errors:
            pdf.cell(200, 6, txt=safe("✅ All features passed validation."), ln=True)
        else:
            pdf.cell(200, 6, txt=safe(f"❌ {len(errors)} feature(s) with issues:"), ln=True)
            for err in errors:
                line = f"  Field Error: {err['error']} (Feature: {err['feature']})"
                pdf.cell(200, 6, txt=safe(line), ln=True)
        pdf.cell(200, 10, txt="", ln=True)

    pdf.output(filename)

def main():
    parser = argparse.ArgumentParser(description="Validate CSB files.")
    parser.add_argument("path", help="Path to a file or directory")
    parser.add_argument("--mode", choices=["crowbar", "trusted-node"], required=True,
                        help="Choose which validation mode to use.")
    parser.add_argument("--schema-version", help="Schema version for trusted-node mode", required=False)
    args = parser.parse_args()

    files = (
        [os.path.join(args.path, f)
         for f in os.listdir(args.path)
         if f.endswith(".geojson") or f.endswith(".json") or f.endswith(".xyz")]
        if os.path.isdir(args.path)
        else [args.path]
    )

    if args.mode == "trusted-node":
        for file in files:
            run_trusted_node_validation(file, schema_version=args.schema_version)
        return

    all_results = []
    for file in files:
        _, errors = run_custom_validation(file)
        all_results.append((file, errors))

    write_report_pdf(all_results, "crowbar_validation_report.pdf")
    print(f"{Fore.BLUE}📄 Crowbar validation results saved to 'crowbar_validation_report.pdf'{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
