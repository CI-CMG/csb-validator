import argparse
import json
import os
import subprocess
from collections import defaultdict
from typing import List, Dict, Any, Tuple
from fpdf import FPDF
from colorama import Fore, Style, init

init(autoreset=True)


def extract_feature_line_numbers(file_path: str) -> Dict[int, int]:
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.splitlines()
    feature_line_map = {}

    in_features = False
    feature_index = 0
    bracket_stack = []
    feature_start_pos = None

    for i, line in enumerate(lines):
        if not in_features and '"features"' in line and "[" in line:
            in_features = True
            continue
        if in_features:
            for j, char in enumerate(line):
                if char == "{":
                    if not bracket_stack:
                        feature_start_pos = i + 1
                    bracket_stack.append("{")
                elif char == "}":
                    if bracket_stack:
                        bracket_stack.pop()
                        if not bracket_stack and feature_start_pos:
                            feature_line_map[feature_index] = feature_start_pos
                            feature_index += 1
                            feature_start_pos = None
                elif char == "]":
                    if not bracket_stack:
                        return feature_line_map

    return feature_line_map


def run_custom_validation(file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
    line_map = extract_feature_line_numbers(file_path)
    errors = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for i, feature in enumerate(data.get("features", [])):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])
            line_number = line_map.get(i, "N/A")

            if not coords or not isinstance(coords, list) or len(coords) < 2:
                errors.append(
                    {
                        "file": file_path,
                        "feature": str(line_number),
                        "error": "Invalid geometry coordinates",
                    }
                )
            else:
                lon, lat = coords[0], coords[1]
                if lon is None or lon < -180 or lon > 180:
                    errors.append(
                        {
                            "file": file_path,
                            "feature": str(line_number),
                            "error": f"Longitude out of bounds: {lon}",
                        }
                    )
                if lat is None or lat < -90 or lat > 90:
                    errors.append(
                        {
                            "file": file_path,
                            "feature": str(line_number),
                            "error": f"Latitude out of bounds: {lat}",
                        }
                    )

            depth = props.get("depth")
            if depth is None:
                errors.append(
                    {
                        "file": file_path,
                        "feature": str(line_number),
                        "error": "Depth is required",
                    }
                )

    except Exception as e:
        errors.append(
            {
                "file": file_path,
                "feature": "N/A",
                "error": f"Failed to parse JSON: {str(e)}",
            }
        )
    return file_path, errors


def run_trusted_node_validation(
    file_path: str, schema_version: str = None
) -> Tuple[str, List[Dict[str, Any]]]:
    cmd = ["csbschema", "validate", "-f", file_path]
    if schema_version:
        cmd.extend(["--version", schema_version])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(
                f"\n{Fore.GREEN}✅ [PASS]{Style.RESET_ALL} {file_path} passed csbschema validation\n"
            )
            return file_path, []
        else:
            print(
                f"\n{Fore.RED}❌ [FAIL]{Style.RESET_ALL} {file_path} failed csbschema validation\n"
            )
            errors = []
            for line in result.stdout.strip().splitlines():
                if "Path:" in line and "error:" in line:
                    path_part, msg_part = line.split("error:", 1)
                    errors.append(
                        {
                            "file": file_path,
                            "feature": path_part.strip().replace("Path:", "").strip(),
                            "error": msg_part.strip(),
                        }
                    )
            if errors:
                print(f"{Fore.YELLOW}Detailed Errors:{Style.RESET_ALL}")
                for err in errors:
                    print(f"  - {Fore.CYAN}Path:{Style.RESET_ALL} {err['feature']}")
                    print(f"    {Fore.MAGENTA}Error:{Style.RESET_ALL} {err['error']}")
            else:
                print("No structured error output found.")
            return file_path, errors
    except Exception as e:
        print(
            f"{Fore.RED}❌ Exception while validating {file_path}: {e}{Style.RESET_ALL}"
        )
        return file_path, [
            {"file": file_path, "feature": "N/A", "error": f"Exception: {str(e)}"}
        ]


def write_report_pdf(results: List[Tuple[str, List[Dict[str, Any]]]], filename: str):
    def safe(text: str) -> str:
        return (
            text.replace("✅", "[PASS]")
            .replace("❌", "[FAIL]")
            .encode("latin-1", "ignore")
            .decode("latin-1")
        )

    files_with_errors = set()
    detailed_errors = []

    for file_path, errors in results:
        if errors:
            files_with_errors.add(file_path)
            for err in errors:
                detailed_errors.append((file_path, err["feature"], err["error"]))

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Courier", "B", 14)
    pdf.cell(200, 10, txt="CSB Validation Summary", ln=True)

    pdf.set_font("Courier", size=10)
    pdf.ln(5)
    pdf.cell(200, 8, txt=f"Total files processed: {len(results)}", ln=True)
    pdf.cell(200, 8, txt=f"Files with errors: {len(files_with_errors)}", ln=True)
    pdf.cell(200, 8, txt=f"Total validation errors: {len(detailed_errors)}", ln=True)
    pdf.ln(8)

    pdf.set_font("Courier", "B", 12)
    pdf.cell(200, 8, txt="Validation Errors Table:", ln=True)
    pdf.ln(2)

    col_file = 60
    col_line = 30
    col_error = 100

    pdf.set_font("Courier", "B", 10)
    pdf.cell(col_file, 7, "File Name", border=1)
    pdf.cell(col_line, 7, "Line", border=1)
    pdf.cell(col_error, 7, "Error Message", border=1, ln=True)

    pdf.set_font("Courier", size=10)
    for file, line, error in detailed_errors:
        base = os.path.basename(file)
        pdf.cell(col_file, 6, safe(base[:50]), border=1)
        pdf.cell(col_line, 6, safe(str(line)), border=1)
        pdf.cell(col_error, 6, safe(error[:85]), border=1, ln=True)

    grouped = defaultdict(list)
    for file, line, error in detailed_errors:
        grouped[file].append((line, error))

    for file_path, file_errors in grouped.items():
        pdf.add_page()
        base = os.path.basename(file_path)
        pdf.set_font("Courier", "B", 12)
        pdf.cell(200, 10, txt=safe(f"Detailed Errors for File: {base}"), ln=True)
        pdf.set_font("Courier", size=10)
        for line, error in file_errors:
            pdf.multi_cell(0, 6, txt=safe(f"Line: {line}\nError: {error}\n"), border=0)

    pdf.output(filename)


def main():
    parser = argparse.ArgumentParser(description="Validate CSB files.")
    parser.add_argument("path", help="Path to a file or directory")
    parser.add_argument(
        "--mode",
        choices=["crowbar", "trusted-node"],
        required=True,
        help="Choose which validation mode to use.",
    )
    parser.add_argument(
        "--schema-version", help="Schema version for trusted-node mode", required=False
    )
    args = parser.parse_args()

    files = (
        [
            os.path.join(args.path, f)
            for f in os.listdir(args.path)
            if f.endswith(".geojson") or f.endswith(".json") or f.endswith(".xyz")
        ]
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
    print(
        f"{Fore.BLUE}📄 Crowbar validation results saved to 'crowbar_validation_report.pdf'{Style.RESET_ALL}"
    )


if __name__ == "__main__":
    main()
