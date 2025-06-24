import argparse
import json
import os
import asyncio
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone
from fpdf import FPDF
from colorama import Fore, Style, init

init(autoreset=True)

def map_feature_property_lines_sync(file_path: str) -> Dict[Tuple[int, str], int]:
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    feature_prop_line_map = {}
    in_features = False
    feature_index = -1
    open_braces = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not in_features and '"features"' in stripped and "[" in stripped:
            in_features = True
            continue
        if in_features:
            if "{" in line and open_braces == 0:
                feature_index += 1
            open_braces += line.count("{")
            open_braces -= line.count("}")
            for prop in ["time", "depth", "heading"]:
                if f'"{prop}"' in line:
                    feature_prop_line_map[(feature_index, prop)] = i + 1
            if "]" in line and open_braces == 0:
                break
    return feature_prop_line_map

def map_feature_coordinates_line_sync(file_path: str) -> Dict[int, int]:
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    coord_line_map = {}
    in_features = False
    feature_index = -1
    open_braces = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not in_features and '"features"' in stripped and "[" in stripped:
            in_features = True
            continue
        if in_features:
            if "{" in line and open_braces == 0:
                feature_index += 1
            open_braces += line.count("{")
            open_braces -= line.count("}")
            if '"coordinates"' in stripped:
                coord_line_map[feature_index] = i + 1
            if "]" in line and open_braces == 0:
                break
    return coord_line_map

def run_custom_validation(file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
    errors = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        line_map = map_feature_property_lines_sync(file_path)
        coord_line_map = map_feature_coordinates_line_sync(file_path)
        for i, feature in enumerate(data.get("features", [])):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])
            line_num = coord_line_map.get(i, i + 1)
            if not coords or not isinstance(coords, list) or len(coords) < 2:
                errors.append({"file": file_path, "line": line_num, "error": "Invalid geometry coordinates"})
                continue
            lon, lat = coords[0], coords[1]
            if lon is None or lon < -180 or lon > 180:
                errors.append({"file": file_path, "line": line_num, "error": f"Longitude out of bounds: {lon}"})
            if lat is None or lat < -90 or lat > 90:
                errors.append({"file": file_path, "line": line_num, "error": f"Latitude out of bounds: {lat}"})
            depth = props.get("depth")
            if depth is None:
                depth_line = line_map.get((i, "depth"), line_num)
                errors.append({"file": file_path, "line": depth_line, "error": "Depth cannot be blank"})
            heading = props.get("heading")
            if heading is not None:
                try:
                    heading_val = float(heading)
                    if heading_val < 0 or heading_val > 360:
                        heading_line = line_map.get((i, "heading"), line_num)
                        errors.append({"file": file_path, "line": heading_line, "error": f"Heading out of bounds: {heading}"})
                except ValueError:
                    heading_line = line_map.get((i, "heading"), line_num)
                    errors.append({"file": file_path, "line": heading_line, "error": f"Heading is not a valid number: {heading}"})
            time_str = props.get("time")
            if not time_str:
                time_line = line_map.get((i, "time"), line_num)
                errors.append({"file": file_path, "line": time_line, "error": "Timestamp cannot be blank"})
            else:
                try:
                    timestamp = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    if timestamp > now:
                        time_line = line_map.get((i, "time"), line_num)
                        time = time_str[:10]
                        errors.append({"file": file_path, "line": time_line, "error": f"Timestamp should be in the past: {time}"})
                except Exception:
                    time_line = line_map.get((i, "time"), line_num)
                    errors.append({"file": file_path, "line": time_line, "error": f"Invalid ISO 8601 timestamp: {time_str}"})
        processing = data.get("properties", {}).get("processing", [])
        if isinstance(processing, list):
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            for p in processing:
                global_time = p.get("timestamp")
                if global_time:
                    time_line = next((i + 1 for i, line in enumerate(lines) if global_time in line), "N/A")
                    try:
                        timestamp = datetime.fromisoformat(global_time.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        if timestamp > now:
                            errors.append({"file": file_path, "line": time_line, "error": f"Timestamp should be in the past: {global_time[:10]}"})
                    except Exception:
                        errors.append({"file": file_path, "line": time_line, "error": f"Invalid ISO 8601 timestamp: {global_time}"})
    except Exception as e:
        errors.append({"file": file_path, "line": "N/A", "error": f"Failed to parse JSON: {str(e)}"})
    return file_path, errors

async def run_trusted_node_validation(file_path: str, schema_version: str = None) -> Tuple[str, List[Dict[str, Any]]]:
    cmd = ["csbschema", "validate", "-f", file_path]
    if schema_version:
        cmd.extend(["--version", schema_version])
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode == 0:
            print(f"\n{Fore.GREEN}✅ [PASS]{Style.RESET_ALL} {file_path} passed csbschema validation\n")
            return file_path, []
        else:
            print(f"\n{Fore.RED}❌ [FAIL]{Style.RESET_ALL} {file_path} failed csbschema validation\n")
            errors = []
            for line in stdout.decode().strip().splitlines():
                if "Path:" in line and "error:" in line:
                    path_part, msg_part = line.split("error:", 1)
                    errors.append({"file": file_path, "error": msg_part.strip()})
            return file_path, errors or [{"file": file_path, "error": "Unstructured error"}]
    except Exception as e:
        return file_path, [{"file": file_path, "error": f"Subprocess error: {str(e)}"}]

def write_report_pdf(results: List[Tuple[str, List[Dict[str, Any]]]], filename: str, mode: str):
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

async def main_async(path: str, mode: str, schema_version: str = None):
    files = (
        [os.path.join(path, f) for f in os.listdir(path)
         if f.endswith(".geojson") or f.endswith(".json") or f.endswith(".xyz")]
        if os.path.isdir(path) else [path]
    )
    if mode == "trusted-node":
        tasks = [run_trusted_node_validation(file, schema_version) for file in files]
        output_pdf = "trusted_node_validation_report.pdf"
    else:
        tasks = [asyncio.to_thread(run_custom_validation, file) for file in files]
        output_pdf = "crowbar_validation_report.pdf"
    all_results = await asyncio.gather(*tasks)
    await asyncio.to_thread(write_report_pdf, all_results, output_pdf, mode)
    print(f"{Fore.BLUE}📄 Validation results saved to '{output_pdf}'{Style.RESET_ALL}")

def main():
    parser = argparse.ArgumentParser(description="Validate CSB files.")
    parser.add_argument("path", help="Path to a file or directory")
    parser.add_argument("--mode", choices=["crowbar", "trusted-node"], required=True, help="Validation mode")
    parser.add_argument("--schema-version", help="Schema version for trusted-node mode", required=False)
    args = parser.parse_args()
    asyncio.run(main_async(args.path, args.mode, args.schema_version))

if __name__ == "__main__":
    main()
