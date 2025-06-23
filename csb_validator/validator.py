import argparse
import json
import os
import asyncio
from collections import defaultdict
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone

import aiofiles
from fpdf import FPDF
from colorama import Fore, Style, init

init(autoreset=True)


async def extract_feature_line_numbers(file_path: str) -> Dict[int, int]:
    async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
        content = await f.read()

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
            for char in line:
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


async def run_custom_validation(file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
    line_map = await extract_feature_line_numbers(file_path)
    errors = []
    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            content = await f.read()
            data = json.loads(content)

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
                continue

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
                        "error": "Depth cannot be blank",
                    }
                )

            heading = props.get("heading")
            if heading is not None:
                try:
                    heading_val = float(heading)
                    if heading_val < 0 or heading_val > 360:
                        errors.append(
                            {
                                "file": file_path,
                                "feature": str(line_number),
                                "error": f"Heading out of bounds: {heading}",
                            }
                        )
                except ValueError:
                    errors.append(
                        {
                            "file": file_path,
                            "feature": str(line_number),
                            "error": f"Heading is not a valid number: {heading}",
                        }
                    )

            time_str = props.get("time")
            if not time_str:
                errors.append(
                    {
                        "file": file_path,
                        "feature": str(line_number),
                        "error": "Timestamp cannot be blank",
                    }
                )
            else:
                try:
                    timestamp = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    if timestamp > now:
                        time = time_str[:10]
                        errors.append(
                            {
                                "file": file_path,
                                "feature": str(line_number),
                                "error": f"Timestamp should be in the past: {time}",
                            }
                        )
                except Exception:
                    errors.append(
                        {
                            "file": file_path,
                            "feature": str(line_number),
                            "error": f"Invalid ISO 8601 timestamp: {time_str}",
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


async def run_trusted_node_validation(
    file_path: str, schema_version: str = None
) -> Tuple[str, List[Dict[str, Any]]]:
    cmd = ["csbschema", "validate", "-f", file_path]
    if schema_version:
        cmd.extend(["--version", schema_version])

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            print(
                f"\n{Fore.GREEN}✅ [PASS]{Style.RESET_ALL} {file_path} passed csbschema validation\n"
            )
            return file_path, []
        else:
            print(
                f"\n{Fore.RED}❌ [FAIL]{Style.RESET_ALL} {file_path} failed csbschema validation\n"
            )
            errors = []
            for line in stdout.decode().strip().splitlines():
                if "Path:" in line and "error:" in line:
                    path_part, msg_part = line.split("error:", 1)
                    errors.append(
                        {
                            "file": file_path,
                            "feature": "N/A",
                            "error": msg_part.strip(),
                        }
                    )
            return file_path, errors or [
                {"file": file_path, "feature": "N/A", "error": "Unstructured error"}
            ]
    except Exception as e:
        return file_path, [
            {
                "file": file_path,
                "feature": "N/A",
                "error": f"Subprocess error: {str(e)}",
            }
        ]


def write_report_pdf(results: List[Tuple[str, List[Dict[str, Any]]]], filename: str, mode: str):
    def safe(text: str) -> str:
        return text.encode("latin-1", "ignore").decode("latin-1")

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

    col_file, col_line, col_error = 60, 30, 125
    pdf.set_font("Courier", "B", 10)
    pdf.cell(col_file, 7, "File Name", border=1)
    if mode != "trusted-node":
        pdf.cell(col_line, 7, "Line", border=1)
    pdf.cell(col_error, 7, "Error Message", border=1, ln=True)

    pdf.set_font("Courier", size=10)
    for file, line, error in detailed_errors:
        base = os.path.basename(file)
        pdf.cell(col_file, 6, safe(base[:50]), border=1)
        if mode != "trusted-node":
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
            if mode != "trusted-node":
                pdf.multi_cell(0, 6, txt=safe(f"Line: {line}\nError: {error}\n"), border=0)
            else:
                pdf.multi_cell(0, 6, txt=safe(f"Error: {error}\n"), border=0)

    pdf.output(filename)


async def main_async(path: str, mode: str, schema_version: str = None):
    files = (
        [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.endswith(".geojson") or f.endswith(".json") or f.endswith(".xyz")
        ]
        if os.path.isdir(path)
        else [path]
    )

    if mode == "trusted-node":
        tasks = [run_trusted_node_validation(file, schema_version) for file in files]
        output_pdf = "trusted_node_validation_report.pdf"
    else:
        tasks = [run_custom_validation(file) for file in files]
        output_pdf = "crowbar_validation_report.pdf"

    all_results = await asyncio.gather(*tasks)
    await asyncio.to_thread(write_report_pdf, all_results, output_pdf, mode)

    print(f"{Fore.BLUE}📄 Validation results saved to '{output_pdf}'{Style.RESET_ALL}")


def main():
    parser = argparse.ArgumentParser(description="Validate CSB files.")
    parser.add_argument("path", help="Path to a file or directory")
    parser.add_argument(
        "--mode",
        choices=["crowbar", "trusted-node"],
        required=True,
        help="Validation mode",
    )
    parser.add_argument(
        "--schema-version", help="Schema version for trusted-node mode", required=False
    )
    args = parser.parse_args()
    asyncio.run(main_async(args.path, args.mode, args.schema_version))


if __name__ == "__main__":
    main()
