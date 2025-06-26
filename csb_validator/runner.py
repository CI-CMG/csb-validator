import os
import asyncio
from colorama import Fore, Style
from csb_validator.validator_crowbar import run_custom_validation
from csb_validator.validator_trusted import run_trusted_node_validation
from csb_validator.pdf_writer import write_report_pdf

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