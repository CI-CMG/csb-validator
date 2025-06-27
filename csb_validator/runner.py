import os
import asyncio
from typing import List, Dict, Any
from csb_validator.validator_crowbar import run_custom_validation
from csb_validator.validator_trusted import run_trusted_node_validation
from colorama import Fore, Style

async def main_async(
    files: List[str],
    mode: str,
    schema_version: str = "",
    page: int = 1,
    page_size: int = 50
) -> Dict[str, Any]:
    all_errors: List[Dict[str, Any]] = []

    if mode == "trusted-node":
        print(f"{Fore.CYAN}Running trusted-node validation on {len(files)} file(s)...{Style.RESET_ALL}")
        tasks = [run_trusted_node_validation(file, schema_version) for file in files]
        results = await asyncio.gather(*tasks)
    elif mode == "crowbar":
        print(f"{Fore.CYAN}Running crowbar validation on {len(files)} file(s)...{Style.RESET_ALL}")
        results = []
        for file in files:
            result = await asyncio.to_thread(run_custom_validation, file)
            results.append(result)
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    for file_path, errors in results:
        all_errors.extend(errors)
        
    total_errors = len(all_errors)
    total_pages = (total_errors + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = start + page_size
    paged_errors = all_errors[start:end]

    return {
        "errors": paged_errors,
        "total_errors": total_errors,
        "total_pages": total_pages,
        "current_page": page,
        "page_size": page_size
    }
