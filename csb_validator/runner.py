import os
import sys
import asyncio
from colorama import Fore, Style
from csb_validator.validator_crowbar import run_custom_validation
from csb_validator.validator_trusted import run_trusted_node_validation
from csb_validator.pdf_writer import write_report_pdf


def _collect_input_files(path: str):
    """Collect .geojson/.json/.xyz files from a path. Recurses into subfolders if path is a directory."""
    exts = (".geojson", ".json", ".xyz")

    if os.path.isdir(path):
        collected = []
        for root, _dirs, filenames in os.walk(path):
            for name in filenames:
                if name.endswith(exts):
                    collected.append(os.path.join(root, name))
        return collected

    return [path]


async def main_async(path: str, mode: str, schema_version: str = None):
    files = _collect_input_files(path)

    if mode == "trusted-node":
        tasks = [run_trusted_node_validation(file, schema_version) for file in files]
        output_pdf = "trusted_node_validation_report.pdf"
    else:
        tasks = [asyncio.to_thread(run_custom_validation, file) for file in files]
        output_pdf = "crowbar_validation_report.pdf"

    all_results = await asyncio.gather(*tasks)

    await asyncio.to_thread(write_report_pdf, all_results, output_pdf, mode)
    print(f"{Fore.BLUE}📄 Validation results saved to '{output_pdf}'{Style.RESET_ALL}")

    errored_files = []
    for result in all_results:
        if isinstance(result, (tuple, list)) and len(result) >= 2:
            file_path, errors = result[0], result[1]
            if errors:
                errored_files.append(file_path)

    if not errored_files:
        print(f"\n{Fore.GREEN}✅ No files had validation errors; nothing to delete.{Style.RESET_ALL}")
        return

    print(f"\n{Fore.RED}⚠️  Files with ERRORS:{Style.RESET_ALL}")
    for fp in errored_files:
        print(f"{Fore.RED} - {fp}{Style.RESET_ALL}")

    if not sys.stdin.isatty():
        print(f"\n{Fore.YELLOW}Non-interactive session detected; skipping delete prompt.{Style.RESET_ALL}")
        return

    choice = input("\nDelete ONLY the files listed above that have ERRORS? [y/N]: ").strip().lower()
    if choice in ("y", "yes"):
        deleted = 0
        failed = 0

        for fp in errored_files:
            try:
                os.remove(fp)
                deleted += 1
            except Exception as e:
                failed += 1
                print(f"{Fore.YELLOW}Failed to delete {fp}: {e}{Style.RESET_ALL}")

        print(f"\n{Fore.BLUE}🧹 Deleted {deleted} errored file(s).{Style.RESET_ALL}")
        if failed:
            print(f"{Fore.YELLOW}⚠️  Failed to delete {failed} file(s).{Style.RESET_ALL}")
    else:
        print(f"\n{Fore.BLUE}No files were deleted.{Style.RESET_ALL}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m csb_validator.runner <path> <mode> [schema_version]")
        raise SystemExit(2)

    path = sys.argv[1]
    mode = sys.argv[2]
    schema_version = sys.argv[3] if len(sys.argv) > 3 else None

    asyncio.run(main_async(path, mode, schema_version))