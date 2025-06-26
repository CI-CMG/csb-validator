import asyncio
from typing import List, Dict, Tuple, Any
from colorama import Fore, Style

Any = Any 

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
