import argparse
import asyncio
from csb_validator.runner import main_async

def main():
    parser = argparse.ArgumentParser(description="Validate CSB files.")
    parser.add_argument("path", help="Path to a file or directory")
    parser.add_argument("--mode", choices=["crowbar", "trusted-node"], required=True, help="Validation mode")
    parser.add_argument("--schema-version", help="Schema version for trusted-node mode")
    args = parser.parse_args()
    asyncio.run(main_async(args.path, args.mode, args.schema_version))