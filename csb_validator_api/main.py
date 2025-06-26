from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from csb_validator.validator_crowbar import run_custom_validation
from csb_validator.validator_trusted import run_trusted_node_validation
import tempfile
import shutil
import os
import asyncio

app = FastAPI(title="CSB Validator API", version="1.0.0")

# Serve the static HTML frontend
app.mount("/static", StaticFiles(directory="csb_validator_api/static"), name="static")

@app.get("/", response_class=HTMLResponse)
def index():
    with open("csb_validator_api/static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.post("/validate")
async def validate_file(
    file: UploadFile = File(...),
    mode: str = Form("crowbar"),
    schema_version: str = Form(None)
):
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    try:
        if mode == "trusted-node":
            result = await run_trusted_node_validation(tmp_path, schema_version)
        else:
            result = await asyncio.to_thread(run_custom_validation, tmp_path)
        os.unlink(tmp_path)
        return JSONResponse(content={"file": result[0], "errors": result[1]}, status_code=200)
    except Exception as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return JSONResponse(content={"error": str(e)}, status_code=500)
