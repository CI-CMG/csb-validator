from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import tempfile
import shutil
import zipfile
import os
import uuid
from csb_validator.runner import main_async

app = FastAPI()

app.mount("/static", StaticFiles(directory="csb_validator_api/static"), name="static")
templates = Jinja2Templates(directory="csb_validator_api/templates")


@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/validate")
async def validate(
    request: Request,
    file: UploadFile = File(...),
    mode: str = Form(...),
    schema_version: str = Form(""),
    page: int = Form(1),
    page_size: int = Form(100)
):
    session_id = str(uuid.uuid4())
    session_dir = os.path.join(tempfile.gettempdir(), session_id)
    os.makedirs(session_dir, exist_ok=True)

    file_path = os.path.join(session_dir, file.filename)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # If ZIP, extract and collect all valid files recursively
    if file.filename.lower().endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(session_dir)

        files_to_validate = []
        for root, _, filenames in os.walk(session_dir):
            for f in filenames:
                if f.endswith((".geojson", ".xyz", ".json")):
                    files_to_validate.append(os.path.join(root, f))
    else:
        files_to_validate = [file_path]

    print(f"Found {len(files_to_validate)} files to validate...")

    result = await main_async(
        files=files_to_validate,
        mode=mode,
        schema_version=schema_version,
        page=page,
        page_size=page_size
    )

    return JSONResponse(content={
        "errors": result["errors"],
        "totalErrors": result["total_errors"],
        "totalPages": result["total_pages"],
        "currentPage": result["current_page"],
        "pageSize": result["page_size"],
        "session": session_id
    })
