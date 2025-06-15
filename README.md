# csb-validator

**csb-validator** is a fast, python-based command-line tool for validating geospatial files, including GeoJSON, XYZ-style JSON, and related formats used in Crowbar. It enforces key data quality rules and is designed to run on many files at once, validating all features asynchronously for efficiency.

---

## Features

* Validates longitude, latitude, depth, heading, and time fields
* PDF report generation for **Crowbar-style** validations
* Batch validation for multiple files in a folder
* Optional schema version validation via csbschema

---

## Validation Rules

### Crowbar Mode

| Field       | Requirement                                                |
| ----------- | ---------------------------------------------------------- |
| `longitude` | Must be present and between **-180** and **180**           |
| `latitude`  | Must be present and between **-90** and **90**             |
| `depth`     | Must be present (not null or missing)   |
| `heading`   | Optional, but if present must be between **0** and **360** |
| `time`      | Must be present, ISO 8601 formatted, and **in the past**   |

> Coordinates should be under `geometry.coordinates`, and other fields under `properties`.

### Trusted Node Mode

Uses the [`csbschema`](https://github.com/CCOMJHC/csbschema) CLI tool to validate against Trusted Node schemas. Requires the `csbschema` CLI to be installed in your environment.

---

## Installation

### From PyPI

```bash
pip install csb-validator
```

### From Source

```bash
git clone https://github.com/your-org/csb-validator.git
cd csb-validator
pip install -r requirements.txt
```

To ensure the CLI works globally:

```bash
chmod +x csb_validator/validator.py
ln -s $(pwd)/csb_validator/validator.py /usr/local/bin/csb-validator
```

---

## Usage

### Crowbar Validation (with PDF Output)

#### Single File

```bash
csb-validator path/to/test.xyz --mode crowbar
```

#### Folder of Files

```bash
csb-validator path/to/folder --mode crowbar
```

➡️ Outputs a PDF report: `crowbar_validation_report.pdf`

### Trusted Node Validation (csbschema CLI Required)

#### Single File

```bash
csb-validator path/to/test.geojson --mode trusted-node
```

#### Specify Schema Version

```bash
csb-validator path/to/test.geojson --mode trusted-node --schema-version v1.2.0
```

➡️ Output: Validation results are printed directly to the **terminal** with color formatting.

---

## Example Output (Crowbar PDF)

```
📋 Validation Report:

✅ file1.geojson: All features passed validation.

❌ file2.geojson: 2 feature(s) with issues:
  Field Error: Timestamp should be in the past (Feature: Feature-3)
  Field Error: Latitude should be ≤ 90 (Feature: Feature-7)

❌ broken_file.json: Failed to process
  Field Error: Expecting value: line 1 column 1 (char 0) (Feature: N/A)
```

---

## Requirements

* Python 3.7+
* `aiofiles` for async file reads
* `fpdf` for PDF report generation
* `csbschema` CLI (only needed for `--mode trusted-node`)

---

## License

MIT License

## Author

Clinton Campbell
NOAA / CIRES
