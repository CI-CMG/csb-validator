# csb-validator

**csb-validator** is a fast, Python-based command-line tool for validating geospatial files, including GeoJSON, XYZ-style JSON, and related formats used in Crowbar. It enforces key data quality rules and is designed to run on many files at once, validating all features asynchronously for efficiency.

---

## What are we validating?

For each Feature in a supported file, the following fields are checked:

| Field       | Requirement                                                 |
| ----------- | ----------------------------------------------------------- |
| `longitude` | Must be present and between **-180** and **180**            |
| `latitude`  | Must be present and between **-90** and **90**              |
| `depth`     | Must be present (not null or missing)                       |
| `heading`   | Optional, but if present, must be between **0** and **360** |
| `time`      | Must be present, ISO 8601 formatted, and **in the past**    |

The validator currently expects `longitude` and `latitude` in the `geometry.coordinates` array and other values in the `properties` object, as typical in GeoJSON and CSB-style JSON.

---

## Installation

### From PyPI

You can install `csb-validator` directly from PyPI:

```bash
pip install csb-validator
```

After installation, you'll be able to use the `csb-validator` command globally:

```bash
csb-validator path/to/file.geojson
```

---

### From Source

```bash
git clone https://github.com/your-org/csb-validator.git
cd csb-validator
pip install -r requirements.txt

# Optional Global Access
chmod +x csb_validator.py
ln -s $(pwd)/validator.py /usr/local/bin/csb-validator
```

---

## Usage

### Single File Execution

```bash
python validator.py path/to/file.geojson
```

### Multi-file Execution

```bash
python validator.py *.geojson
python validator.py *.geojson *.xyz.json
```

---

## Sample Output

```
📋 Validation Report:

✅ file1.geojson: All features passed validation.

❌ file2.geojson: 2 feature(s) with issues.
  Feature #3:
    - Timestamp should be in the past
  Feature #7:
    - Latitude should be ≤ 90

❌ broken_file.json: Failed to process
  - Error: Expecting value: line 1 column 1 (char 0)
```
