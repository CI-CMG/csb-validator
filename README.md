# csb-validator
csb-validator is a command-line utility designed to validate GeoJSON files containing crowdsourced bathymetry (CSB) data. It enforces strict data integrity rules to ensure compliance with Crowbar and CSB 3.1 specifications



## What are we validating?

| Field       | Requirement                                                               |
|-------------|---------------------------------------------------------------------------|
| `longitude` | Must be present and between **-180** and **180**                          |
| `latitude`  | Must be present and between **-90** and **90**                            |
| `depth`     | Must be present (not null or missing)                                     |
| `heading`   | Optional, but if present, must be between **0** and **360**               |
| `time`      | Must be present, ISO 8601 formatted, and **in the past**       


## Installation

```bash
git clone https://github.com/your-org/csb-validator.git
cd csb-validator
pip install -r requirements.txt  # optional if aiofiles is not already installed

## Optional Global Access
chmod +x csb_validator.py
ln -s $(pwd)/validator.py /usr/local/bin/csb-validator


# Example single file execution 
python validator.py path/to/file.geojson

# Example mult-file execution
python validator.py *.geojson
python validator.py *.geojson *.xyz.json

# Sample output
📋 Validation Report:
========================================

✅ file1.geojson: All features passed validation.

❌ file2.geojson: 2 feature(s) with issues.
  Feature #3:
    - Timestamp should be in the past
  Feature #7:
    - Latitude should be ≤ 90

❌ broken_file.json: Failed to process
  - Error: Expecting value: line 1 column 1 (char 0)
