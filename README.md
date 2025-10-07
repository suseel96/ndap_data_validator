## CSV Validation and S3 Uploader (FastAPI)

### Quick start

```bash
# Install uv (one-time)
# Windows PowerShell:
iwr https://astral.sh/uv/install.ps1 -UseBasicParsing | iex
# macOS/Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies from pyproject.toml and create .venv
uv sync

# Run the app
uv run uvicorn main:app --reload --port 8000

# Handy commands
# Add a new dependency (updates pyproject + lock)
uv add <package>
# Update lockfile to latest allowed versions
uv lock --upgrade
```

Open `http://localhost:8000`.

### Features
- Upload CSV
- Assign roles per column: Location, Time, Measures, Others
- Measures can be integer/float; Time can be date-only
- Validation: Location & Time require no nulls; Measures must be numeric and no nulls
- Conditional S3 upload (original or cleaned CSV) when all checks pass

### S3 credentials
- Use environment (default AWS creds resolution via `boto3`)
- Or enter manually in the form

### Notes
- Numeric parsing coerces invalids to nulls; those trigger validation failures for Measures.
- Date/datetime parsing uses `pandas.to_datetime`.

### uv notes
- The source of truth is `pyproject.toml`; `uv.lock` ensures reproducible installs.
- `uv sync` reads both files and installs into a local `.venv`.
- You can still use `requirements.txt` temporarily via `uv pip install -r requirements.txt`, but it is deprecated in favor of `pyproject.toml`.
