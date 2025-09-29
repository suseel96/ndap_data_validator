## CSV Validation and S3 Uploader (Streamlit)

### Quick start

```bash
# Create and activate a virtual env (Windows PowerShell)
python -m venv .venv
. .venv/Scripts/Activate.ps1

# Install deps
pip install -r requirements.txt

# Run app
streamlit run app.py
```

Open the URL shown (typically `http://localhost:8501`).

### Features
- Upload CSV
- Preview and select data types per column (string, integer, float, boolean, date, datetime, category)
- Validation checks: null counts and conversion errors
- Conditional S3 upload (original or cleaned CSV)

### S3 credentials
- Use environment (default AWS creds resolution via `boto3`)
- Or enter manually in the UI

### Notes
- Integer/float parsing uses pandas with coercion; invalid values become nulls.
- Boolean parsing supports: true/t/yes/y/1 and false/f/no/n/0 (case-insensitive).
- Date/datetime parsing uses `pandas.to_datetime` with coercion.


