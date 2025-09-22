# Data Explorer App

Interactive Streamlit app to quickly explore local data files (Parquet, CSV, JSON, JSONL), filter them with a simple UI, and export filtered results.

## Features

- Upload local files: Parquet, CSV, JSON, JSONL
- Automatic detection and optional parsing of datetime columns
- Sidebar filters:
  - Required time window filter (≤ 30 days) on a chosen datetime column
  - Numeric range sliders for numeric columns
  - Value pickers for low‑cardinality categorical columns
  - Case‑insensitive substring search for high‑cardinality text columns
- Column selection for export
- Export options:
  - CSV (single file if ≤ 500k rows)
  - CSV split into 500k‑row parts packaged as a ZIP for larger datasets
  - Parquet (requires `pyarrow`; gracefully disabled if not available)

## Requirements

- Python 3.9+
- pip

Python packages (from `requirements.txt`):

- `streamlit`
- `pandas`

Recommended (for Parquet read/write):

- `pyarrow`

Install it alongside the base requirements if you plan to work with Parquet:

```bash
pip install pyarrow
```

## Quick start

```bash
# From the project root
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt

# Optional, recommended for Parquet support
pip install pyarrow

# Run the app
streamlit run dataexplorerapp.py
```

Then open the URL Streamlit prints (usually http://localhost:8501).

## Usage

1. Click "Upload file" and choose a `.parquet`, `.csv`, `.json`, or `.jsonl` file.
2. In the sidebar:
   - Select the columns to include in the export.
   - Choose a datetime column for the required time filter.
   - Pick a date range up to 30 days. Export is enabled only after a valid time window is set.
   - Optionally refine with numeric ranges, categorical selections, or substring matches.
3. Click "Apply filters".
4. Use the export buttons to download CSV (or ZIP of CSV parts) and, if available, Parquet.

### Notes on datetime handling

- The app tries to auto‑parse object columns to datetimes when ≥80% of values parse successfully.
- A datetime column is required to enable export. If none exists, the sidebar will indicate that a time column is required.
- If a column named `time` exists and is datetime‑typed, it will be selected by default.

### Export behavior

- CSV export is always available.
- When filtered rows exceed 500,000, the app provides a ZIP containing multiple CSV parts.
- Parquet export requires `pyarrow`. If `pyarrow` is not installed, a note will be shown and Parquet export will be unavailable.

## Troubleshooting

- Parquet read/write errors: `pip install pyarrow`
- Large files causing memory pressure: apply filters to reduce rows/columns before exporting.
- No export button enabled: ensure you selected a datetime column and a ≤30‑day date range.

## Development

- Main app entry point: `dataexplorerapp.py`
- Dependencies: `requirements.txt`
- Start the app with `streamlit run dataexplorerapp.py` while your virtual environment is active.

Contributions and improvements welcome.
