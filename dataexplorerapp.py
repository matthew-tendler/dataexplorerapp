import io
import os
import json
import zipfile
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st


def load_file(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    data = uploaded_file.getvalue()
    if name.endswith('.parquet'):
        return pd.read_parquet(io.BytesIO(data))
    if name.endswith('.csv'):
        return pd.read_csv(io.BytesIO(data))
    if name.endswith('.jsonl') or name.endswith('.jsonl.gz'):
        return pd.read_json(io.BytesIO(data), lines=True)
    if name.endswith('.json'):
        # try json array first, fall back to jsonl
        try:
            return pd.read_json(io.BytesIO(data), orient='records')
        except ValueError:
            return pd.read_json(io.BytesIO(data), lines=True)
    raise ValueError('Unsupported file type. Please upload Parquet, CSV, JSON, or JSONL')


def build_filters_form(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], bool]:
    with st.sidebar.form("filters_form"):
        st.header('Filters')
        selected_columns = st.multiselect('Columns to include in export', list(df.columns), default=list(df.columns))

        datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        if not datetime_cols:
            st.error('No datetime columns found. A time column is required for export.')
            submitted = st.form_submit_button('Apply filters')
            return None, False
        default_time_col = 'time' if 'time' in datetime_cols else datetime_cols[0]
        time_col = st.selectbox('Time column (required, max 30 days)', datetime_cols, index=datetime_cols.index(default_time_col))
        col_dates = df[time_col].dt.date
        dataset_min, dataset_max = col_dates.min(), col_dates.max()
        default_end = dataset_max
        default_start = max(dataset_min, default_end - pd.Timedelta(days=7))
        date_range = st.date_input('Select date range (required, ≤ 30 days)', (default_start, default_end))

        numeric_ranges = {}
        substrings = {}
        categorical_selections = {}
        for col in df.columns:
            if col == time_col:
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                min_val, max_val = float(df[col].min()), float(df[col].max())
                numeric_ranges[col] = st.slider(f'{col} range', min_val, max_val, (min_val, max_val))
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                min_date, max_date = df[col].dt.date.min(), df[col].dt.date.max()
                categorical_selections[f'__dt__{col}'] = st.date_input(f'{col} date range (optional)', (min_date, max_date))
            else:
                unique_vals = sorted(df[col].dropna().unique().tolist())
                if len(unique_vals) <= 100:
                    categorical_selections[col] = st.multiselect(f'{col} values', unique_vals)
                else:
                    substrings[col] = st.text_input(f'Substring in {col}')

        submitted = st.form_submit_button('Apply filters')

    if not submitted:
        return None, False

    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.warning('Please select a start and end date for the time window.')
        return None, False
    start_date, end_date = date_range
    if (end_date - start_date).days > 30:
        st.error('Selected time window exceeds 30 days. Please choose 30 days or less.')
        return None, False

    filtered = df
    mask_time = (filtered[time_col].dt.date >= start_date) & (filtered[time_col].dt.date <= end_date)
    filtered = filtered[mask_time]

    for col, (low, high) in numeric_ranges.items():
        if (low, high) != (float(df[col].min()), float(df[col].max())):
            filtered = filtered[filtered[col].between(low, high)]

    for key, rng in list(categorical_selections.items()):
        if key.startswith('__dt__'):
            col = key.replace('__dt__', '')
            if isinstance(rng, tuple) and len(rng) == 2:
                s, e = rng
                if not (s == df[col].dt.date.min() and e == df[col].dt.date.max()):
                    filtered = filtered[(filtered[col].dt.date >= s) & (filtered[col].dt.date <= e)]
        else:
            sel = rng
            if sel:
                filtered = filtered[filtered[key].isin(sel)]

    for col, text in substrings.items():
        if text:
            filtered = filtered[filtered[col].astype(str).str.contains(text, case=False, na=False)]

    filtered = filtered[selected_columns]
    return filtered, True


def group_and_chart(df: pd.DataFrame):
    st.subheader('Group and Chart')
    group_cols = st.multiselect('Group by columns', list(df.columns))
    agg_col = st.selectbox('Aggregate column (numeric)', [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])])
    agg_fn = st.selectbox('Aggregation', ['sum', 'mean', 'count', 'min', 'max'])

    if group_cols and agg_col:
        grouped = df.groupby(group_cols)[agg_col]
        if agg_fn == 'sum':
            result = grouped.sum().reset_index()
        elif agg_fn == 'mean':
            result = grouped.mean().reset_index()
        elif agg_fn == 'count':
            result = grouped.count().reset_index()
        elif agg_fn == 'min':
            result = grouped.min().reset_index()
        else:
            result = grouped.max().reset_index()
        st.bar_chart(result.set_index(group_cols)[agg_col])
        with st.expander('Aggregated data'):
            st.dataframe(result)


def main():
    st.set_page_config(page_title='Data Explorer', layout='wide')
    st.title('Data Explorer')
    st.write('Upload a Parquet, CSV, JSON, or JSONL file to explore.')

    uploaded = st.file_uploader('Upload file', type=['parquet', 'csv', 'json', 'jsonl'])
    if not uploaded:
        st.info('Awaiting file upload...')
        return

    try:
        df = load_file(uploaded)
    except Exception as e:
        st.error(f'Failed to load file: {e}')
        return

    # Attempt to parse datetimes automatically without noisy warnings
    for col in df.columns:
        if df[col].dtype == object:
            parsed = pd.to_datetime(df[col], errors='coerce')
            success_ratio = parsed.notna().mean()
            if success_ratio > 0.8:
                df[col] = parsed

    filtered, applied = build_filters_form(df)

    st.subheader('Export')
    if not applied:
        st.info('Set the required time window and any other filters, then click Apply to enable export.')
        return

    st.caption(f"Filtered rows: {len(filtered):,} × {len(filtered.columns)} columns")

    chunk_size = 500_000
    if len(filtered) <= chunk_size:
        csv = filtered.to_csv(index=False).encode('utf-8')
        st.download_button('Download CSV', data=csv, file_name='filtered.csv', mime='text/csv')
    else:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            num_parts = 0
            for start in range(0, len(filtered), chunk_size):
                end = min(start + chunk_size, len(filtered))
                part = filtered.iloc[start:end]
                num_parts += 1
                zf.writestr(f'filtered_part_{num_parts}.csv', part.to_csv(index=False))
        st.download_button('Download CSV (ZIP, 500k rows per file)', data=zip_buf.getvalue(), file_name='filtered_parts.zip', mime='application/zip')

    try:
        pbuf = io.BytesIO()
        filtered.to_parquet(pbuf, engine='pyarrow', index=False)
        st.download_button('Download Parquet', data=pbuf.getvalue(), file_name='filtered.parquet', mime='application/octet-stream')
    except Exception as e:
        st.caption(f'Parquet download unavailable: {e}')


if __name__ == '__main__':
    main()
