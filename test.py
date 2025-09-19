#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
ETL for Sales Dashboard (Alaa)
- Robust .xls → .xlsx conversion (Excel COM) with absolute paths
- Smart header detection
- Multiple filename patterns per input (separated by ';')

Run:
  python test.py --in-dir "." --out-dir ".\dashboard_data"
"""

import os, re, argparse
from glob import glob
import pandas as pd
import numpy as np

# ===================== Helpers =====================

def _normalize_col(s):
    if s is None:
        return s
    s = str(s).strip().replace('\u200f', '').replace('\u200e', '')
    s = re.sub(r'\s+', ' ', s)
    return s.lower()

def _pick(cols_map, *opts):
    for o in opts:
        k = _normalize_col(o)
        if k in cols_map:
            return cols_map[k]
    return None

def _find_header_row(path, required_any, max_scan=30, sheet_name=0, default_skip=2):
    """Scan the first rows to detect the real header; fallback to default_skip."""
    probe = pd.read_excel(
        path, header=None, nrows=max_scan, sheet_name=sheet_name,
        dtype=str, engine='openpyxl'
    )
    probe = probe.fillna('')
    for i in range(len(probe)):
        row = probe.iloc[i].astype(str).tolist()
        norm = [_normalize_col(x) for x in row]
        hits = 0
        for variants in required_any:
            if any(v in norm for v in variants):
                hits += 1
        if hits >= max(2, len(required_any)//2):
            print(f"[header-detect] using row {i} for {os.path.basename(path)}")
            return i
    print(f"[header-detect] fallback to row {default_skip} for {os.path.basename(path)}")
    return default_skip

def ensure_converted_xlsx(in_dir, path):
    """
    If file is .xls -> convert to .xlsx via Excel COM into in_dir/converted/<name>.xlsx
    Return the .xlsx path (absolute).
    """
    if path is None:
        return None
    ext = os.path.splitext(path)[1].lower()
    if ext != '.xls':
        return os.path.abspath(path)

    conv_dir = os.path.join(in_dir, "converted")
    os.makedirs(conv_dir, exist_ok=True)
    base = os.path.basename(path)
    xls_full = os.path.abspath(path)
    xlsx_full = os.path.abspath(os.path.join(conv_dir, os.path.splitext(base)[0] + ".xlsx"))

    if os.path.exists(xlsx_full) and os.path.getmtime(xlsx_full) >= os.path.getmtime(xls_full):
        print(f"[convert] reuse: {xlsx_full}")
        return xlsx_full

    print(f"[convert] Excel COM: {xls_full} -> {xlsx_full}")
    try:
        import win32com.client as win32
        excel = win32.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(xls_full, UpdateLinks=0, ReadOnly=True)
        wb.SaveAs(xlsx_full, 51)  # 51 = xlOpenXMLWorkbook (.xlsx)
        wb.Close(SaveChanges=False)
        excel.Quit()
    except Exception as e:
        try:
            excel.Quit()
        except Exception:
            pass
        raise SystemExit(f"Failed to convert {xls_full} to .xlsx via Excel COM: {e}")

    return xlsx_full

def pick_latest_by_glob(folder, pattern_multi):
    """
    Support multiple patterns separated by ';'
    Returns latest file by mtime or None.
    """
    patterns = [p.strip() for p in str(pattern_multi).split(';') if p.strip()]
    files = []
    for pat in patterns:
        files.extend(glob(os.path.join(folder, pat)))
    files = sorted(set(files), key=os.path.getmtime)
    return files[-1] if files else None

def to_json(df, path):
    df = df.copy()
    for c in df.columns:
        if 'date' in c:
            try:
                df[c] = pd.to_datetime(df[c]).dt.strftime('%Y-%m-%d')
            except Exception:
                pass
    df.to_json(path, orient='records', force_ascii=False)
    print("Wrote", path)

# ===================== Readers =====================

def read_sales_register(path):
    required_any = [
        ['outlet name','outlet','الفرع','اسم المعرض'],
        ['billdate','bill date','التاريخ'],
        ['billamt','bill amount','amount','net amount','netamount','صافي المبلغ'],
        ['total sales qty','qty','quantity','إجمالي عدد القطع','الكمية'],
    ]
    hdr = _find_header_row(path, required_any, default_skip=2)
    df = pd.read_excel(path, dtype=str, header=hdr, engine='openpyxl')
    df = df.loc[:, ~df.columns.astype(str).str.match(r'Unnamed', na=False)]
    cols = {_normalize_col(c): c for c in df.columns}

    def pick(*opts):
        for o in opts:
            k = _normalize_col(o)
            if k in cols: return cols[k]
        return None

    c_outlet = pick('outlet name','outlet','الفرع','اسم المعرض')
    c_amt    = pick('billamt','bill amount','amount','net amount','netamount','صافي المبلغ')
    c_date   = pick('billdate','bill date','التاريخ')
    c_salesm = pick('sales man name','sales man','salesman','cashier','اسم الكاشير')
    c_qty    = pick('total sales qty','qty','quantity','إجمالي عدد القطع','الكمية')

    if c_outlet is None or c_date is None or c_qty is None:
        raise ValueError(f"Sales Register missing key columns (need outlet/date/qty). Found: {list(df.columns)}")

    # استخرج الرقم من عمود الكمية (يتعامل مع قيم مثل "3 s" أو "1" أو "17 قطعة")
    qty_num = pd.to_numeric(
        df[c_qty].astype(str).str.extract(r'(\d+)')[0],
        errors='coerce'
    ).fillna(0).astype(int)

    out = pd.DataFrame({
        'outlet_name': df[c_outlet].astype(str).str.strip(),
        'bill_date':   pd.to_datetime(df[c_date], errors='coerce').dt.date,
        'amount':      pd.to_numeric(df.get(c_amt, 0), errors='coerce').fillna(0.0),
        'bills':       1,                         # كل صف = فاتورة
        'qty':         qty_num,                   # الكمية الرقمية المصححة
        'salesman_raw': df.get(c_salesm, 'Unknown').astype(str).fillna('Unknown')
    })

    # استخراج staff_id والاسم
    out['staff_id'] = out['salesman_raw'].str.extract(r'(^\d{2,})')[0]
    out['staff_id'] = out['staff_id'].fillna(out['salesman_raw'].str.extract(r'(\d{2,})')[0])
    out['staff_id'] = out['staff_id'].fillna('unknown')
    out['staff_name_guess'] = out['salesman_raw'].str.replace(r'^\d{2,}-','', regex=True)
    out['staff_name_guess'] = out['staff_name_guess'].str.replace(r'unknown\s*\d*','Unknown', regex=True).str.strip()

    # علّم فواتير القطعة الواحدة
    out['one_item_flag'] = (out['qty'] == 1).astype(int)

    # تجميع يوم/معرض/موظف
    grp = out.groupby(['bill_date','outlet_name','staff_id','staff_name_guess'], dropna=False).agg(
        net_amount=('amount','sum'),
        bills=('bills','sum'),
        qty=('qty','sum'),
        one_item_bills=('one_item_flag','sum')
    ).reset_index()
    return grp


def read_outlet_totals(path):
    required_any = [
        ['outlet name', 'outlet', 'اسم المعرض', 'الفرع'],
        ['bill dt.', 'bill date', 'التاريخ'],
        ['bill amount', 'billamt', 'amount', 'صافي المبلغ', 'net amount'],
    ]
    hdr = _find_header_row(path, required_any, default_skip=2)
    df = pd.read_excel(path, dtype=str, header=hdr, engine='openpyxl')
    df = df.loc[:, ~df.columns.astype(str).str.match(r'Unnamed', na=False)]
    cols = {_normalize_col(c): c for c in df.columns}

    c_outlet = _pick(cols, 'outlet name', 'outlet', 'اسم المعرض', 'الفرع')
    c_date = _pick(cols, 'bill dt.', 'bill date', 'التاريخ')
    c_amt = _pick(cols, 'bill amount', 'billamt', 'amount', 'صافي المبلغ', 'net amount')
    c_bills = _pick(cols, 'no of bills', 'bills', 'عدد الفواتير')

    if c_outlet is None or c_date is None:
        raise ValueError(
            f"Outlet totals missing key columns after header-detect. "
            f"Found: {list(df.columns)} (header row guessed={hdr})"
        )

    out = pd.DataFrame({
        'bill_date': pd.to_datetime(df[c_date], errors='coerce').dt.date,
        'outlet_name': df[c_outlet].astype(str).str.strip(),
        'outlet_amount': pd.to_numeric(df.get(c_amt, 0), errors='coerce').fillna(0.0),
        'outlet_bills': pd.to_numeric(df.get(c_bills, 0), errors='coerce').fillna(0.0).astype(int) if c_bills else 0
    })
    return out

def read_salesman_summary(path):
    required_any = [
        ['bill date', 'التاريخ'],
        ['outlet name', 'outlet', 'الفرع', 'اسم المعرض'],
        ['sales man name', 'sales man', 'salesman', 'cashier', 'اسم الكاشير'],
        ['net amount', 'صافي المبلغ'],
    ]
    hdr = _find_header_row(path, required_any, default_skip=2)
    df = pd.read_excel(path, dtype=str, header=hdr, engine='openpyxl')
    df = df.loc[:, ~df.columns.astype(str).str.match(r'Unnamed', na=False)]
    cols = {_normalize_col(c): c for c in df.columns}

    c_date = _pick(cols, 'bill date', 'التاريخ')
    c_outlet = _pick(cols, 'outlet name', 'outlet', 'الفرع', 'اسم المعرض')
    c_salesm = _pick(cols, 'sales man name', 'sales man', 'salesman', 'cashier', 'اسم الكاشير')
    c_net = _pick(cols, 'net amount', 'صافي المبلغ')
    c_bills = _pick(cols, 'total sales bills', 'bills', 'عدد الفواتير')
    c_ret = _pick(cols, 'sales return amount', 'return amount', 'مرتجعات')
    c_retb = _pick(cols, 'total sales return bills', 'return bills')

    out = pd.DataFrame({
        'bill_date': pd.to_datetime(df[c_date], errors='coerce').dt.date if c_date else pd.NaT,
        'outlet_name': df.get(c_outlet, 'Unknown').astype(str).str.strip(),
        'salesman_raw': df.get(c_salesm, 'Unknown').astype(str),
        'net_amount': pd.to_numeric(df.get(c_net, 0), errors='coerce').fillna(0.0),
        'bills': pd.to_numeric(df.get(c_bills, 0), errors='coerce').fillna(0).astype(int) if c_bills else 0,
        'return_amount': pd.to_numeric(df.get(c_ret, 0), errors='coerce').fillna(0.0) if c_ret else 0.0,
        'return_bills': pd.to_numeric(df.get(c_retb, 0), errors='coerce').fillna(0).astype(int) if c_retb else 0
    })
    out['staff_id'] = out['salesman_raw'].str.extract(r'(^\d{2,})')[0].fillna(
        out['salesman_raw'].str.extract(r'(\d{2,})')[0]
    ).fillna('unknown')
    out['staff_name_guess'] = out['salesman_raw'].str.replace(r'^\d{2,}-', '', regex=True).str.replace(
        r'unknown\s*\d*', 'Unknown', regex=True
    ).str.strip()
    return out

def read_staff_master(path):
    encodings = ['utf-8-sig','utf-8','cp1256','windows-1256','iso-8859-6']
    ext = os.path.splitext(path)[1].lower()
    sm = None
    if ext in ('.xls','.xlsx'):
        sm = pd.read_excel(path, dtype=str, engine='openpyxl')
    else:
        for enc in encodings:
            try:
                sm = pd.read_csv(path, dtype=str, encoding=enc)
                print(f"[staff_master] loaded with encoding={enc}")
                break
            except Exception:
                continue
    if sm is None:
        raise ValueError("Failed to read staff_master. Save as CSV (UTF-8) or XLSX.")

    for col in ['staff_id','staff_name_ar']:
        if col not in sm.columns:
            raise ValueError(f"staff_master missing column: {col}")

    for col in ['staff_name_en','system_name','target_amount']:
        if col not in sm.columns:
            sm[col] = ''

    sm['staff_id'] = sm['staff_id'].astype(str).str.strip()
    sm['target_amount'] = pd.to_numeric(sm['target_amount'], errors='coerce').fillna(0.0)

    return sm[['staff_id','staff_name_ar','staff_name_en','system_name','target_amount']]

def infer_staff_outlet_history(facts_daily):
    df = facts_daily.copy()
    df['bill_date'] = pd.to_datetime(df['bill_date'], errors='coerce')
    df = df.dropna(subset=['bill_date', 'outlet_name'])

    day_pick = (
        df.sort_values(['bills', 'net_amount'], ascending=[False, False])
          .drop_duplicates(['bill_date', 'staff_id'])
          [['bill_date', 'staff_id', 'outlet_name']]
          .sort_values(['staff_id', 'bill_date'])
    )

    rows = []
    for staff_id, grp in day_pick.groupby('staff_id', sort=False):
        grp = grp.reset_index(drop=True)
        if grp.empty:
            continue
        cur_outlet = grp.loc[0, 'outlet_name']
        start = grp.loc[0, 'bill_date']
        prev = start
        for i in range(1, len(grp)):
            d = grp.loc[i, 'bill_date']
            o = grp.loc[i, 'outlet_name']
            delta_days = int((d - prev).days)
            if (o != cur_outlet) and (delta_days <= 7):
                rows.append({
                    'staff_id': staff_id,
                    'outlet_name': cur_outlet,
                    'start_date': start,
                    'end_date': grp.loc[i - 1, 'bill_date']
                })
                cur_outlet = o
                start = d
            prev = d
        rows.append({
            'staff_id': staff_id,
            'outlet_name': cur_outlet,
            'start_date': start,
            'end_date': None
        })
    hist = pd.DataFrame(rows)
    if not hist.empty:
        for c in ['start_date', 'end_date']:
            hist[c] = pd.to_datetime(hist[c]).dt.date
    return hist

# ===================== Main =====================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in-dir',  type=str, default='.')
    ap.add_argument('--out-dir', type=str, default='./dashboard_data')
    # multiple patterns separated by ';'
    ap.add_argument('--glob-salesreg', default='salesreg_*.xls*')
    ap.add_argument('--glob-outlet',   default='sales_*.xls*')
    ap.add_argument('--glob-sum',      default='3041_Salesman_*Summary*.xls*;salesman_*.xls*')
    ap.add_argument('--staff-master',  default='staff_master.csv')
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    p_reg = pick_latest_by_glob(args.in_dir, args.glob_salesreg)
    p_out = pick_latest_by_glob(args.in_dir, args.glob_outlet)
    p_sum = pick_latest_by_glob(args.in_dir, args.glob_sum)
    if not p_reg:
        raise SystemExit("No Sales Register file found.")
    if not p_out:
        print("WARN: No Outlet totals file found.")
    if not p_sum:
        print("WARN: No Salesman summary file found.")

    # .xls → .xlsx (absolute paths)
    p_reg = ensure_converted_xlsx(args.in_dir, p_reg)
    p_out = ensure_converted_xlsx(args.in_dir, p_out) if p_out else None
    p_sum = ensure_converted_xlsx(args.in_dir, p_sum) if p_sum else None

    print("Using files:", p_reg, p_out, p_sum)

    # read + merge
    reg = read_sales_register(p_reg)
    if p_sum:
        summ = read_salesman_summary(p_sum)
        m = pd.merge(
            reg,
            summ[['bill_date', 'outlet_name', 'staff_id', 'net_amount', 'bills', 'return_amount', 'return_bills']],
            on=['bill_date', 'outlet_name', 'staff_id'],
            how='outer',
            suffixes=('_reg', '_sum')
        )

        def coalesce(a,b):
            return np.where(pd.notnull(b) & (b!=0), b, a)

        m['net_amount'] = coalesce(m['net_amount_reg'], m['net_amount_sum'])
        m['bills']      = coalesce(m['bills_reg'],      m['bills_sum'])
        # one_item_bills فقط من السجل
        m['one_item_bills'] = m.get('one_item_bills', 0).fillna(0).astype(int)

        m['return_amount'] = m.get('return_amount', 0.0).fillna(0.0)
        m['return_bills']  = m.get('return_bills', 0).fillna(0).astype(int)
        m['net_amount'] = pd.to_numeric(m['net_amount'], errors='coerce').fillna(0.0)
        m['bills']      = pd.to_numeric(m['bills'], errors='coerce').fillna(0).astype(int)

        m['staff_name_guess'] = m.get('staff_name_guess_reg', m.get('staff_name_guess_sum','Unknown'))

        facts = m[['bill_date','outlet_name','staff_id','staff_name_guess',
                   'net_amount','bills','one_item_bills','return_amount','return_bills']].copy()

    else:
        reg['return_amount'] = 0.0
        reg['return_bills']  = 0
        # خذ one_item_bills من السجل مباشرة
        facts = reg[['bill_date','outlet_name','staff_id','staff_name_guess',
                     'net_amount','bills','one_item_bills','return_amount','return_bills']].copy()

    # staff master (optional)
    staff_path = os.path.join(args.in_dir, args.staff_master)
    if os.path.exists(staff_path):
        sm = read_staff_master(staff_path)
        facts = facts.merge(sm[['staff_id','staff_name_ar']], on='staff_id', how='left')
        facts['staff_name'] = facts['staff_name_ar'].combine_first(facts['staff_name_guess'])
    else:
        facts['staff_name'] = facts['staff_name_guess']

    # normalize outlets
    facts['outlet_name'] = facts['outlet_name'].astype(str).str.replace(r'\s+',' ', regex=True).str.strip()

    # outlet dimension
    outlets = pd.DataFrame({'outlet_name': sorted(facts['outlet_name'].dropna().unique().tolist())})
    outlets['outlet_id'] = outlets.index + 1
    facts = facts.merge(outlets, on='outlet_name', how='left')

    # staff dimension (دمج target_amount إن وجد)
    if os.path.exists(staff_path):
        staff = facts[['staff_id','staff_name']].drop_duplicates().merge(
            sm[['staff_id','target_amount']], on='staff_id', how='left'
        )
        staff['target_amount'] = pd.to_numeric(staff['target_amount'], errors='coerce').fillna(0.0)
    else:
        staff = facts[['staff_id','staff_name']].drop_duplicates()
        staff['target_amount'] = 0.0
    staff = staff.sort_values('staff_id')
    staff['staff_name'] = staff['staff_name'].fillna('Unknown')

    # history
    hist = infer_staff_outlet_history(facts)

    # outputs
    os.makedirs(args.out_dir, exist_ok=True)
    to_json(facts, os.path.join(args.out_dir, 'facts.json'))
    to_json(hist,  os.path.join(args.out_dir, 'staff_outlet_history.json'))
    to_json(staff, os.path.join(args.out_dir, 'staff.json'))
    to_json(outlets[['outlet_id','outlet_name']], os.path.join(args.out_dir, 'outlets.json'))
    facts.to_csv(os.path.join(args.out_dir, 'facts.csv'), index=False, encoding='utf-8-sig')

    print("Done. Records:", len(facts))

if __name__ == '__main__':
    main()
