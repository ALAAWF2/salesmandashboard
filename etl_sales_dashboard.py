#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL for Sales Dashboard (Alaa) — reads three daily reports + optional Staff Master,
produces clean JSON for a lightweight web dashboard (no Power BI).

Inputs (examples; configure via --in-dir and --glob-* flags):
  - sales register:      salesreg_YYYY-MM-DD.xls
  - outlet totals:       sales_YYYY-MM-DD.xls
  - salesman summary:    3041_Salesman_W_rn_Summary_*.xls
  - staff master (opt.): staff_master.csv  (columns: staff_id, staff_name_ar, staff_name_en, system_name)

Outputs (in out-dir, default ./dashboard_data):
  - facts.json                  (daily staff metrics per outlet)
  - staff_outlet_history.json   (inferred assignment intervals per staff)
  - staff.json                  (staff dimension)
  - outlets.json                (outlet dimension)
  - facts.csv                   (same as facts.json but CSV for quick checks)

Run:
  python etl_sales_dashboard.py --in-dir "C:\path\to\folder\with\xls" --out-dir "C:\path\to\dashboard_data"

Notes:
- Designed to tolerate small header name differences.
- For .xls, you need xlrd installed (pip install xlrd==1.2.0).
- If your files are .xlsx, install openpyxl; pandas will auto-pick.
"""
import re, os, sys, json, argparse, datetime as dt
from glob import glob

import pandas as pd
import numpy as np

def _normalize_col(s):
    if s is None: return s
    s = str(s).strip()
    s = s.replace('\u200f','').replace('\u200e','')  # strip RTL marks
    s = re.sub(r'\s+', ' ', s)
    return s.lower()

def read_sales_register(path):
    # Expected columns (any of these variants are accepted):
    # Outlet Name | BillNo(Prefix) | BillAmt | BillDate | Sales Man | Total Sales Qty
    df = pd.read_excel(path, dtype=str)
    cols = { _normalize_col(c): c for c in df.columns }
    def pick(*opts):
        for o in opts:
            if _normalize_col(o) in cols: return cols[_normalize_col(o)]
        return None
    c_outlet = pick('outlet name','outlet','الفرع','اسم المعرض')
    c_billno = pick('billno(prefix)','billno','bill no','رقم الفاتورة')
    c_amt    = pick('billamt','bill amount','amount','net amount','netamount','صافي المبلغ')
    c_date   = pick('billdate','bill date','التاريخ')
    c_salesm = pick('sales man','salesman','cashier','اسم الكاشير','sales man name')
    c_qty    = pick('total sales qty','qty','quantity')
    if c_outlet is None or c_date is None:
        raise ValueError("Sales Register missing key columns. Found: %s" % list(df.columns))

    out = pd.DataFrame({
        'outlet_name': df[c_outlet].astype(str).str.strip(),
        'bill_date':   pd.to_datetime(df[c_date], errors='coerce').dt.date,
        'amount':      pd.to_numeric(df.get(c_amt, 0), errors='coerce').fillna(0.0),
        'bills':       1,  # each row is a bill
        'qty':         pd.to_numeric(df.get(c_qty, 0), errors='coerce').fillna(0.0),
        'salesman_raw': df.get(c_salesm, 'Unknown').astype(str).fillna('Unknown')
    })
    # Extract staff_id as leading digits in "4491-Name" patterns
    out['staff_id'] = out['salesman_raw'].str.extract(r'(^\d{2,})')[0]
    # Fallback: tail digits like "Unknown 789" -> 789
    out['staff_id'] = out['staff_id'].fillna(out['salesman_raw'].str.extract(r'(\d{2,})')[0])
    out['staff_id'] = out['staff_id'].fillna('unknown')
    # Canonical salesman name
    out['staff_name_guess'] = out['salesman_raw'].str.replace(r'^\d{2,}-','', regex=True)
    out['staff_name_guess'] = out['staff_name_guess'].str.replace(r'unknown\s*\d*','Unknown', regex=True).str.strip()
    # Aggregate to per-day per-staff per-outlet for speed
    grp = out.groupby(['bill_date','outlet_name','staff_id','staff_name_guess'], dropna=False).agg(
        net_amount=('amount','sum'),
        bills=('bills','sum'),
        qty=('qty','sum')
    ).reset_index()
    return grp

def read_outlet_totals(path):
    # Expected columns: Outlet Name | Bill Dt. | Bill Amount | No Of Bills
    df = pd.read_excel(path, dtype=str)
    cols = { _normalize_col(c): c for c in df.columns }
    def pick(*opts):
        for o in opts:
            if _normalize_col(o) in cols: return cols[_normalize_col(o)]
        return None
    c_outlet = pick('outlet name','outlet','اسم المعرض','الفرع')
    c_date   = pick('bill dt.','bill date','التاريخ')
    c_amt    = pick('bill amount','billamt','amount','صافي المبلغ')
    c_bills  = pick('no of bills','bills','عدد الفواتير')
    if c_outlet is None or c_date is None:
        raise ValueError("Outlet totals missing key columns.")
    out = pd.DataFrame({
        'bill_date': pd.to_datetime(df[c_date], errors='coerce').dt.date,
        'outlet_name': df[c_outlet].astype(str).str.strip(),
        'outlet_amount': pd.to_numeric(df.get(c_amt, 0), errors='coerce').fillna(0.0),
        'outlet_bills': pd.to_numeric(df.get(c_bills, 0), errors='coerce').fillna(0.0).astype(int)
    })
    return out

def read_salesman_summary(path):
    # Expected columns: Bill Date | Outlet Name | Sales Man Name | Net Amount | Total Sales Bills | Sales Return Amount | Total Sales Return Bills
    df = pd.read_excel(path, dtype=str)
    cols = { _normalize_col(c): c for c in df.columns }
    def pick(*opts):
        for o in opts:
            if _normalize_col(o) in cols: return cols[_normalize_col(o)]
        return None
    c_date   = pick('bill date','التاريخ')
    c_outlet = pick('outlet name','outlet','الفرع','اسم المعرض')
    c_salesm = pick('sales man name','sales man','salesman','cashier','اسم الكاشير')
    c_net    = pick('net amount','صافي المبلغ')
    c_bills  = pick('total sales bills','bills','عدد الفواتير')
    c_ret    = pick('sales return amount','return amount','مرتجعات')
    c_retb   = pick('total sales return bills','return bills')
    out = pd.DataFrame({
        'bill_date': pd.to_datetime(df[c_date], errors='coerce').dt.date if c_date else pd.NaT,
        'outlet_name': df.get(c_outlet, 'Unknown').astype(str).str.strip(),
        'salesman_raw': df.get(c_salesm, 'Unknown').astype(str),
        'net_amount': pd.to_numeric(df.get(c_net, 0), errors='coerce').fillna(0.0),
        'bills': pd.to_numeric(df.get(c_bills, 0), errors='coerce').fillna(0).astype(int),
        'return_amount': pd.to_numeric(df.get(c_ret, 0), errors='coerce').fillna(0.0),
        'return_bills': pd.to_numeric(df.get(c_retb, 0), errors='coerce').fillna(0).astype(int)
    })
    out['staff_id'] = out['salesman_raw'].str.extract(r'(^\d{2,})')[0].fillna(out['salesman_raw'].str.extract(r'(\d{2,})')[0]).fillna('unknown')
    out['staff_name_guess'] = out['salesman_raw'].str.replace(r'^\d{2,}-','', regex=True).str.replace(r'unknown\s*\d*','Unknown', regex=True).str.strip()
    # Keep as-is (already aggregated)
    return out

def read_staff_master(path):
    sm = pd.read_csv(path, dtype=str)
    # required: staff_id, staff_name_ar
    for col in ['staff_id','staff_name_ar']:
        if col not in sm.columns:
            raise ValueError(f"staff_master missing column: {col}")
    # optional columns
    for col in ['staff_name_en','system_name']:
        if col not in sm.columns: sm[col] = ''
    sm['staff_id'] = sm['staff_id'].astype(str).str.strip()
    return sm[['staff_id','staff_name_ar','staff_name_en','system_name']]

def infer_staff_outlet_history(facts_daily):
    """Build staff->outlet intervals by dominant-outlet-per-day then compress contiguous runs."""
    # Per staff/day pick outlet with max bills (tie -> max net_amount)
    day_pick = facts_daily.sort_values(['bills','net_amount'], ascending=[False,False]).drop_duplicates(['bill_date','staff_id'])
    day_pick = day_pick[['bill_date','staff_id','outlet_name']].copy()
    day_pick = day_pick.sort_values(['staff_id','bill_date'])
    # Build runs
    rows = []
    for staff_id, grp in day_pick.groupby('staff_id', sort=False):
        grp = grp.sort_values('bill_date').reset_index(drop=True)
        if grp.empty: continue
        cur_outlet = grp.loc[0,'outlet_name']
        start = grp.loc[0,'bill_date']
        prev = start
        for i in range(1, len(grp)):
            d = grp.loc[i,'bill_date']
            o = grp.loc[i,'outlet_name']
            if o != cur_outlet and (d - prev).days <= 7:
                # outlet changed; close previous interval day before change
                rows.append({'staff_id': staff_id, 'outlet_name': cur_outlet, 'start_date': start, 'end_date': grp.loc[i-1,'bill_date']})
                cur_outlet = o
                start = d
            prev = d
        rows.append({'staff_id': staff_id, 'outlet_name': cur_outlet, 'start_date': start, 'end_date': None})
    hist = pd.DataFrame(rows)
    return hist

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in-dir',  type=str, default='.', help='Folder containing XLS/XLSX inputs')
    ap.add_argument('--out-dir', type=str, default='./dashboard_data', help='Folder to write JSON/CSVs')
    ap.add_argument('--glob-salesreg', default='salesreg_*.xls*', help='Glob for Sales Register')
    ap.add_argument('--glob-outlet',   default='sales_*.xls*',     help='Glob for Outlet totals')
    ap.add_argument('--glob-sum',      default='3041_Salesman_*Summary*.xls*', help='Glob for salesman summary')
    ap.add_argument('--staff-master',  default='staff_master.csv', help='CSV mapping staff_id -> names')
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # Pick latest files by mtime
    def latest(gl):
        files = sorted(glob(os.path.join(args.in_dir, gl)), key=os.path.getmtime)
        return files[-1] if files else None

    p_reg = latest(args.glob_salesreg)
    p_out = latest(args.glob_outlet)
    p_sum = latest(args.glob_sum)

    if not p_reg: raise SystemExit("No Sales Register file found.")
    if not p_out: print("WARN: No Outlet totals file found.")
    if not p_sum: print("WARN: No Salesman summary file found.")

    print("Using files:", p_reg, p_out, p_sum)

    reg = read_sales_register(p_reg)
    if p_sum:
        summ = read_salesman_summary(p_sum)
        # unify fields, and prefer more accurate bills/returns from summary where available
        # Merge on keys
        m = pd.merge(reg, summ[['bill_date','outlet_name','staff_id','net_amount','bills','return_amount','return_bills']],
                     on=['bill_date','outlet_name','staff_id'], how='outer', suffixes=('_reg','_sum'))
        # Combine
        def coalesce(a,b): 
            return np.where(pd.notnull(b) & (b!=0), b, a)
        m['net_amount'] = coalesce(m['net_amount_reg'], m['net_amount_sum']).fillna(0.0)
        m['bills']      = coalesce(m['bills_reg'], m['bills_sum']).fillna(0).astype(int)
        m['return_amount'] = m.get('return_amount', 0.0)
        m['return_bills']  = m.get('return_bills', 0).fillna(0).astype(int)
        # staff name
        m['staff_name_guess'] = m.get('staff_name_guess_reg', m.get('staff_name_guess_sum','Unknown'))
        facts = m[['bill_date','outlet_name','staff_id','staff_name_guess','net_amount','bills','return_amount','return_bills']].copy()
    else:
        reg['return_amount'] = 0.0
        reg['return_bills'] = 0
        facts = reg[['bill_date','outlet_name','staff_id','staff_name_guess','net_amount','bills','return_amount','return_bills']].copy()

    # Attach staff master if available
    staff_path = os.path.join(args.in_dir, args.staff_master)
    staff_dim = pd.DataFrame(columns=['staff_id','staff_name_ar','staff_name_en','system_name'])
    if os.path.exists(staff_path):
        staff_dim = read_staff_master(staff_path)
        facts = facts.merge(staff_dim[['staff_id','staff_name_ar']], on='staff_id', how='left')
        facts['staff_name'] = facts['staff_name_ar'].combine_first(facts['staff_name_guess'])
    else:
        facts['staff_name'] = facts['staff_name_guess']

    # Clean outlet names (trim spaces; unify dashes)
    facts['outlet_name'] = facts['outlet_name'].astype(str).str.replace(r'\s+',' ', regex=True).str.strip()

    # Build outlet dim
    outlets = pd.DataFrame({'outlet_name': sorted(facts['outlet_name'].dropna().unique().tolist())})
    outlets['outlet_id'] = outlets.index + 1

    # Map outlet_id in facts
    facts = facts.merge(outlets, on='outlet_name', how='left')

    # Build staff dim (after merge)
    staff = facts[['staff_id','staff_name']].drop_duplicates().sort_values('staff_id')
    staff['staff_name'] = staff['staff_name'].fillna('Unknown')

    # Infer history
    hist = infer_staff_outlet_history(facts.rename(columns={'net_amount':'net_amount', 'bills':'bills'}))

    # Exports
    def to_json(df, name):
        p = os.path.join(args.out_dir, name)
        df_out = df.copy()
        # Convert dates to iso
        for c in df_out.columns:
            if 'date' in c:
                try:
                    df_out[c] = pd.to_datetime(df_out[c]).dt.strftime('%Y-%m-%d')
                except Exception:
                    pass
        df_out.to_json(p, orient='records', force_ascii=False)
        print("Wrote", p)

    to_json(facts, 'facts.json')
    to_json(hist, 'staff_outlet_history.json')
    to_json(staff, 'staff.json')
    to_json(outlets[['outlet_id','outlet_name']], 'outlets.json')

    # CSV for QA
    facts.to_csv(os.path.join(args.out_dir, 'facts.csv'), index=False, encoding='utf-8-sig')
    print("Done. Records:", len(facts))

if __name__ == '__main__':
    main()
