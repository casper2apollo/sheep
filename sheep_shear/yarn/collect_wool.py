""" collect_wool.py

parse xml
functions for reading and parsing the xml taken from the 
    
PRIMARY_DOC_URL = "https://www.sec.gov/Archives/edgar/data/1652044/000119312525272562/xslF345X05/ownership.xml"

"""
from io import StringIO
import pandas as pd
import numpy as np
import requests
import time
import re


class CollectWool:
    
    HEADERS = {
        "User-Agent": "casper casper@2apollo.net",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "www.sec.gov",
    }
    
    def __init__(self, primary_doc_url: str):
        self.primary_doc_url = primary_doc_url
        

    def fetch(self, sleep_s: float = 0.2) -> requests.Response:
        """Small helper with light throttling."""
        time.sleep(sleep_s)
        r = requests.get(self.primary_doc_url, headers=self.HEADERS, timeout=30)
        r.raise_for_status()
        return r
    
    
    def get_issuer_name_and_symbol(self, tables):
        issuer_prefix = "2. Issuer Name and Ticker or Trading Symbol"

        for t in tables:
            # flatten to strings and scan every cell safely
            vals = (
                t.astype(str)
                .replace({"nan": "", "None": ""})
                .values
                .ravel()
            )
            for cell in vals:
                cell = str(cell).strip()
                if cell.startswith(issuer_prefix):
                    m = re.search(
                        rf"^{re.escape(issuer_prefix)}\s*(.*?)\s*\[\s*([^\]]+?)\s*\]\s*$",
                        cell
                    )
                    if m:
                        return m.group(1).strip(), m.group(2).strip()

        return None, None


    def _flatten_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if isinstance(out.columns, pd.MultiIndex):
            out.columns = [
                " ".join([str(x) for x in tup if x and str(x) != "nan"]).strip()
                for tup in out.columns.to_list()
            ]
        else:
            out.columns = [str(c).strip() for c in out.columns]
        return out


    def _pick_col(self, cols, patterns):
        for pat in patterns:
            rx = re.compile(pat, re.I)
            for c in cols:
                if rx.search(c):
                    return c
        raise KeyError(f"Could not find column matching any of: {patterns}\nColumns: {list(cols)}")

        
    def summarize_table_I(self, df_table: pd.DataFrame) -> pd.DataFrame:
        df = self._flatten_cols(df_table)

        # Pick columns (tuned for Form 4 Table I)
        title_col  = self._pick_col(df.columns, [r"Title of Security"])
        amt_col    = self._pick_col(df.columns, [r"^\s*Amount\s*$", r"\bSecurities\b.*\bAmount\b"])
        ad_col     = self._pick_col(df.columns, [r"\(A\)\s*or\s*\(D\)"])
        price_col  = self._pick_col(df.columns, [r"^\s*Price\s*$", r"\bPrice\b"])
        tot_col    = self._pick_col(df.columns, [r"Amount of Securities Beneficially Owned"])

        work = df[[title_col, amt_col, ad_col, price_col, tot_col]].copy()

        # --- amount ---
        amt_str = (
            work[amt_col].astype(str).str.strip()
            .str.replace(",", "", regex=False)
            .str.replace(r"\([^)]*\)", "", regex=True)   # remove (2), (6), (9), etc
            .str.strip()
        )
        work["amount"] = pd.to_numeric(amt_str, errors="coerce")


        # --- price ---
        price_str = work[price_col].astype(str).str.strip()
        # remove parenthetical footnotes: "0.91(1)" -> "0.91", "(1)(2)" -> ""
        price_str = price_str.str.replace(r"\([^)]*\)", "", regex=True).str.strip()
        price_num = (
            price_str.str.replace(",", "", regex=False)
            .str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
        )
        work["price"] = pd.to_numeric(price_num, errors="coerce")

        # --- total owned ---
        tot_str = (
            work[tot_col].astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
            .replace({"nan": "", "None": ""})
        )
        work["total_owned"] = pd.to_numeric(tot_str, errors="coerce")

        # Keep only actual transaction rows that have a transaction amount
        work = work.dropna(subset=["amount"]).copy()
        work["A_or_D"] = work[ad_col].astype(str).str.strip()
        work["security"] = work[title_col].astype(str).str.strip()

        # Total amount and total owned (owned after transaction is typically constant; max is safe)
        totals = (
            work.groupby(["security", "A_or_D"], as_index=False)
                .agg(
                    total_amount=("amount", "sum"),
                    total_amount_owned=("total_owned", "max"),
                )
        )

        # Weighted average price using only non-NaN prices
        priced = work.dropna(subset=["price", "amount"]).copy()
        if priced.empty:
            out = totals.copy()
            out["avg_price"] = np.nan
        else:
            priced["wpx"] = priced["price"] * priced["amount"]
            num = priced.groupby(["security", "A_or_D"], as_index=False)["wpx"].sum()
            den = priced.groupby(["security", "A_or_D"], as_index=False)["amount"].sum().rename(columns={"amount": "w"})
            avg = num.merge(den, on=["security", "A_or_D"], how="left")
            avg["avg_price"] = avg["wpx"] / avg["w"]
            avg = avg[["security", "A_or_D", "avg_price"]]
            out = totals.merge(avg, on=["security", "A_or_D"], how="left")

        return out[["security", "total_amount", "A_or_D", "avg_price", "total_amount_owned"]].sort_values(["security", "A_or_D"])
    

    def _find_table_I(self, tables: list[pd.DataFrame]) -> pd.DataFrame:
        """Pick the first table that matches Table I column signatures."""
        required = [
            [r"Title of Security"],
            [r"^\s*Amount\s*$", r"\bSecurities\b.*\bAmount\b"],
            [r"\(A\)\s*or\s*\(D\)"],
            [r"^\s*Price\s*$", r"\bPrice\b"],
        ]

        best = None
        best_score = -1

        for t in tables:
            try:
                df = self._flatten_cols(t)
                score = 0
                # count how many required columns we can locate
                for pats in required:
                    try:
                        self._pick_col(df.columns, pats)
                        score += 1
                    except KeyError:
                        pass

                if score > best_score:
                    best_score = score
                    best = t

                if score == len(required):
                    return t  # perfect match
            except Exception:
                continue

        raise KeyError(f"Could not locate Table I. Best score={best_score}/4. "
                    f"Top candidate columns={list(self._flatten_cols(best).columns) if best is not None else 'n/a'}")


    def grab(self):
        
        response = self.fetch()
        
        html = response.content.decode("utf-8", errors="replace")
        tables = pd.read_html(StringIO(html))   # list[pd.DataFrame]
        
        issuer_name, issuer_symbol = self.get_issuer_name_and_symbol(tables)
        table_I = self._find_table_I(tables)
        summary_df = self.summarize_table_I(table_I)
        
        summary_df["issuer_name"] = issuer_name
        summary_df["issuer_symbol"] = issuer_symbol
        
        return summary_df.to_dict("records")
        
        
