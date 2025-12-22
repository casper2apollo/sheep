""" casper_input.py

fetch my input please

"""
import logging
import yaml
import threading
import time
import json
import pandas as pd
from queue import Queue
from datetime import datetime, timezone
from typing import Optional, Union

from src.casper_engine.sec_edgar_client import SecEdgarClient
from src.casper_engine.collect_records import CollectRecords
from src.casper_engine.clean_signal import clean_signal



class CasperEngine:
    
    def __init__(self, output_queue: Queue, sec_user_agent: str, watchlist_path: str, log: logging, cycle_interval: int = 300):
        self.output_queue = output_queue
        self.sec_user_agent = sec_user_agent
        self.watchlist_path = watchlist_path
        self.cycle_interval = cycle_interval
        self.log = log



    def _parse_utc(self, s: str | None):
        """Parse SEC-style ISO strings to tz-aware UTC datetimes, or NaT."""
        if not s:
            return pd.NaT
        # handles both "...Z" and "...-05:00" style
        return pd.to_datetime(s, utc=True, errors="coerce")


    def _filings_to_recent_df(
        self,
        filings,
        dt_utc_raw: Optional[Union[str, datetime]],
    ) -> pd.DataFrame:
        """
        Convert Form4Filing objects to a DataFrame and filter to only
        those with event_dt_utc > dt_utc (UTC-aware).
        """

        df = pd.DataFrame([f.__dict__ for f in filings])  # dataclass -> dict
        if df.empty:
            return df

        df["acceptance_dt_utc"] = df["acceptanceDateTime"].map(self._parse_utc)
        df["filing_dt_utc"] = df["filingDate"].map(self._parse_utc)  # date-only becomes midnight UTC
        df["event_dt_utc"] = df["acceptance_dt_utc"].fillna(df["filing_dt_utc"])

        # --- normalize cutoff datetime ---
        dt_utc: Optional[datetime] = None

        if isinstance(dt_utc_raw, datetime):
            dt_utc = dt_utc_raw
        elif isinstance(dt_utc_raw, str) and dt_utc_raw.strip():
            s = dt_utc_raw.strip()
            # try ISO-8601 first
            try:
                # handle trailing 'Z'
                if s.endswith("Z"):
                    s = s.replace("Z", "+00:00")
                dt_utc = datetime.fromisoformat(s)
            except ValueError:
                # try date-only YYYY-MM-DD
                try:
                    dt_utc = datetime.strptime(s, "%Y-%m-%d")
                except ValueError:
                    self.log.warning("Could not parse sec_last_accessed=%r; skipping time filter", dt_utc_raw)

        # No usable cutoff -> return all rows
        if dt_utc is None:
            return df

        # ensure dt_utc is tz-aware UTC
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)

        return df[df["event_dt_utc"] > pd.Timestamp(dt_utc)].copy()




    def cycle(self, client: SecEdgarClient, company: dict):
        filings = client.get_form4_filings(cik=str(company.get("sec_cik")))
        df_filings = self._filings_to_recent_df(filings, company.get("sec_last_accessed"))
        n = 0
        signals = []
        for _, row in df_filings.iterrows():
            collect_records = CollectRecords(primary_doc_url=row["primaryDocUrl"])
            records = collect_records.grab()
            self.log.info(f"[{company.get("sec_symbol")}] record: {records}")
            sig = clean_signal(records=records, expected_symbol=company.get("sec_symbol"))
            if sig:
                self.log.info(f"[{company.get("sec_symbol")}] SIGNAL: {sig}")
                self.output_queue.put(sig)
                sig["doc_url"] = str(row["primaryDocUrl"])
                signals.append(sig)
            
            n += 1
    
        return n, signals



    def run(self):
        """hit sec at set intervals"""
        client = SecEdgarClient(user_agent=self.sec_user_agent)
        
        while True:
            m = 0
            all_signals = []
            with open(self.watchlist_path, "r", encoding="utf-8") as f:
                watchlist = json.load(f)
            
            for company in watchlist:
                n, signals = self.cycle(client, company)
                m += n
                all_signals.extend(signals)
                company["sec_last_accessed"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                self.log.info(f"finished processing {n} records of {company}")
            
            with open(self.watchlist_path, "w", encoding="utf-8") as f:
                json.dump(watchlist, f, indent=2)
                
            time.sleep(int(self.cycle_interval))

    
    