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

from src.casper_engine.sec_edgar_client import SecEdgarClient
from src.casper_engine.collect_records import CollectRecords
from src.casper_engine.clean_signal import clean_signal


class CasperEngine:
    
    def __init__(self, output_queue: Queue, cfg: dict, log: logging):
        self.output_queue = output_queue
        self.cfg = cfg
        self.log = log



    def _parse_utc(self, s: str | None):
        """Parse SEC-style ISO strings to tz-aware UTC datetimes, or NaT."""
        if not s:
            return pd.NaT
        # handles both "...Z" and "...-05:00" style
        return pd.to_datetime(s, utc=True, errors="coerce")
    
        
    
    def _filings_to_df(self, filings):
        df = pd.DataFrame([f.__dict__ for f in filings])  # dataclass -> dict
        # Make datetime columns tz-aware UTC
        df["acceptance_dt_utc"] = df["acceptanceDateTime"].map(self._parse_utc)
        df["filing_dt_utc"] = df["filingDate"].map(self._parse_utc)  # date-only becomes midnight UTC
        df["event_dt_utc"] = df["acceptance_dt_utc"].fillna(df["filing_dt_utc"])
        return df



    def _filings_to_recent_df(self, filings, dt_utc: datetime) -> pd.DataFrame:
        """
        Convert Form4Filing objects to a DataFrame and filter to only
        those with event_dt_utc > dt_utc (UTC-aware).
        """
        df = pd.DataFrame([f.__dict__ for f in filings])  # dataclass -> dict

        df["acceptance_dt_utc"] = df["acceptanceDateTime"].map(self._parse_utc)
        df["filing_dt_utc"] = df["filingDate"].map(self._parse_utc)  # date-only becomes midnight UTC
        df["event_dt_utc"] = df["acceptance_dt_utc"].fillna(df["filing_dt_utc"])

        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)

        return df[df["event_dt_utc"] > pd.Timestamp(dt_utc)].copy()



    def cycle(self, client: SecEdgarClient, company: dict):
        filings = client.get_form4_filings(cik=str(company.get("sec_cik")))
        df_filings = self._filings_to_df(filings, company.get("sec_last_accessed"))
        n = 0
        signals = []
        for _, row in df_filings.iterrows():
            collect_records = CollectRecords(primary_doc_url=row["primaryDocUrl"])
            records = collect_records.grab()
            sig = clean_signal(records)
            if sig:
                self.output_queue.put(sig)
                sig["doc_url"] = str(row["primaryDocUrl"])
                signals.append(sig)
            
            n += 1
    
        return n, signals



    def run(self):
        """hit sec at set intervals"""
        client = SecEdgarClient(user_agent=self.cfg.get("user_agent", ""))
        
        while True:
            m = 0
            all_signals = []
            with open(self.cfg.get("watchlist"), "r", encoding="utf-8") as f:
                watchlist = json.load(f)
            
            for company in watchlist:
                n, signals = self.cycle(client, company)
                m += n
                all_signals.extend(signals)
                company["sec_last_accessed"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

            
            with open(self.cfg.get("watchlist"), "w", encoding="utf-8") as f:
                json.dump(watchlist, f, indent=2)
            heartbeat = {
                "num_forms_scanned": m,
                "signals": all_signals
            }
            self.heartbeat_queue.put(heartbeat)
            
                
            time.sleep(int(self.cfg.get("sec_poll_freq")))

    
    