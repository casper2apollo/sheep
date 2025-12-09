# full loop


# scan SEC for list of companies at regular intervals

# for each new form 4 document parse to dictionary

# check that we have expected contents, 
    # issuer for google is not google
    # issuer is geing accuired by google
    # stock is common stock
    
# send buy signal to kafka producer

import json, time, logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sheep_shear.yarn.collect_wool import CollectWool
from sheep_shear.yarn.clean_wool import clean_wool
from sheep_shear.herd.call_herd import SecEdgarClient


log = logging.getLogger(__name__)



class SheepShear:
    
    def __init__(self, sec_cfg: dict, path_cfg: dict, buy_signal_queue):
        self.sec_cfg = sec_cfg
        self.path_cfg = path_cfg
        self.buy_signal_queue = buy_signal_queue
        self.TZ = ZoneInfo("America/Toronto")
        
        
    def _next_even_hour_boundary(now: datetime) -> datetime:
        """move to the next top-of-hour
        if it's an odd hour, skip to the next hour so we land on an even hour (2,4,6,...)
        """
        nxt = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        if nxt.hour % 2 == 1:
            nxt += timedelta(hours=1)
        return nxt
    
    
    def _parse_utc(s: str | None):
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
        # choose best timestamp for filtering
        df["event_dt_utc"] = df["acceptance_dt_utc"].fillna(df["filing_dt_utc"])
        return df
    
    
    def _filter_recent(df: pd.DataFrame, dt_utc: datetime) -> pd.DataFrame:
        # ensure dt_utc is tz-aware UTC
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)

        return df[df["event_dt_utc"] > pd.Timestamp(dt_utc)].copy()
    
    
    
    def clean_wool(records: dict, expected_symbol: str) -> dict:
        sym = (records.get("issuer_symbol") or "").strip().upper()
        exp = (expected_symbol or "").strip().upper()
        a_or_d = (records.get("A_or_D") or "").strip().upper()

        if sym != exp:
            return {}

        if a_or_d != "A":
            return {}

        return records

        
    
    def shear(self, client: SecEdgarClient, sheep: dict):
        buy_signals = []
        
        sec_cik = str(sheep.get("sec_cik"))
        filings = client.get_form4_filings(cik=sec_cik)

        dt_utc = datetime.fromisoformat(
            sheep["sec_last_accessed"].replace("Z", "+00:00")
        )
        
        df = self._filings_to_df(filings)
        df_new = self._filter_recent(df, dt_utc)
        
        expected_symbol = sheep["sec_symbol"]

        n = 0
        ## convert filings to buy signals
        for _, row in df_new.iterrows():
            primary_url = row["primaryDocUrl"]
            collect_wool = CollectWool(primary_doc_url=primary_url)
            records = collect_wool.grab()
            sig = clean_wool(records=records, expected_symbol=expected_symbol)
            if sig:
                self.buy_signal_queue.put(sig)
                n += 1
        
        return n
            
            
                
    def run_cycle(self, client: SecEdgarClient) -> int:
        """One scan pass across all sheep. Returns number of signals queued."""
        sheep_cache_path = self.path_cfg["sheep_cache"]
        with open(sheep_cache_path, "r", encoding="utf-8") as f:
            sheep_cache = json.load(f)

        n = 0
        for sheep in sheep_cache:
            if not isinstance(sheep, dict):
                continue
            temp = self.shear(client, sheep)
            n += temp
        return n



    def shear_loop(self) -> None:
        user_agent = self.sec_cfg["user_agent"]
        client = SecEdgarClient(user_agent=user_agent)

        while True:
            now = datetime.now(self.TZ)
            run_at = self._next_even_hour_boundary(now)
            sleep_s = (run_at - now).total_seconds()

            if sleep_s > 0:
                time.sleep(sleep_s)

            # Run the cycle exactly at the boundary
            started = datetime.now(self.TZ)
            count = self.run_cycle(client)
            finished = datetime.now(self.TZ)

            log.info(f"[{started.isoformat()}] cycle done: queued={count} duration={(finished-started).total_seconds():.2f}s")



