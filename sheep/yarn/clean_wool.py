""" spin_yarn.py
    
for functions where we filter and check the pulled files files

multi-threading possible since each form process is independent

return the real trad signals

"""

from typing import Any, Dict, Union, List

def clean_wool(records: Union[Dict[str, Any], List[Dict[str, Any]]], expected_symbol: str) -> Dict[str, Any]:
    # Normalize to a single dict or bail
    if isinstance(records, list):
        if len(records) != 1:
            return {}
        record = records[0]
        if not isinstance(record, dict):
            return {}
    elif isinstance(records, dict):
        record = records
    else:
        return {}

    sym = (record.get("issuer_symbol") or "").strip().upper()
    exp = (expected_symbol or "").strip().upper()
    a_or_d = (record.get("A_or_D") or "").strip().upper()

    if sym == exp:
        return {}

    if a_or_d != "A":
        return {}
    
    if not sym:
        return {}
    
    clean_record = {
        "action": "buy",
        "symbol": sym
    }

    return clean_record
