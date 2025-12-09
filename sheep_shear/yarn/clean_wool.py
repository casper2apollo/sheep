""" spin_yarn.py
    
for functions where we filter and check the pulled files files

multi-threading possible since each form process is independent

return the real trad signals

"""


def clean_wool(records: dict, expected_symbol: str) -> dict:
    sym = (records.get("issuer_symbol") or "").strip().upper()
    exp = (expected_symbol or "").strip().upper()
    a_or_d = (records.get("A_or_D") or "").strip().upper()

    if sym != exp:
        return {}

    if a_or_d != "A":
        return {}

    return records
