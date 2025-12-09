import os
import math
import pytest

from sheep_shear.yarn.collect_wool import CollectWool

# run tests with 
# RUN_SEC_INTEGRATION=1 python -m pytest -q

CASES = [
    (
        "https://www.sec.gov/Archives/edgar/data/1652044/000119312525272562/xslF345X05/ownership.xml",
        {
            "security": "Class C Capital Stock",
            "total_amount": 32500.0,
            "A_or_D": "D",
            "avg_price": 283.4763107692308,
            "issuer_name": "Alphabet Inc.",
            "issuer_symbol": "GOOGL",
        },
    ),
    (
        "https://www.sec.gov/Archives/edgar/data/1652044/000141588925021025/xslF345X05/form4-08052025_080848.xml",
        {
            "security": "Common Stock",
            "total_amount": 1500000,
            "A_or_D": "A",
            "avg_price": 3.3,
            "total_amount_owned": 3262440,
            "issuer_name": "Prime Medicine, Inc.",
            "issuer_symbol": "PRME",
        },
    ),
    (
        "https://www.sec.gov/Archives/edgar/data/1721789/000141588925013728/xslF345X05/form4-05192025_110520.xml",
        {
            "security": "Class A Common Stock",
            "total_amount": 47980156.0,
            "A_or_D": "A",
            "avg_price": None,
            "total_amount_owned": 147490961.0,
            "issuer_name": "Starco Brands, Inc.",
            "issuer_symbol": "STCB",
        },
    ),
    (
        "https://www.sec.gov/Archives/edgar/data/1134655/000123191925000574/xslF345X05/form4-12042025_111237.xml",
        {
            "security": "Common Stock",
            "total_amount": 199732,
            "A_or_D": "D",
            "avg_price": 0.9176064927,
            "total_amount_owned": 4924566,
            "issuer_name": "Werewolf Therapeutics, Inc.",
            "issuer_symbol": "HOWL",
        },
    ),
    (
        "https://www.sec.gov/Archives/edgar/data/718877/000095017024020392/xslF345X05/ownership.xml",
        {
            "security": "Class A Common Stock",
            "total_amount": 1000000,
            "A_or_D": "D",
            "avg_price": 1.95,
            "total_amount_owned": 11677398,
            "issuer_name": "PLAYSTUDIOS, Inc.",
            "issuer_symbol": "MYPS",
        },
    ),
    (
        "https://www.sec.gov/Archives/edgar/data/1321655/000132165525000044/xslF345X05/wk-form4_1743120118.xml",
        {
            "security": "Common Stock",
            "total_amount": 244011,
            "A_or_D": "A",
            "avg_price": 3.88,
            "total_amount_owned": 3421007,
            "issuer_name": "SURF AIR MOBILITY INC.",
            "issuer_symbol": "SRFM",
        }
    ),
    (
        "https://www.sec.gov/Archives/edgar/data/1321655/000132165524000211/xslF345X05/wk-form4_1730943130.xml",
        {
            "security": "Class A Common Stock",
            "total_amount": 958356,
            "A_or_D": "D",
            "avg_price": 0.11931585632061573,
            "total_amount_owned": 3831977,
            "issuer_name": "MSP Recovery, Inc.",
            "issuer_symbol": "LIFW",
        }
    )
]


def _is_nan(x) -> bool:
    return isinstance(x, float) and math.isnan(x)


def _record_matches_expected(rec: dict, expected: dict) -> bool:
    """
    True if rec contains at least the keys in expected and the values match.
    - floats compared with pytest.approx
    - expected None matches None OR NaN
    """
    for k, v in expected.items():
        if k not in rec:
            return False
        rv = rec[k]

        if v is None:
            if rv is None or _is_nan(rv):
                continue
            return False

        if isinstance(v, float):
            try:
                return_val = float(rv)
            except Exception:
                return False
            if return_val != pytest.approx(v, rel=1e-6, abs=1e-9):
                return False
            continue

        if rv != v:
            return False

    return True


@pytest.mark.parametrize("primary_doc_url, expected", CASES)
def test_collect_wool_grab_matches_expected(primary_doc_url, expected):
    # Avoid accidental network calls in CI unless explicitly enabled.
    if not os.getenv("RUN_SEC_INTEGRATION"):
        pytest.skip("Set RUN_SEC_INTEGRATION=1 to run live SEC integration tests.")

    records = CollectWool(primary_doc_url).grab()

    assert isinstance(records, list), f"Expected list of dicts, got: {type(records)}"
    assert records, "Expected at least one record from grab()"

    if not any(_record_matches_expected(r, expected) for r in records):
        pytest.fail(f"No returned record matched expected.\nExpected: {expected}\nGot: {records}")
