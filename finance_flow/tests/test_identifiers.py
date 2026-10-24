import pytest

from finance_flow.identifiers import derive_bloomberg_ticker, derive_ric, share_class


@pytest.mark.parametrize(
    ("ticker", "mic", "ric"),
    [
        ("AAPL", "XNAS", "AAPL.O"),
        ("IBM", "XNYS", "IBM.N"),
        ("BRK.B", "XNYS", "BRKb.N"),
        ("ABRpD", "XNYS", "ABR_pd.N"),
        ("XYZp", "XNYS", "XYZ_p.N"),
        ("AAC.WS", "XNYS", "AAC_t.N"),
        ("AAC.U", "XNYS", "AAC_u.N"),
        ("SPY", "ARCX", "SPY.P"),
        ("IMO", "XASE", "IMO.A"),
        ("AAPL", "OTCM", None),
        (None, "XNAS", None),
        ("ODD.ABC", "XNYS", None),
    ],
)
def test_derive_ric(ticker, mic, ric):
    assert derive_ric(ticker, mic) == ric


def test_derive_bloomberg_ticker():
    assert derive_bloomberg_ticker("META") == "META US Equity"
    assert derive_bloomberg_ticker("BRK.B") == "BRK/B US Equity"
    assert derive_bloomberg_ticker("ABRpD") == "ABR/PD US Equity"
    assert derive_bloomberg_ticker(None) is None


def test_share_class():
    assert share_class("GOOGL", "Alphabet Inc. Class A Common Stock") == "A"
    assert share_class("BRK.B", "Berkshire Hathaway Inc.") == "B"
    assert share_class("AAPL", "Apple Inc.") is None
