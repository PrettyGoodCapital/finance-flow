import re

__all__ = (
    "RIC_EXCHANGE_SUFFIXES",
    "derive_bloomberg_ticker",
    "derive_ric",
    "share_class",
)

# Primary-listing MIC to the RIC exchange suffix of US equities.
RIC_EXCHANGE_SUFFIXES = {
    "XNYS": ".N",
    "XNAS": ".O",
    "XASE": ".A",
    "ARCX": ".P",
    "BATS": ".Z",
}

_PREFERRED = re.compile(r"^(?P<root>[A-Z]+)p(?P<series>[A-Z]?)$")
# Suffixes after a dot in a listing ticker, mapped to RIC instrument codes.
_RIC_SUFFIXES = {"WS": "_t", "U": "_u", "RT": "_r", "W": "_t"}
_CLASS_NAME = re.compile(r"\bClass ([A-Z])\b", re.IGNORECASE)


def derive_ric(ticker: str | None, mic: str | None) -> str | None:
    """Build the conventional US equity RIC for a listing ticker on its primary exchange.

    Class shares take a lowercase class letter (``BRK.B`` on NYSE is ``BRKb.N``), preferreds take ``_p`` and the
    series letter (``ABRpD`` is ``ABR_pd.N``), and warrants, units, and rights take ``_t``, ``_u``, and ``_r``. Returns
    None for exchanges without a known suffix. The result follows the published convention, not LSEG's own symbology,
    so unusual instruments can differ.
    """
    suffix = RIC_EXCHANGE_SUFFIXES.get(mic or "")
    if not ticker or suffix is None:
        return None
    preferred = _PREFERRED.match(ticker)
    if preferred:
        return f"{preferred['root']}_p{preferred['series'].lower()}{suffix}"
    if "." in ticker:
        root, _, rest = ticker.partition(".")
        parts = rest.split(".")
        code = _RIC_SUFFIXES.get(parts[0])
        if code is not None:
            return f"{root}{code}{''.join(part.lower() for part in parts[1:])}{suffix}"
        if len(rest) == 1:
            return f"{root}{rest.lower()}{suffix}"
        return None
    return f"{ticker}{suffix}"


def derive_bloomberg_ticker(ticker: str | None, exchange_code: str = "US", market_sector: str = "Equity") -> str | None:
    """Build a Bloomberg composite ticker such as ``BRK/B US Equity`` from a listing ticker.

    Class separators become ``/`` and preferreds take ``/P`` and the series letter. Prefer the ticker OpenFIGI reports
    for the security's FIGI where it is known; this covers dates before a ticker change.
    """
    if not ticker:
        return None
    preferred = _PREFERRED.match(ticker)
    base = f"{preferred['root']}/P{preferred['series']}" if preferred else ticker.replace(".", "/")
    return f"{base} {exchange_code} {market_sector}"


def share_class(ticker: str | None, name: str | None) -> str | None:
    """The share class letter from a listing's name (``... Class A Common Stock``) or its ticker (``BRK.B``)."""
    match = _CLASS_NAME.search(name or "")
    if match:
        return match.group(1).upper()
    if ticker and "." in ticker:
        rest = ticker.partition(".")[2]
        if len(rest) == 1 and rest.isalpha():
            return rest.upper()
    return None
