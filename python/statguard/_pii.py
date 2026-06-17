"""
PII detection for StatGuard.

Scans Polars DataFrames for columns that appear to contain personally
identifiable information, using two complementary methods:

1. Column-name heuristics — fast, zero data access, catches obvious names.
2. Value pattern matching — regex scan on a sample of string column values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

try:
    import polars as pl
    _POLARS = True
except ImportError:
    _POLARS = False

# ── PII patterns (value-level detection) ─────────────────────────────────────

_PATTERNS: Dict[str, re.Pattern] = {
    "email": re.compile(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.ASCII
    ),
    "phone": re.compile(
        r"(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", re.ASCII
    ),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b", re.ASCII),
    "credit_card": re.compile(
        r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|"
        r"3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b",
        re.ASCII,
    ),
    "ip_address": re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}"
        r"(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b",
        re.ASCII,
    ),
    "date_of_birth": re.compile(
        r"\b(?:0[1-9]|1[0-2])[/\-](?:0[1-9]|[12]\d|3[01])[/\-](?:19|20)\d{2}\b"
    ),
    "passport": re.compile(r"\b[A-Z]{1,2}\d{6,9}\b"),
    "iban": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b"),
}

# ── Column-name heuristics ────────────────────────────────────────────────────

_NAME_HINTS: Dict[str, List[str]] = {
    "email":        ["email", "e_mail", "email_address", "mail"],
    "phone":        ["phone", "mobile", "cell", "telephone", "tel", "phone_number", "contact_number"],
    "ssn":          ["ssn", "social_security", "social_security_number", "tax_id", "national_id"],
    "credit_card":  ["credit_card", "card_number", "cc_number", "card_no", "payment_card"],
    "ip_address":   ["ip", "ip_address", "ipv4", "client_ip", "user_ip", "remote_addr"],
    "name":         ["first_name", "last_name", "full_name", "surname", "given_name", "family_name"],
    "date_of_birth":["dob", "date_of_birth", "birthdate", "birth_date", "birthday"],
    "address":      ["address", "street", "city", "zip", "postal_code", "postcode", "addr"],
    "gender":       ["gender", "sex"],
    "nationality":  ["nationality", "citizenship", "country_of_birth"],
    "passport":     ["passport", "passport_number", "travel_document"],
    "iban":         ["iban", "bank_account", "account_number", "sort_code"],
}


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class PiiFinding:
    """A column that appears to contain PII."""
    column: str
    pii_type: str
    detection_method: str   # "name_heuristic" or "pattern_match"
    risk: str               # "high", "medium", "low"
    sample_matches: int = 0
    sample_size: int = 0

    def __str__(self) -> str:
        method = "name" if self.detection_method == "name_heuristic" else "pattern"
        detail = (
            f"{self.sample_matches}/{self.sample_size} values matched"
            if self.detection_method == "pattern_match"
            else "column name suggests PII"
        )
        return (
            f"[{self.risk.upper()}] {self.column!r} — {self.pii_type} "
            f"({method}: {detail})"
        )


# ── Main API ──────────────────────────────────────────────────────────────────

def scan_pii(
    df: "pl.DataFrame",
    sample_rows: int = 2_000,
    pattern_threshold: float = 0.05,
) -> List[PiiFinding]:
    """
    Scan a Polars DataFrame for columns that appear to contain PII.

    Args:
        df:                 DataFrame to scan.
        sample_rows:        How many rows to sample for pattern matching.
        pattern_threshold:  Fraction of non-null values that must match a
                            pattern before it is flagged (default 5%).

    Returns:
        List of PiiFindings, one per suspicious column. A column may appear
        more than once if multiple PII types are detected.
    """
    if not _POLARS:
        raise ImportError("polars is required for PII scanning")

    findings: List[PiiFinding] = []
    seen: set = set()
    sample = df.head(sample_rows)

    for col_name in df.columns:
        col_lower = col_name.lower().replace(" ", "_").replace("-", "_")

        # 1. Column-name heuristic
        for pii_type, hints in _NAME_HINTS.items():
            if any(h in col_lower for h in hints):
                key = (col_name, pii_type)
                if key not in seen:
                    seen.add(key)
                    findings.append(PiiFinding(
                        column=col_name,
                        pii_type=pii_type,
                        detection_method="name_heuristic",
                        risk="medium",
                    ))
                break

        # 2. Value pattern matching (string columns only)
        col = sample[col_name]
        if col.dtype != pl.String:
            continue

        non_null = col.drop_nulls()
        total = len(non_null)
        if total == 0:
            continue

        for pii_type, pattern in _PATTERNS.items():
            matches = sum(1 for v in non_null.to_list() if pattern.search(v))
            frac = matches / total
            if frac >= pattern_threshold:
                key = (col_name, pii_type)
                if key not in seen:
                    seen.add(key)
                    risk = "high" if frac >= 0.5 else "medium"
                    findings.append(PiiFinding(
                        column=col_name,
                        pii_type=pii_type,
                        detection_method="pattern_match",
                        risk=risk,
                        sample_matches=matches,
                        sample_size=total,
                    ))

    return findings


def pii_report(findings: List[PiiFinding]) -> str:
    """Format a list of PiiFindings as a human-readable report."""
    if not findings:
        return "No PII detected."
    lines = [f"PII scan — {len(findings)} finding(s):", ""]
    for f in findings:
        lines.append(f"  {f}")
    return "\n".join(lines)
