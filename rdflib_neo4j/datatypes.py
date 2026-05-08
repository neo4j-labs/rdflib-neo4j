import datetime
from decimal import Decimal
from typing import Any, Optional

from rdflib import Literal, XSD

try:
    from neo4j.time import Date, DateTime, Duration
except ImportError:
    Date = DateTime = Duration = None


def coerce_literal(
    literal: Literal,
    keep_lang_tag: bool = False,
    keep_custom_data_types: bool = False,
    language_filter: Optional[str] = None,
) -> Any:
    """Convert an rdflib Literal to the appropriate Python/Neo4j type.

    Returns None if the literal should be skipped (language_filter mismatch).
    """

    # Language filter: skip literals whose lang tag doesn't match
    if language_filter and literal.language and literal.language.lower() != language_filter.lower():
        return None  # signal to caller to skip this literal

    # Language-tagged literals
    if literal.language:
        if keep_lang_tag:
            return f"{str(literal)}@{literal.language}"
        return str(literal)

    # Typed literals
    datatype = literal.datatype

    if datatype in (
        XSD.integer,
        XSD.long,
        XSD.int,
        XSD.short,
        XSD.byte,
        XSD.nonNegativeInteger,
        XSD.positiveInteger,
        XSD.nonPositiveInteger,
        XSD.negativeInteger,
        XSD.unsignedLong,
        XSD.unsignedInt,
        XSD.unsignedShort,
        XSD.unsignedByte,
    ):
        return int(literal)

    if datatype in (XSD.float, XSD.double):
        return float(literal)

    if datatype == XSD.decimal:
        v = literal.toPython()
        return float(v) if isinstance(v, Decimal) else float(v)

    if datatype == XSD.boolean:
        return literal.toPython()  # rdflib returns bool

    if datatype in (XSD.string, XSD.normalizedString):
        return str(literal)

    if datatype == XSD.date:
        if Date is not None:
            d = literal.toPython()  # returns datetime.date
            return Date(d.year, d.month, d.day)
        return str(literal)

    if datatype in (XSD.dateTime, XSD.dateTimeStamp):
        if DateTime is not None:
            dt = literal.toPython()  # returns datetime.datetime
            if dt.tzinfo is not None:
                return DateTime(
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.hour,
                    dt.minute,
                    dt.second,
                    dt.microsecond * 1000,
                    dt.utcoffset().total_seconds(),
                )
            return DateTime(
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second,
                dt.microsecond * 1000,
            )
        return str(literal)

    if datatype in (XSD.duration, XSD.dayTimeDuration, XSD.yearMonthDuration):
        if Duration is not None:
            td = literal.toPython()  # returns datetime.timedelta
            if isinstance(td, datetime.timedelta):
                return Duration(seconds=int(td.total_seconds()))
        return str(literal)

    # Unknown/custom datatype
    if datatype and keep_custom_data_types:
        return f"{str(literal)}^^{str(datatype)}"

    # Fall back: use toPython, convert Decimal to float
    v = literal.toPython()
    return float(v) if isinstance(v, Decimal) else v
