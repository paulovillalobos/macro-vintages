"""The series this engine tracks.

Keys were checked against the live APIs on 2026-10-20 (``scripts/probe_sources.py``).
Two choices here come from that probing rather than from documentation:

* Euro-area aggregates use ``EA21`` / ``EA``: after Bulgaria joined the euro on
  2026-01-01, ``une_rt_m`` dropped ``EA20`` and a ``geo=EA20`` request returns
  HTTP 200 with zero observations instead of an error.
* HICP comes from Eurostat's ECOICOP ver. 2 dataset ``prc_hicp_minr``. The older
  ``prc_hicp_manr`` and the ECB ``ICP`` series stopped at 2025-12.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from macro_vintages.periods import Frequency


class Source(StrEnum):
    ECB = "ecb"
    EUROSTAT = "eurostat"


@dataclass(frozen=True, slots=True)
class SeriesSpec:
    code: str
    source: Source
    source_key: str
    title: str
    unit: str
    frequency: Frequency
    interval_s: int


_HOUR = 3600

CATALOGUE: tuple[SeriesSpec, ...] = (
    SeriesSpec(
        "ecb.exr.usd_eur",
        Source.ECB,
        "EXR/D.USD.EUR.SP00.A",
        "US dollar per euro, ECB reference rate",
        "USD",
        Frequency.DAILY,
        _HOUR,
    ),
    SeriesSpec(
        "ecb.exr.gbp_eur",
        Source.ECB,
        "EXR/D.GBP.EUR.SP00.A",
        "Pound sterling per euro, ECB reference rate",
        "GBP",
        Frequency.DAILY,
        _HOUR,
    ),
    SeriesSpec(
        "ecb.exr.jpy_eur",
        Source.ECB,
        "EXR/D.JPY.EUR.SP00.A",
        "Japanese yen per euro, ECB reference rate",
        "JPY",
        Frequency.DAILY,
        _HOUR,
    ),
    SeriesSpec(
        "ecb.exr.chf_eur",
        Source.ECB,
        "EXR/D.CHF.EUR.SP00.A",
        "Swiss franc per euro, ECB reference rate",
        "CHF",
        Frequency.DAILY,
        _HOUR,
    ),
    SeriesSpec(
        "ecb.rate.dfr",
        Source.ECB,
        "FM/D.U2.EUR.4F.KR.DFR.LEV",
        "ECB deposit facility rate",
        "percent",
        Frequency.DAILY,
        6 * _HOUR,
    ),
    SeriesSpec(
        "ecb.rate.mrr",
        Source.ECB,
        "FM/D.U2.EUR.4F.KR.MRR_FR.LEV",
        "ECB main refinancing operations fixed rate",
        "percent",
        Frequency.DAILY,
        6 * _HOUR,
    ),
    SeriesSpec(
        "ecb.rate.mlf",
        Source.ECB,
        "FM/D.U2.EUR.4F.KR.MLFR.LEV",
        "ECB marginal lending facility rate",
        "percent",
        Frequency.DAILY,
        6 * _HOUR,
    ),
    SeriesSpec(
        "estat.hicp.ea.annual_rate",
        Source.EUROSTAT,
        "prc_hicp_minr?coicop18=TOTAL&geo=EA&unit=RCH_A",
        "Euro area HICP, all items, annual rate of change",
        "percent",
        Frequency.MONTHLY,
        3 * _HOUR,
    ),
    SeriesSpec(
        "estat.gdp.ea21.qoq",
        Source.EUROSTAT,
        "namq_10_gdp?geo=EA21&na_item=B1GQ&s_adj=SCA&unit=CLV_PCH_PRE",
        "Euro area (EA21) GDP, chain-linked volumes, % change on previous quarter, SCA",
        "percent",
        Frequency.QUARTERLY,
        3 * _HOUR,
    ),
    SeriesSpec(
        "estat.unemployment.ea21",
        Source.EUROSTAT,
        "une_rt_m?age=TOTAL&geo=EA21&s_adj=SA&sex=T&unit=PC_ACT",
        "Euro area (EA21) unemployment rate, seasonally adjusted",
        "percent of labour force",
        Frequency.MONTHLY,
        3 * _HOUR,
    ),
)
