# Recorded responses

Real responses from the live APIs, captured with curl on 2026-10-20. Tests use
them so parsers are checked against what the sources actually send, not against
a hand-written idea of it. `scripts/probe_sources.py` re-runs the probes.

| File | Request |
|---|---|
| `ecb_exr_usd_last30.csv` | `EXR/D.USD.EUR.SP00.A?format=csvdata&detail=dataonly&lastNObservations=30` |
| `ecb_dfr_last5.csv` | `FM/D.U2.EUR.4F.KR.DFR.LEV?format=csvdata&detail=dataonly&lastNObservations=5` |
| `ecb_not_modified_404.json` | same USD key with `updatedAfter` after the last change: 404, not 304 |
| `ecb_unknown_key_404.json` | `EXR/D.XXX.EUR.SP00.A`: also 404, same body shape |
| `eurostat_gdp_ea21.json` | `namq_10_gdp?geo=EA21&na_item=B1GQ&s_adj=SCA&unit=CLV_PCH_PRE` (status `e` on some quarters) |
| `eurostat_hicp_ea_last6.json` | `prc_hicp_minr?coicop18=TOTAL&geo=EA&unit=RCH_A&lastTimePeriod=6` |
| `eurostat_une_ea20_empty.json` | `une_rt_m?...&geo=EA20`: 200 with a zero-sized `geo` dimension (EA20 dropped in 2026) |
| `eurostat_async_413.json` | `une_rt_m?geo=QQ`: 413 ASYNCHRONOUS_RESPONSE (minutes later the same URL returned 200 with an empty `geo`) |
| `eurostat_unknown_dataset_404.json` | `nonexistent_xyz`: 404 |
