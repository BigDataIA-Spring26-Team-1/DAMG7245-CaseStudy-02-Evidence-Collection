# Evidence Collection Report

Generated on: 2026-02-06
Data snapshot source: `pe-org-air-platform/data/raw`, `pe-org-air-platform/data/processed`, `pe-org-air-platform/data/signals`

## 1) Summary Statistics for Each Company

| Company | Raw Docs | Processed Docs | Chunk Files | Jobs Items | News Items | Patents Items | Tech Keywords (>0) |
|---|---:|---:|---:|---:|---:|---:|---:|
| ADP | 3 | 3 | 3 | 13 | 12 | 17 | 1 |
| CAT | 3 | 3 | 3 | 15 | 16 | 15 | 0 |
| DE | 3 | 3 | 3 | 15 | 15 | 14 | 1 |
| GS | 3 | 3 | 3 | 15 | 15 | 13 | 0 |
| HCA | 3 | 3 | 3 | 14 | 15 | 12 | 0 |
| JPM | 4 | 4 | 4 | 17 | 15 | 15 | 0 |
| PAYX | 3 | 3 | 3 | 14 | 16 | 10 | 0 |
| TGT | 4 | 4 | 4 | 18 | 14 | 14 | 0 |
| UNH | 3 | 3 | 3 | 15 | 12 | 12 | 0 |
| WMT | 3 | 3 | 3 | 14 | 14 | 14 | 0 |

Portfolio totals:
- Companies: 10
- Raw documents: 32
- Processed documents: 32
- Chunk files: 32

## 2) Document Counts by Filing Type

### Portfolio-level counts (processed)

| Filing Type | Count |
|---|---:|
| 10-K | 10 |
| 10-Q | 10 |
| 8-K | 12 |
| DEF-14A | 0 |

### Company-level counts (processed)

| Company | 10-K | 10-Q | 8-K | DEF-14A |
|---|---:|---:|---:|---:|
| ADP | 1 | 1 | 1 | 0 |
| CAT | 1 | 1 | 1 | 0 |
| DE | 1 | 1 | 1 | 0 |
| GS | 1 | 1 | 1 | 0 |
| HCA | 1 | 1 | 1 | 0 |
| JPM | 1 | 1 | 2 | 0 |
| PAYX | 1 | 1 | 1 | 0 |
| TGT | 1 | 1 | 2 | 0 |
| UNH | 1 | 1 | 1 | 0 |
| WMT | 1 | 1 | 1 | 0 |

## 3) Signal Scores by Category

Scoring method (0-100) used for this report:
- Jobs score = `min(100, jobs_item_count / 50 * 100)`
- News score = `min(100, news_item_count / 40 * 100)`
- Patents score = `min(100, patents_item_count / 20 * 100)`
- Tech score = `min(100, unique_tech_keywords / 10 * 100)`

| Company | Jobs Score | News Score | Patents Score | Tech Score |
|---|---:|---:|---:|---:|
| ADP | 26.0 | 30.0 | 85.0 | 10.0 |
| CAT | 30.0 | 40.0 | 75.0 | 0.0 |
| DE | 30.0 | 37.5 | 70.0 | 10.0 |
| GS | 30.0 | 37.5 | 65.0 | 0.0 |
| HCA | 28.0 | 37.5 | 60.0 | 0.0 |
| JPM | 34.0 | 37.5 | 75.0 | 0.0 |
| PAYX | 28.0 | 40.0 | 50.0 | 0.0 |
| TGT | 36.0 | 35.0 | 70.0 | 0.0 |
| UNH | 30.0 | 30.0 | 60.0 | 0.0 |
| WMT | 28.0 | 35.0 | 70.0 | 0.0 |

Average score by category:
- Jobs: 30.0
- News: 36.0
- Patents: 68.0
- Tech: 2.0

## 4) Data Quality Issues Encountered

Integrity checks completed:
- No raw vs processed count mismatches found per company.
- No processed vs chunk file count mismatches found per company.
- No missing or empty signal artifact files found for included companies.

Issues/gaps observed:
- DEF-14A filings are documented as a target evidence source in `README.md:62`, but not collected in the evidence script (`pe-org-air-platform/scripts/collect_evidence.py:31` sets `TARGET_FORMS = ["10-K", "10-Q", "8-K"]`).
- Tech signal coverage is sparse: 8 of 10 companies have zero detected tech keywords in `tech_counts.json`, which can understate technical readiness.
- Signal scoring logic is not fully standardized across scripts: `pe-org-air-platform/scripts/compute_signal_scores.py` and `pe-org-air-platform/scripts/compute_summary_signals.py` use different normalization thresholds/weighting, which can produce non-comparable score views.
