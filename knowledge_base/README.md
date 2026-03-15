# Production Knowledge Base

Extracted from 4 health plan implementations (~47,000 lines of production code).
Source: `/Users/nizam/Documents/Work/LLM_Learning/`

## Purpose
Training data for Qwen2.5-Coder:7B to generate:
1. Python data transformation functions
2. BigQuery validation SQL
3. NPPES validation logic
4. Business rule implementations

## Files
- `transforms_catalog.json` — Every Python transform pattern with code
- `bq_validations_catalog.json` — Every BQ validation SQL pattern
- `nppes_patterns.json` — NPPES BQ table + API validation logic
- `business_rules.json` — Reference maps, normalization rules, detection logic
- `reference_data.json` — States, degrees, facility types, NUCC codes, null equivalents
- `hp1_humana_extraction.md` — Raw extraction from Health Plan 1
- `hp2_advanced_extraction.md` — Raw extraction from Health Plan 2
- `hp3_evry_hp4_medstar_extraction.md` — Raw extraction from Health Plans 3+4

## Version
- Extracted: 2026-03-12
- Source code: Health Plans 1-4 in LLM_Learning folder
