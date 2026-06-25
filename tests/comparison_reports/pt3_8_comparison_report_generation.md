# Cross-Provider Comparison Report

- Generated at: 2026-04-14T12:17:38.334011Z

## Provider Summary

| Provider | Total | OK | Success Ratio | Mean Parse Time (ms) | Quality Pass Rate |
|---|---:|---:|---:|---:|---:|
| deepdoc | 4 | 4 | 1.000 | 12659.62 | 1.000 |
| docling | 4 | 4 | 1.000 | 63611.79 | 1.000 |
| internal | 4 | 4 | 1.000 | 19.39 | 0.250 |
| marker_mcp | 4 | 4 | 1.000 | 7228.23 | 0.000 |
| mineru | 4 | 4 | 1.000 | 356353.83 | 0.500 |
| transformers | 4 | 4 | 1.000 | 15796.60 | 1.000 |

## Cases

| Provider | Document ID | Category | Status | Mode | Text Chars | Tables | Images | Parse Time (ms) | Reason |
|---|---|---|---|---|---:|---:|---:|---:|---|
| internal | bosib_large_pdf | large_mixed_pdf | ok | sync | 11446450 | 0 | 0 | 39.29 |  |
| marker_mcp | bosib_large_pdf | large_mixed_pdf | ok | sync_fallback | 205 | 0 | 2 | 6516.81 |  |
| mineru | bosib_large_pdf | large_mixed_pdf | ok | sync | 189845 | 0 | 0 | 111777.51 |  |
| deepdoc | bosib_large_pdf | large_mixed_pdf | ok | sync | 216713 | 0 | 0 | 2696.95 |  |
| docling | bosib_large_pdf | large_mixed_pdf | ok | sync | 212994 | 0 | 0 | 53339.44 |  |
| transformers | bosib_large_pdf | large_mixed_pdf | ok | sync | 216713 | 0 | 0 | 5191.29 |  |
| internal | celex_32016r0679 | regulatory_text | ok | sync | 981596 | 0 | 0 | 5.10 |  |
| marker_mcp | celex_32016r0679 | regulatory_text | ok | sync | 2904 | 0 | 0 | 8320.10 |  |
| mineru | celex_32016r0679 | regulatory_text | ok | sync | 347445 | 0 | 0 | 139517.74 |  |
| deepdoc | celex_32016r0679 | regulatory_text | ok | sync | 378319 | 0 | 0 | 3244.38 |  |
| docling | celex_32016r0679 | regulatory_text | ok | sync | 395673 | 0 | 0 | 30867.17 |  |
| transformers | celex_32016r0679 | regulatory_text | ok | sync | 378319 | 0 | 0 | 5250.16 |  |
| internal | ibrd_financial_statements_2025 | financial_table_heavy | ok | sync | 2957805 | 0 | 0 | 10.59 |  |
| marker_mcp | ibrd_financial_statements_2025 | financial_table_heavy | ok | sync | 351 | 0 | 1 | 6873.58 |  |
| mineru | ibrd_financial_statements_2025 | financial_table_heavy | ok | sync | 327214 | 0 | 0 | 229641.32 |  |
| deepdoc | ibrd_financial_statements_2025 | financial_table_heavy | ok | sync | 410573 | 0 | 0 | 9824.98 |  |
| docling | ibrd_financial_statements_2025 | financial_table_heavy | ok | sync | 411018 | 0 | 0 | 34791.33 |  |
| transformers | ibrd_financial_statements_2025 | financial_table_heavy | ok | sync | 410573 | 0 | 0 | 12171.28 |  |
| internal | nist_sp_800_53r5 | long_technical_text | ok | sync | 6070348 | 0 | 0 | 22.60 |  |
| marker_mcp | nist_sp_800_53r5 | long_technical_text | ok | sync | 350 | 0 | 1 | 7202.45 |  |
| mineru | nist_sp_800_53r5 | long_technical_text | ok | sync | 1302229 | 0 | 0 | 944478.74 |  |
| deepdoc | nist_sp_800_53r5 | long_technical_text | ok | sync | 1658149 | 0 | 0 | 34872.15 |  |
| docling | nist_sp_800_53r5 | long_technical_text | ok | sync | 1603602 | 0 | 0 | 135449.21 |  |
| transformers | nist_sp_800_53r5 | long_technical_text | ok | sync | 1658149 | 0 | 0 | 40573.67 |  |
