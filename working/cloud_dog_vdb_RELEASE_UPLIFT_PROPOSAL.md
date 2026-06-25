# platform-vdb 0.4.0 - PROPOSED Uplift Plan

**Package:** `cloud_dog_vdb`  
**Status:** PROPOSED  
**Date:** 2026-02-28

---

## 1. Proposed Release Intent

Extend ingestion/indexing to support configurable parser ecosystems (internal and external), OCR strategies, and richer chunk/table controls, while preserving complete compatibility for existing adopters.

---

## 2. Feature Scope (Proposed)

- Multi-provider parser chain: MinerU, marker-mcp, DeepDoc, Docling, baseline internal parser.
- OCR framework: local, external service, and LLM OCR providers.
- Table handling policies: text/markdown/html/json/dual plus table chunking policies.
- IR-first ingestion path with provenance and quality gates.
- Compatibility bridge preserving legacy converter path and defaults.

---

## 3. Backward Compatibility Statement

0.4.0 is proposed as a non-breaking release:
- no required API changes for existing clients,
- no required config changes,
- old defaults preserved,
- old deterministic identity behaviour preserved in default mode.

Any behaviour changes are opt-in via new ingestion config options.

---

## 4. Documentation Enhancement Plan

The final release documentation should be updated as follows:

1. `README.md`
- add parser/OCR/table feature overview,
- add compatibility promise and migration quick-start.

2. `REQUIREMENTS.md`
- merge FR2/BC requirements from proposal into normative requirements.

3. `ARCHITECTURE.md`
- add parser/OCR/IR modules, flows, and compatibility bridge.

4. `TESTS.md`
- add UT2/ST2/PT1/IT2/QT2/CT1 suites and release gates.

5. `docs/reference-designs/vdb-reference.md` (repo level)
- add parser chain and OCR examples,
- add table policy examples,
- add provider capability matrix references.

6. New recommended docs under package root
- `PARSER-PROVIDERS.md` (capabilities, config, failure modes),
- `OCR-POLICIES.md` (modes, heuristics, cost controls),
- `MIGRATION-0.3-to-0.4.md` (non-breaking migration notes).

---

## 5. Risks and Mitigations

- Risk: external parser variability.
  - Mitigation: provider capability declarations, quality gates, deterministic fallback.

- Risk: security exposure from external commands/endpoints.
  - Mitigation: allowlists, sandboxing, timeout, redaction, QT2 security suites.

- Risk: regression in legacy ingestion behaviour.
  - Mitigation: CT1 compatibility suite and default-path parity tests.

- Risk: dependency bloat.
  - Mitigation: optional extras only; lazy provider activation.

---

## 6. Implementation Phasing (Proposed)

1. Phase A: Introduce parser/OCR interfaces, registry, IR model, compatibility bridge.
2. Phase B: Add provider adapters and table policy engine.
3. Phase C: Add full test suites and compatibility gates.
4. Phase D: Promote docs and release artefacts.

---

## 7. Approval Checklist

- [ ] Requirements uplift accepted.
- [ ] Architecture uplift accepted.
- [ ] Test uplift accepted.
- [ ] Compatibility contract accepted.
- [ ] Security controls accepted.
- [ ] Documentation update scope accepted.
