# Expert Report Fixed Layout Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reshape expert analysis reports into a fixed business-friendly layout with an executive summary, up to three key insights, and up to three action items while preserving detailed expert evidence behind a folded detail view.

**Architecture:** Keep backward-compatible report fields (`summary`, `insights`, `recommendations`) but add normalized expert-facing sections (`executive_summary`, `top_insights`, `action_items`) in the backend report payload. Update the frontend expert views to render those normalized sections first and move evidence, conversation, reasoning, and limitations into a detail collapse. Old reports continue to render through frontend/backend fallback normalization.

**Tech Stack:** Python/FastAPI backend, Vue 3 + Ant Design Vue frontend, pytest, node:test.

---

### Task 1: Lock backend report contract with failing tests

**Files:**
- Modify: `doris-api/tests/test_analyst_agent.py`

**Step 1: Write the failing test**

Add coverage that an expert report returns:
- `executive_summary`
- `top_insights` capped at 3
- `action_items` capped at 3
- backward-compatible `summary`, `insights`, `recommendations`

**Step 2: Run test to verify it fails**

Run: `docker run --rm -v /Users/apple/S-Matrix/doris-api:/app -w /app s-matrix-smatrix-api python -m pytest tests/test_analyst_agent.py -q`

Expected: FAIL because the new fields do not exist yet.

**Step 3: Write minimal implementation**

Update `doris-api/analyst_agent.py` to derive fixed expert sections from strategist output and reuse them in the report payload.

**Step 4: Run test to verify it passes**

Run: `docker run --rm -v /Users/apple/S-Matrix/doris-api:/app -w /app s-matrix-smatrix-api python -m pytest tests/test_analyst_agent.py -q`

Expected: PASS for the new contract tests.

### Task 2: Lock frontend fixed layout with failing tests

**Files:**
- Modify: `doris-frontend/tests/analysis-ui.test.ts`

**Step 1: Write the failing test**

Add assertions that the component contains:
- `经营摘要`
- `关键洞察`
- `动作建议`
- folded `详细分析`
- normalized helper names for expert section selection

**Step 2: Run test to verify it fails**

Run: `npm test -- analysis-ui.test.ts`

Expected: FAIL because the fixed layout markup/helpers do not exist yet.

**Step 3: Write minimal implementation**

Update `doris-frontend/src/components/DataAnalysis.vue` and `doris-frontend/src/api/doris.ts` to use the fixed expert section fields with fallback logic for old reports.

**Step 4: Run test to verify it passes**

Run: `npm test -- analysis-ui.test.ts`

Expected: PASS.

### Task 3: Implement backend expert section normalization

**Files:**
- Modify: `doris-api/analyst_agent.py`
- Test: `doris-api/tests/test_analyst_agent.py`

**Step 1: Add normalization helpers**

Implement helpers that derive:
- one concise executive summary
- up to three key insights
- up to three action items

**Step 2: Keep compatibility**

Ensure old payload readers can still use:
- `summary`
- `insights`
- `recommendations`

while new consumers can use:
- `executive_summary`
- `top_insights`
- `action_items`

**Step 3: Verify**

Run: `docker run --rm -v /Users/apple/S-Matrix/doris-api:/app -w /app s-matrix-smatrix-api python -m pytest tests/test_analyst_agent.py tests/test_analysis_api.py -q`

Expected: PASS.

### Task 4: Implement frontend fixed expert main view

**Files:**
- Modify: `doris-frontend/src/components/DataAnalysis.vue`
- Modify: `doris-frontend/src/api/doris.ts`
- Test: `doris-frontend/tests/analysis-ui.test.ts`

**Step 1: Render normalized expert sections**

Show for expert reports:
- `经营摘要`
- `关键洞察`
- `动作建议`

**Step 2: Fold detailed analysis**

Move:
- evidence chains
- root causes
- conversation timeline
- reasoning trace
- limitations

into a `详细分析` collapse.

**Step 3: Keep non-expert behavior stable**

Retain the existing quick/standard/deep report presentation.

**Step 4: Verify**

Run: `npm test -- analysis-ui.test.ts`

Expected: PASS.

### Task 5: End-to-end verification

**Files:**
- Modify: `doris-api/analyst_agent.py`
- Modify: `doris-api/tests/test_analyst_agent.py`
- Modify: `doris-frontend/src/components/DataAnalysis.vue`
- Modify: `doris-frontend/src/api/doris.ts`
- Modify: `doris-frontend/tests/analysis-ui.test.ts`

**Step 1: Run backend focused suite**

Run: `docker run --rm -v /Users/apple/S-Matrix/doris-api:/app -w /app s-matrix-smatrix-api python -m pytest tests/test_analyst_agent.py tests/test_analysis_api.py tests/test_analysis_scheduler.py tests/test_analysis_dispatcher.py tests/test_docker_startup_resilience.py -q`

Expected: PASS.

**Step 2: Run frontend tests**

Run: `npm test`

Expected: PASS.

**Step 3: Run frontend build**

Run: `npm run build`

Expected: PASS, ignoring any pre-existing chunk size warning.
