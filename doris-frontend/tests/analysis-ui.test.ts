import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

test('analysis API supports expert depth and reasoning query params', () => {
  const apiSource = readFileSync(new URL('../src/api/doris.ts', import.meta.url), 'utf8');

  assert.match(apiSource, /depth:\s*'quick'\s*\|\s*'standard'\s*\|\s*'deep'\s*\|\s*'expert'/);
  assert.match(apiSource, /getReport:\s*\(id: string,\s*includeReasoning: boolean = false\)/);
  assert.match(apiSource, /latestReport:\s*\(tableName: string,\s*includeReasoning: boolean = false\)/);
  assert.match(apiSource, /include_reasoning: includeReasoning \? 'true' : undefined/);
});

test('analysis UI exposes expert mode and reasoning affordances', () => {
  const component = readFileSync(new URL('../src/components/DataAnalysis.vue', import.meta.url), 'utf8');

  assert.match(component, /Expert/);
  assert.match(component, /reasoning/i);
  assert.match(component, /conversation/i);
  assert.match(component, /2-5 分钟|2-5 minutes/i);
});

test('analysis UI renders fixed expert business layout', () => {
  const component = readFileSync(new URL('../src/components/DataAnalysis.vue', import.meta.url), 'utf8');

  assert.match(component, /经营摘要/);
  assert.match(component, /关键洞察/);
  assert.match(component, /动作建议/);
  assert.match(component, /详细分析/);
  assert.match(component, /expertExecutiveSummary/);
  assert.match(component, /expertTopInsights/);
  assert.match(component, /expertActionItems/);
});
