/**
 * Extração retroativa das métricas de incidente (SLA) para Whiteboard Reports.
 * Não grava nas issues. Lê jira-config.json na raiz.
 *
 * Uso (raiz do repo):
 *   node scripts/export-sla-whiteboard.mjs
 */
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { computeSlaSnapshot, toWhiteboardRow } from './sla-engine.mjs';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const JQL = 'project = Mobilidade AND type = Incident AND created >= startOfYear(-2) ORDER BY created ASC';
const OUT_DIR = join(ROOT, 'saida', 'sla-whiteboard');
const OUT_FILE = join(OUT_DIR, 'incidentes-mobilidade.json');
const CONCURRENCY = 6;

const FIELDS = [
  'summary',
  'status',
  'created',
  'updated',
  'resolutiondate',
  'assignee',
  'reporter',
  'priority',
  'issuetype',
  'project',
  'labels',
  'components',
  'resolution',
  'customfield_10802',
  'customfield_14693',
  'customfield_14692',
  'customfield_14759',
  'customfield_15802',
  'customfield_15806',
  'customfield_15807',
  'customfield_15840',
].join(',');

function authHeader(cfg) {
  const token = Buffer.from(`${cfg.email}:${cfg.api_token}`).toString('base64');
  return `Basic ${token}`;
}

async function jiraJson(cfg, path, { method = 'GET', body } = {}) {
  const url = `${cfg.base_url.replace(/\/$/, '')}${path}`;
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url, {
      method,
      headers: {
        Accept: 'application/json',
        Authorization: authHeader(cfg),
        ...(body ? { 'Content-Type': 'application/json' } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (res.status === 429 || res.status >= 500) {
      if (attempt >= 6) throw new Error(`HTTP ${res.status} em ${path}`);
      const retryAfter = Number(res.headers.get('retry-after')) || 0;
      const backoff = Math.min(Math.max(retryAfter * 1000, 2 ** attempt * 500), 30000);
      await new Promise((r) => setTimeout(r, backoff));
      continue;
    }
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`HTTP ${res.status} ${path}: ${text.slice(0, 400)}`);
    }
    return res.json();
  }
}

async function searchAll(cfg) {
  const issues = [];
  let nextPageToken;
  do {
    const page = await jiraJson(cfg, '/rest/api/3/search/jql', {
      method: 'POST',
      body: {
        jql: JQL,
        maxResults: 100,
        fields: FIELDS.split(','),
        ...(nextPageToken ? { nextPageToken } : {}),
      },
    });
    issues.push(...(page.issues || []));
    nextPageToken = page.nextPageToken;
    process.stdout.write(`\rIssues listadas: ${issues.length}`);
  } while (nextPageToken);
  process.stdout.write('\n');
  return issues;
}

async function getChangelog(cfg, key) {
  const all = [];
  let startAt = 0;
  for (;;) {
    const page = await jiraJson(cfg, `/rest/api/3/issue/${key}/changelog?startAt=${startAt}&maxResults=100`);
    const values = page.values || [];
    all.push(...values);
    startAt += values.length;
    if (!values.length || page.isLast || startAt >= (page.total || 0)) break;
  }
  return all;
}

async function getComments(cfg, key) {
  const all = [];
  let startAt = 0;
  for (;;) {
    const page = await jiraJson(cfg, `/rest/api/3/issue/${key}/comment?startAt=${startAt}&maxResults=100`);
    const values = page.comments || [];
    all.push(...values);
    startAt += values.length;
    if (!values.length || startAt >= (page.total || 0)) break;
  }
  return all;
}

async function mapPool(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  async function worker() {
    for (;;) {
      const idx = i++;
      if (idx >= items.length) return;
      out[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}

const cfg = JSON.parse(await readFile(join(ROOT, 'jira-config.json'), 'utf8'));
console.log(`JQL: ${JQL}`);
const issues = await searchAll(cfg);
console.log(`Processando changelog + comentários (${CONCURRENCY} em paralelo)...`);

const t0 = Date.now();
let done = 0;
const rows = await mapPool(issues, CONCURRENCY, async (issue) => {
  const [changelog, comments] = await Promise.all([getChangelog(cfg, issue.key), getComments(cfg, issue.key)]);
  const snap = computeSlaSnapshot(issue, changelog, comments);
  done++;
  if (done % 50 === 0 || done === issues.length) {
    const rate = done / ((Date.now() - t0) / 60000);
    process.stdout.write(`\r  ${done}/${issues.length} (${rate.toFixed(0)}/min)   `);
  }
  return toWhiteboardRow(issue, snap);
});

await mkdir(OUT_DIR, { recursive: true });
const envelope = {
  generatedAt: new Date().toISOString(),
  fonte: 'exportacao-estatica-sla-changelog',
  jql: JQL,
  tribo: 'Mobilidade',
  count: rows.length,
  hasMore: false,
  issues: rows,
};
await writeFile(OUT_FILE, JSON.stringify(envelope), 'utf8');
console.log(`\nGravado ${rows.length} issues em ${OUT_FILE}`);
