// Runs the actual App/HTTP client in memory using existing React/TypeScript.
// The hook scheduler models state/effect lifecycles; this is not a browser test.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const React = require('react');
const { renderToStaticMarkup } = require('react-dom/server');
const ts = require('typescript');

function component() {
  const hooks = [];
  let cursor = 0, dirty = true, tree;
  const pending = [];
  const calls = [];
  const data = {
    complete: true, total: 45, missing: false, failed: false,
    drift: false, reject: false, hold: false,
  };
  const deferred = [];
  const hooksReact = {
    ...React,
    useState(initial) {
      const index = cursor++;
      hooks[index] ||= { value: initial };
      return [hooks[index].value, next => {
        const value = typeof next === 'function' ? next(hooks[index].value) : next;
        if (!Object.is(value, hooks[index].value)) {
          hooks[index].value = value;
          dirty = true;
        }
      }];
    },
    useEffect(effect, deps) {
      const index = cursor++;
      const previous = hooks[index];
      if (!previous || deps.some((d, i) => !Object.is(d, previous.deps[i]))) {
        hooks[index] = { deps, cleanup: previous?.cleanup };
        pending.push(() => {
          hooks[index].cleanup?.();
          hooks[index].cleanup = effect();
        });
      }
    },
  };
  function response(url, state) {
    if (state.reject && !url.includes('?limit=100')) {
      return { ok: false, status: 500, statusText: 'Error', json: async () => ({ detail: 'fixture failure' }) };
    }
    if (state.missing && !url.includes('?limit=100')) {
      return { ok: false, status: 404, statusText: 'Not Found', json: async () => ({ detail: 'Run not found' }) };
    }
    let body;
    if (url === '/api/runs?limit=100') {
      body = { runs: ['unit-a', 'unit-b'].map(run_id => ({ run_id, summary_present: true })) };
    } else if (url.endsWith('/drift')) {
      body = state.drift
        ? { status: 'evaluated', success: true, evaluation_complete: true, evaluated_windows: 1,
            windows: [{ window_index: 0, records: 1000, status: 'evaluated', alert: true }], errors: [] }
        : { status: 'disabled', success: false, evaluation_complete: false, windows: [], errors: [] };
    } else if (url.endsWith('/prediction-counts')) {
      body = { verified: true, total: state.total, predicted_normal: state.total - 1, predicted_attack: 1 };
    } else if (url.includes('/predictions?')) {
      const offset = Number(new URL(url, 'http://fixture').searchParams.get('offset'));
      body = { verified: true, total: state.total, offset, limit: 20,
        predictions: Array.from({ length: Math.min(20, state.total - offset) }, (_, i) => ({
          event_id: `fixture:${offset + i}`, source_row: offset + i,
          prediction: 0, probability_attack: .1,
        })) };
    } else {
      body = { success: state.complete && !state.failed, accounting_complete: state.complete,
        inference: { verified: true, predictions: state.total }, errors: state.failed ? ['fixture incomplete'] : [] };
    }
    return { ok: true, json: async () => body };
  }
  // Deferred replies deliberately ignore abort to test the component's guards.
  const fetch = async url => {
    calls.push(url);
    const state = { ...data };
    if (state.hold && !url.includes('?limit=100')) {
      return new Promise(resolve => deferred.push(() => resolve(response(url, state))));
    }
    return response(url, state);
  };
  function load(file, requireModule) {
    const output = ts.transpileModule(fs.readFileSync(path.join(__dirname, '../src', file), 'utf8'), {
      compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true },
    }).outputText;
    const exports = {};
    vm.runInNewContext(output, { exports, require: requireModule, fetch, AbortController, URL, Error });
    return exports;
  }
  const api = load('api.ts', require);
  const App = load('App.tsx', name => name === 'react' ? hooksReact : name === './api' ? api : require(name)).default;
  function render() {
    if (dirty) {
      dirty = false;
      cursor = 0;
      tree = App();
      while (pending.length) pending.shift()();
    }
    return tree;
  }
  async function settle() {
    for (let i = 0; i < 12; i++) {
      render();
      await new Promise(resolve => setImmediate(resolve));
    }
  }
  function walk(node) {
    if (Array.isArray(node)) return node.flatMap(walk);
    if (!node || typeof node !== 'object') return [];
    return [node, ...walk(node.props?.children)];
  }
  function click(text) {
    const button = walk(render()).find(n => n.type === 'button' && renderToStaticMarkup(n).includes(text));
    assert.ok(button, `Missing button ${text}`);
    assert.ok(!button.props.disabled, `Disabled button ${text}`);
    button.props.onClick();
  }
  return { data, calls, settle, click,
    html: () => renderToStaticMarkup(render()),
    selected: () => walk(render()).filter(n => n.type === 'button' && n.props['aria-pressed']).map(n => renderToStaticMarkup(n)),
    release: () => deferred.splice(0).forEach(resolve => resolve()),
    close: () => hooks.forEach(h => h.cleanup?.()),
  };
}

async function selected(t) {
  const ui = component();
  t.after(ui.close);
  await ui.settle();
  ui.click('unit-a');
  await ui.settle();
  assert.match(ui.html(), /45 saved classifications/);
  return ui;
}

test('Refresh updates displayed details, drift and counts without changing selection', async t => {
  const ui = await selected(t);
  ui.data.total = 73;
  ui.data.drift = true;
  ui.click('Refresh');
  assert.doesNotMatch(ui.html(), /Analysis finished|45 saved classifications/);
  await ui.settle();
  assert.match(ui.html(), /73 saved classifications/);
  assert.match(ui.html(), /Change detected/);
  assert.match(ui.html(), /Records classified<\/span><strong>73/);
  assert.match(ui.selected()[0], /unit-a/);
  assert.equal(ui.calls.filter(p => p === '/api/runs/unit-a').length, 2);
  assert.equal(ui.calls.filter(p => p === '/api/runs?limit=100').length, 2);
});

test('Refresh replaces displayed success with incomplete and failed results', async t => {
  const ui = await selected(t);
  ui.data.complete = false;
  ui.click('Refresh');
  await ui.settle();
  assert.match(ui.html(), /Analysis incomplete/);
  assert.doesNotMatch(ui.html(), /Analysis finished|Results available|45 saved classifications/);
  ui.data.complete = true;
  ui.data.failed = true;
  ui.click('Refresh');
  await ui.settle();
  assert.match(ui.html(), /fixture incomplete/);
  assert.doesNotMatch(ui.html(), /Results available/);
  ui.data.failed = false;
  ui.data.total = 52;
  ui.click('Refresh');
  await ui.settle();
  assert.match(ui.html(), /52 saved classifications/);
  assert.match(ui.html(), /Results available/);
  assert.doesNotMatch(ui.html(), /Analysis incomplete/);
});

test('Refresh removes stale success when selected evidence is missing or requests fail', async t => {
  const ui = await selected(t);
  ui.data.missing = true;
  ui.click('Refresh');
  await ui.settle();
  assert.match(ui.html(), /HTTP 404: Run not found/);
  assert.doesNotMatch(ui.html(), /Analysis finished|45 saved classifications/);
  ui.data.reject = true;
  ui.click('Refresh');
  await ui.settle();
  assert.match(ui.html(), /HTTP 500: fixture failure/);
  assert.doesNotMatch(ui.html(), /Analysis finished/);
});

test('Pagination and selection work; Refresh resets pagination for the selected run', async t => {
  const ui = await selected(t);
  ui.click('Next');
  await ui.settle();
  assert.match(ui.html(), /Record 21/);
  ui.click('Refresh');
  await ui.settle();
  assert.match(ui.html(), /Record 1</);
  assert.doesNotMatch(ui.html(), /Record 21/);
  ui.click('unit-b');
  await ui.settle();
  assert.match(ui.selected()[0], /unit-b/);
});

test('Late aborted replies cannot restore stale success after a newer Refresh', async t => {
  const ui = await selected(t);
  ui.data.hold = true;
  ui.click('Refresh');
  await ui.settle();
  ui.data.hold = false;
  ui.data.complete = false;
  ui.click('Refresh');
  await ui.settle();
  ui.release();
  await ui.settle();
  assert.match(ui.html(), /Analysis incomplete/);
  assert.doesNotMatch(ui.html(), /Analysis finished|45 saved classifications/);
});
