import { useEffect, useState } from "react";
import { getJson, type Drift, type Predictions, type PredictionCounts, type Run, type RunDetail } from "./api";

const SIZE = 20;
const errorText = (e: unknown) => e instanceof Error ? e.message : "Unexpected error";
function nameOf(id: string) {
  if (id.startsWith("drift-partial")) return "Partial traffic analysis";
  if (id.startsWith("drift-")) return "Traffic change analysis";
  if (id.startsWith("transport")) return "Data delivery check";
  if (/^\d{8}T/.test(id)) return "Intrusion prediction analysis";
  return "Recorded analysis";
}
function dateOf(id: string) {
  const m = id.match(/(20\d{6})T(\d{6})/);
  if (!m) return "Saved session";
  return `${m[1].slice(0,4)}-${m[1].slice(4,6)}-${m[1].slice(6,8)} · ${m[2].slice(0,2)}:${m[2].slice(2,4)} UTC`;
}
function trafficMessage(d: Drift) {
  if (d.status === "disabled") return { label: "Not checked", description: "Traffic change monitoring was switched off. This does not mean traffic was normal.", tone: "neutral" };
  if (!d.success || !d.evaluation_complete) return { label: "Result unavailable", description: "This comparison did not finish. We cannot say whether traffic changed.", tone: "caution" };
  if (d.windows.some(w => w.status === "evaluated" && w.alert === true)) return { label: "Change detected", description: "Traffic patterns differed from the reference data. This does not prove an attack.", tone: "attention" };
  if (d.windows.some(w => w.status === "evaluated" && w.alert === false)) return { label: "No change flagged", description: "The completed comparisons did not flag a change. This is not a guarantee of safety.", tone: "positive" };
  return { label: "Not evaluated", description: "No completed comparison is available.", tone: "neutral" };
}
function predictionMessage(d: RunDetail) {
  if (!d.success || !d.accounting_complete) return { label: "Analysis incomplete", description: "The run did not finish successfully. Predictions may be missing.", tone: "caution" };
  if (!d.inference?.verified) return { label: "No verified predictions", description: "This session has no verified model classifications.", tone: "neutral" };
  return { label: "Results available", description: "The model's saved classifications can be explored below. These are not confirmed attacks.", tone: "positive" };
}
export default function App() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [runId, setRunId] = useState("");
  const [search, setSearch] = useState("");
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [drift, setDrift] = useState<Drift | null>(null);
  const [predictions, setPredictions] = useState<Predictions | null>(null);
  const [counts, setCounts] = useState<PredictionCounts | null>(null);
  const [countsError, setCountsError] = useState("");
  const [showAllRuns, setShowAllRuns] = useState(false);
  const [offset, setOffset] = useState(0);
  const [reload, setReload] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [predictionError, setPredictionError] = useState("");
  const [predictionLoading, setPredictionLoading] = useState(false);
  const [runsError, setRunsError] = useState("");
  useEffect(() => {
    const c = new AbortController();
    getJson<{ runs: Run[] }>("/api/runs?limit=100", c.signal).then(v => { setRuns(v.runs); setRunsError(""); })
      .catch(e => { if (!c.signal.aborted) setRunsError(errorText(e)); });
    return () => c.abort();
  }, [reload]);
  useEffect(() => {
    setDetail(null); setDrift(null); setPredictions(null); setCounts(null); setCountsError(""); setError(""); setPredictionError("");
    if (!runId) return;
    const c = new AbortController();
    setLoading(true);
    Promise.all([
      getJson<RunDetail>(`/api/runs/${encodeURIComponent(runId)}`, c.signal),
      getJson<Drift>(`/api/runs/${encodeURIComponent(runId)}/drift`, c.signal)
    ]).then(([a,b]) => { setDetail(a); setDrift(b); })
      .catch(e => { if (!c.signal.aborted) setError(errorText(e)); })
      .finally(() => { if (!c.signal.aborted) setLoading(false); });
    return () => c.abort();
  }, [runId]);
  useEffect(() => {
    setPredictionError("");
    if (!runId || !detail?.success || !detail.accounting_complete || !detail.inference?.verified) { setPredictionLoading(false); return; }
    const c = new AbortController();
    setPredictionLoading(true);
    getJson<Predictions>(`/api/runs/${encodeURIComponent(runId)}/predictions?offset=${offset}&limit=${SIZE}`, c.signal)
      .then(v => { if (!c.signal.aborted) setPredictions(v); })
      .catch(e => { if (!c.signal.aborted) setPredictionError(errorText(e)); })
      .finally(() => { if (!c.signal.aborted) setPredictionLoading(false); });
    return () => c.abort();
  }, [runId, detail, offset]);
  useEffect(() => {
    setCounts(null); setCountsError("");
    if (!runId || !detail?.success || !detail.accounting_complete || !detail.inference?.verified) return;
    const controller = new AbortController();
    getJson<PredictionCounts>(`/api/runs/${encodeURIComponent(runId)}/prediction-counts`, controller.signal)
      .then(setCounts).catch(e => { if (!controller.signal.aborted) setCountsError(errorText(e)); });
    return () => controller.abort();
  }, [runId, detail]);
  const query = search.trim().toLowerCase();
  const visible = runs.filter(r => query.startsWith("drift-") || /^20\d{6}t/.test(query) ? r.run_id.toLowerCase().includes(query) : `${nameOf(r.run_id)} ${r.run_id} ${dateOf(r.run_id)}`.toLowerCase().includes(query));
  const complete = detail?.success === true && detail.accounting_complete === true;
  const p = detail ? predictionMessage(detail) : null;
  const d = drift ? trafficMessage(drift) : null;
  const sessionList = showAllRuns || search.trim() ? visible : visible.slice(0, 4);
  const sessionLabel = (r: Run) => `${nameOf(r.run_id)} · ${r.run_id}`;
  return <div className="shell">
    <aside className="sidebar"><div className="brand"><span className="brand-icon">◈</span><div><strong>Sentinel</strong><small>Network insights</small></div></div><div className="nav-label">WORKSPACE</div><div className="nav-active">◫ &nbsp; Overview</div><div className="side-foot"><span className="live-dot"/> Research demonstration<br/><small>Previously recorded data</small></div></aside>
    <div className="main-area"><div className="topbar"><span>Workspace <span className="slash">/</span> <strong>Overview</strong></span><span className="environment">● &nbsp; DEMO ENVIRONMENT</span></div>
    <main className="page">
      <header className="hero"><div><div className="eyebrow">YOUR SECURITY OVERVIEW</div><h1>Understand your network.</h1><p>Explore what the system observed in recorded network traffic, in plain language.</p></div><div className="hero-art" aria-hidden="true"><span>⌁</span></div></header>
      <div className="notice"><span className="notice-icon">ⓘ</span><div><strong>This is a research demonstration.</strong><p>These are saved test results, not live monitoring. A warning does not confirm a real-world cyberattack.</p></div></div>
      <section className="panel"><div className="panel-heading"><div><span className="eyebrow">EXPLORE RESULTS</span><h2>Choose an analysis</h2><p>Pick a saved session to see what happened. Technical names are optional.</p></div><button className="refresh" onClick={() => setReload(x => x+1)}>↻ Refresh</button></div>
        <div className="search-row"><label className="searchbox"><span>⌕</span><span className="sr-only">Search saved analyses</span><input value={search} onChange={e => setSearch(e.target.value)} placeholder="Find an analysis by name or date"/></label><span className="count">{visible.length} sessions</span></div>
        <div className="session-grid">{sessionList.map(r => <button key={r.run_id} aria-pressed={runId===r.run_id} className={`session ${runId===r.run_id?"selected":""}`} onClick={() => {setRunId(r.run_id);setOffset(0);}}><span className="session-mark">{r.run_id.startsWith("drift")?"⌁":"◫"}</span><span><strong>{sessionLabel(r)}</strong><small>{dateOf(r.run_id)}</small></span><span className="session-arrow">→</span></button>)}</div>
        {runsError && <p role="alert" className="error">Unable to load saved analyses: {runsError}</p>}
        {!runsError && visible.length===0 && <p className="empty">No matching sessions. Try another search.</p>}
        {!search.trim() && visible.length>4 && <button className="show-more" onClick={()=>setShowAllRuns(x=>!x)}>{showAllRuns?"Show fewer analyses":`Browse all ${visible.length} analyses`}</button>}
        {runs.length===100 && <p className="subtle">Showing up to 100 recent sessions.</p>}
      </section>
      {error && <div role="alert" className="error">Unable to load the analysis: {error}</div>}
      {loading && <p role="status" className="loading">Loading saved evidence…</p>}
      {!runId && <section className="empty-state"><span className="empty-symbol">◈</span><h2>Your results will appear here</h2><p>Choose an analysis above to explore a clear summary of the saved evidence.</p></section>}
      {detail && drift && !loading && <div className="results">
        <div className="results-heading"><div><span className="eyebrow">ANALYSIS SUMMARY</span><h2>{nameOf(runId)}</h2><p>{dateOf(runId)} · Saved session</p></div><span className={`pill ${complete?"positive":"caution"}`}>{complete?"Analysis finished":"Analysis incomplete"}</span></div>
        <div className="insight-grid"><article className={`insight ${p?.tone}`}><div className="insight-icon">◈</div><span className="eyebrow">INTRUSION PREDICTIONS</span><h3>{p?.label}</h3><p>{p?.description}</p><div className="insight-foot">{detail.inference?.verified && complete ? `${detail.inference.predictions ?? "Verified"} saved classifications` : "No confirmed prediction result"}</div></article>
        <article className={`insight ${d?.tone}`}><div className="insight-icon">⌁</div><span className="eyebrow">TRAFFIC PATTERN CHANGES</span><h3>{d?.label}</h3><p>{d?.description}</p><div className="insight-foot">{drift.evaluation_complete ? `${drift.windows.filter(w=>w.alert===true).length} flagged out of ${drift.windows.filter(w=>w.status==="evaluated").length} evaluated samples` : "No completed conclusion"}</div></article></div>
        <details className="panel collapsible"><summary><span className="eyebrow">RESULT RELIABILITY</span><strong>How complete is this analysis?</strong><small>Open verification details</small></summary><div className="collapsible-content"><p>Results are marked verified only when the saved evidence supports them.</p><div className="reliability"><div><span className={detail.success?"ok":"warn"}>{detail.success?"✓":"!"}</span><div><strong>Processing</strong><small>{detail.success?"Finished successfully":"Did not finish successfully"}</small></div></div><div><span className={detail.accounting_complete?"ok":"warn"}>{detail.accounting_complete?"✓":"!"}</span><div><strong>Records checked</strong><small>{detail.accounting_complete?"All expected records accounted for":"Record accounting incomplete"}</small></div></div><div><span className={detail.inference?.verified?"ok":"neutral-mark"}>{detail.inference?.verified?"✓":"i"}</span><div><strong>Model evidence</strong><small>{detail.inference?.verified?"Predictions verified":"Not available or not verified"}</small></div></div></div>{detail.errors?.length>0 && <p className="error">{detail.errors.join("; ")}</p>}</div></details>
        <details className="panel collapsible"><summary><span className="eyebrow">DETAILED FINDINGS</span><strong>Traffic change observations</strong><small>Explore individual samples and statistics</small></summary><div className="collapsible-content"><p>Traffic change means patterns differed from a reference sample. It does not automatically mean an intrusion.</p>{drift.status==="disabled" ? <div className="inline-empty">ⓘ &nbsp; Monitoring was switched off for this session.</div> : drift.windows.length===0 ? <div className="inline-empty">ⓘ &nbsp; No completed traffic comparisons are available.</div> : <div className="windows">{drift.windows.map(w=><div className="window" key={w.window_index}><div><strong>Traffic sample {w.window_index+1}</strong><small>{w.records.toLocaleString()} network records</small></div><span className={`pill ${w.alert===true?"attention":w.alert===false?"positive":"caution"}`}>{w.alert===true?"Change detected":w.alert===false?"No change flagged":"Not evaluated"}</span>{w.ks_results && <details className="details"><summary>View statistical details</summary><pre>{JSON.stringify(w.ks_results,null,2)}</pre></details>}</div>)}</div>}{drift.errors?.length>0 && <p className="error">{drift.errors.join("; ")}</p>}</div></details>
        <section className="panel"><span className="eyebrow">SAVED CLASSIFICATIONS</span><h2>What did the model predict?</h2><p>These automated labels are not confirmed security incidents. Counts below cover the entire verified run, not just the visible page.</p>{counts && counts.verified ? <div className="count-summary"><div><span>Predicted normal</span><strong>{counts.predicted_normal.toLocaleString()}</strong></div><div><span>Possible attacks</span><strong>{counts.predicted_attack.toLocaleString()}</strong></div><div><span>Records classified</span><strong>{counts.total.toLocaleString()}</strong></div><div className="count-bar" aria-label={`Predicted normal ${counts.predicted_normal} of ${counts.total}; possible attacks ${counts.predicted_attack}`}><span style={{width:`${counts.total ? 100*counts.predicted_normal/counts.total : 0}%`}}/></div></div> : countsError ? <p role="alert" className="error">Full-run counts unavailable: {countsError}</p> : complete && detail.inference?.verified ? <p role="status">Checking full-run classification counts…</p> : null}{!complete||!detail.inference?.verified ? <div className="inline-empty">ⓘ &nbsp; Verified predictions are unavailable for this session.</div> : <><details className="prediction-details"><summary>Explore individual network records</summary>{predictionError && <p role="alert" className="error">Could not load predictions: {predictionError}</p>}{predictionLoading && <p role="status">Loading requested page…</p>}{!predictions || predictions.offset !== offset ? <p role="status">Loading predictions…</p> : <><div className="table-meta"><span>{predictions.total.toLocaleString()} saved classifications</span><span>Showing {predictions.total===0?0:offset+1}–{Math.min(offset+predictions.predictions.length,predictions.total)}</span></div><div className="table-scroll" aria-busy={predictionLoading || predictions.offset !== offset}><table><thead><tr><th>Network record</th><th>Model prediction</th><th>Estimated attack probability</th></tr></thead><tbody>{predictions.predictions.map((x,i)=><tr key={x.event_id+i}><td>Record {x.source_row+1}</td><td><span className={`tag ${x.prediction===1?"tag-attention":"tag-neutral"}`}>{x.prediction===1?"Possible attack":"Predicted normal"}</span></td><td>{Number.isFinite(x.probability_attack)?`${(x.probability_attack*100).toFixed(1)}%`:"Unavailable"}</td></tr>)}</tbody></table></div><div className="pager"><button disabled={predictionLoading || offset===0} onClick={()=>setOffset(Math.max(0,offset-SIZE))}>← Previous</button><span>{predictions.total===0?0:offset+1}–{Math.min(offset+predictions.predictions.length,predictions.total)} of {predictions.total}</span><button disabled={predictionLoading || offset+SIZE>=predictions.total} onClick={()=>setOffset(offset+SIZE)}>Next →</button></div></>}</details></>}</section>
        <details className="technical"><summary>Technical details for researchers</summary><div><p><strong>Run identifier:</strong> <code>{runId}</code></p><p><strong>Drift status:</strong> {drift.status}</p><p><strong>Snapshot identifier:</strong> <code>{drift.snapshot_id??"None"}</code></p><p><strong>Evaluation complete:</strong> {drift.evaluation_complete?"Yes":"No"}</p><p>KS refers to the Kolmogorov–Smirnov statistical test.</p></div></details>
      </div>}
      <footer><strong>Sentinel</strong> · Research evidence explorer <span>Historical replay only · Not a real-time security service · Model reliability on unseen attacks is limited</span></footer>
    </main></div></div>;
}
