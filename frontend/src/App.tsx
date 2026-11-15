import { useEffect, useState } from "react";
import { getJson, type Drift, type Predictions, type Run, type RunDetail } from "./api";

const PAGE_SIZE = 20;
const message = (error: unknown) => error instanceof Error ? error.message : "Unexpected error";

export default function App() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [runId, setRunId] = useState("");
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [drift, setDrift] = useState<Drift | null>(null);
  const [predictions, setPredictions] = useState<Predictions | null>(null);
  const [offset, setOffset] = useState(0);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    getJson<{ runs: Run[] }>("/api/runs?limit=100", controller.signal)
      .then(data => setRuns(data.runs))
      .catch(err => { if (!controller.signal.aborted) setError(message(err)); });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    setDetail(null); setDrift(null); setPredictions(null); setError("");
    if (!runId) return;
    const controller = new AbortController();
    setLoading(true);
    Promise.all([
      getJson<RunDetail>(`/api/runs/${encodeURIComponent(runId)}`, controller.signal),
      getJson<Drift>(`/api/runs/${encodeURIComponent(runId)}/drift`, controller.signal),
    ]).then(([run, driftData]) => { setDetail(run); setDrift(driftData); })
      .catch(err => { if (!controller.signal.aborted) setError(message(err)); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [runId]);

  useEffect(() => {
    setPredictions(null);
    if (!runId || !detail?.success || !detail.accounting_complete || !detail.inference?.verified) return;
    const controller = new AbortController();
    getJson<Predictions>(`/api/runs/${encodeURIComponent(runId)}/predictions?offset=${offset}&limit=${PAGE_SIZE}`, controller.signal)
      .then(setPredictions)
      .catch(err => { if (!controller.signal.aborted) setError(message(err)); });
    return () => controller.abort();
  }, [runId, detail, offset]);

  return <main>
    <header><p className="eyebrow">CIC-IDS2017 · Bounded replay evidence</p><h1>Intrusion Detection Evidence</h1><p>Read-only inspection of persisted Kafka/Spark run results. Not a live network monitor.</p></header>
    <section><label htmlFor="run">Select a replay run</label><select id="run" value={runId} onChange={e => { setOffset(0); setRunId(e.target.value); }}><option value="">Choose a run</option>{runs.map(r => <option key={r.run_id} value={r.run_id}>{r.run_id}</option>)}</select><button onClick={() => window.location.reload()}>Refresh run list</button></section>
    {loading && <p role="status">Loading evidence…</p>}
    {error && <p role="alert" className="error">{error}</p>}
    {detail && <section><h2>Run integrity</h2><div className="stats"><div><span>Run success</span><strong>{String(detail.success)}</strong></div><div><span>Accounting complete</span><strong>{String(detail.accounting_complete)}</strong></div><div><span>Inference verified</span><strong>{detail.inference ? String(detail.inference.verified) : "Unavailable"}</strong></div><div><span>Prediction count</span><strong>{detail.inference?.predictions ?? "N/A"}</strong></div></div>{detail.errors?.length > 0 && <p className="error">{detail.errors.join("; ")}</p>}</section>}
    {drift && <section><h2>KS drift monitoring</h2><p><strong>Status:</strong> {drift.status} · <strong>Evaluation complete:</strong> {String(drift.evaluation_complete)}</p>{drift.status === "disabled" ? <p>Monitoring was disabled. No drift conclusion is available.</p> : drift.windows.length === 0 ? <p>No evaluated windows available.</p> : drift.windows.map(w => <article key={w.window_index}><h3>Window {w.window_index} · {w.records} records</h3><p>Status: {w.status} · Drift alert: <strong>{w.alert === null ? "Not evaluated" : w.alert ? "Detected" : "Not detected"}</strong></p>{w.source_capture && <p>Capture: {w.source_capture}</p>}{w.ks_results?.features && <div className="scroll"><table><thead><tr><th>Feature</th><th>KS statistic</th><th>p-value</th><th>Drift detected</th></tr></thead><tbody>{Object.entries(w.ks_results.features).map(([name, v]) => <tr key={name}><td>{name}</td><td>{v.ks_statistic}</td><td>{v.p_value}</td><td>{String(v.drift_detected)}</td></tr>)}</tbody></table></div>}</article>)}{drift.errors?.length > 0 && <p className="error">{drift.errors.join("; ")}</p>}</section>}
    {detail && <section><h2>Model predictions</h2><p>These are model classifications, not KS drift alerts. The held-out classifier has known severe generalization limitations.</p>{!detail.inference?.verified || !detail.success || !detail.accounting_complete ? <p>Verified predictions are unavailable for this run.</p> : predictions ? <><p>Showing {predictions.predictions.length} of {predictions.total} verified predictions, starting at offset {offset}.</p><div className="scroll"><table><thead><tr><th>Event ID</th><th>Source row</th><th>Prediction</th><th>Attack probability</th></tr></thead><tbody>{predictions.predictions.map((p,i) => <tr key={p.event_id + "-" + i}><td>{p.event_id}</td><td>{p.source_row}</td><td>{p.prediction_label}</td><td>{Number(p.probability_attack).toFixed(4)}</td></tr>)}</tbody></table></div><div className="pager"><button disabled={offset===0} onClick={() => setOffset(Math.max(0,offset-PAGE_SIZE))}>Previous</button><span>Offset {offset}</span><button disabled={offset+PAGE_SIZE>=predictions.total} onClick={() => setOffset(offset+PAGE_SIZE)}>Next</button></div></> : <p>Loading verified predictions…</p>}</section>}
    <footer>Evidence viewer only · No claims of production detection accuracy or continuous streaming</footer>
  </main>;
}
