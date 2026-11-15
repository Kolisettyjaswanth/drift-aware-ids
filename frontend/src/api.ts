export type Run = { run_id: string; summary_present: boolean };
export type RunDetail = { run_id: string; success: boolean; accounting_complete: boolean; inference: { verified: boolean; predictions?: number; valid_records?: number } | null; drift: { status: string } | null; errors: string[] };
export type Prediction = { event_id: string; source_row: number; prediction: number; prediction_label: string; probability_attack: number; probability_benign: number };
export type Predictions = { total: number; offset: number; limit: number; verified: boolean; predictions: Prediction[] };
export type DriftWindow = { window_index: number; status: string; records: number; alert: boolean | null; source_capture?: string; ks_results?: { alert: boolean; features?: Record<string, { ks_statistic: number; p_value: number; drift_detected: boolean }> } };
export type Drift = { status: string; success: boolean; evaluation_complete: boolean; evaluated_windows: number; snapshot_id: string | null; errors: string[]; windows: DriftWindow[] };

export async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal });
  if (!response.ok) {
    let detail = response.statusText;
    try { const body: { detail?: string } = await response.json(); detail = body.detail || detail; } catch { /* keep HTTP message */ }
    throw new Error(`HTTP ${response.status}: ${detail}`);
  }
  return response.json() as Promise<T>;
}
