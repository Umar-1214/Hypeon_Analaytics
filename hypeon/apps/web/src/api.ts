const API_BASE = import.meta.env.VITE_API_URL || '/api';

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

/** Standard envelope from /api/v1. Parse and return data or throw on errors. */
export interface ApiEnvelope<T = unknown> {
  success: boolean;
  data: T;
  meta?: { correlation_id?: string };
  errors: string[];
}

async function requestV1<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  });
  const body = (await res.json()) as ApiEnvelope<T>;
  if (!res.ok || !body.success) {
    const msg = body.errors?.length ? body.errors.join('; ') : await res.text();
    throw new Error(msg || `Request failed: ${path}`);
  }
  return body.data as T;
}

export interface UnifiedMetricRow {
  date: string;
  channel: string;
  spend: number;
  attributed_revenue: number;
  roas?: number;
  mer?: number;
  cac?: number;
  revenue_new?: number;
  revenue_returning?: number;
}

export interface UnifiedMetricsResponse {
  metrics: UnifiedMetricRow[];
  start_date?: string;
  end_date?: string;
}

export interface DecisionRow {
  decision_id: string;
  created_at: string;
  entity_type: string;
  entity_id: string;
  decision_type: string;
  reason_code: string;
  explanation_text?: string;
  projected_impact?: number;
  confidence_score: number;
  status: string;
}

export interface DecisionsResponse {
  decisions: DecisionRow[];
  total: number;
}

export interface MMMStatusResponse {
  last_run_id?: string;
  last_run_at?: string;
  status: string;
}

export interface MMMResultRow {
  run_id: string;
  created_at: string;
  channel: string;
  coefficient: number;
  goodness_of_fit_r2?: number;
  model_version?: string;
}

export interface MMMResultsResponse {
  run_id?: string;
  results: MMMResultRow[];
}

export interface SimulateRequest {
  meta_spend_change?: number;
  google_spend_change?: number;
}

export interface SimulateResponse {
  projected_revenue_delta: number;
  current_spend: Record<string, number>;
  new_spend: Record<string, number>;
}

export interface BudgetAllocationResponse {
  total_budget: number;
  recommended_allocation: Record<string, number>;
  current_spend: Record<string, number>;
  predicted_revenue_at_recommended: number;
}

export interface AttributionMMMReportResponse {
  channels: string[];
  attribution_share: Record<string, number>;
  mmm_share: Record<string, number>;
  disagreement_score: number;
  instability_flagged: boolean;
}

export interface CopilotContextResponse {
  start_date?: string;
  end_date?: string;
  lookback_days: number;
  channels: string[];
  total_spend: number;
  total_revenue: number;
  roas_overall: number;
  decisions_total: number;
  decisions_pending: number;
  mmm_last_run_id?: string;
  instability_flagged: boolean;
}

export interface CopilotAskResponse {
  answer: string;
  sources: string[];
  session_id?: number;
  message_id?: number;
}

export interface CopilotSessionItem {
  id: number;
  title?: string | null;
  created_at: string;
}

export interface CopilotMessageItem {
  id: number;
  role: string;
  content: string;
  created_at: string;
}

/** Enriched decision from /api/v1/decisions */
export interface EnrichedDecision {
  decision_id: string;
  channel: string;
  recommended_action: string;
  budget_change_pct?: number;
  reasoning: { mta_support?: number; mmm_support?: number; alignment_score?: number };
  risk_flags: string[];
  confidence_score: number;
  model_versions?: { mta_version?: string; mmm_version?: string };
  run_id?: string;
  created_at?: string;
  decision_type?: string;
  explanation_text?: string;
  status?: string;
}

export interface ModelInfo {
  run_id?: string;
  timestamp?: string;
  mta_version?: string;
  mmm_version?: string;
  data_snapshot_id?: string;
}

export const apiV1 = {
  engineHealth: () => requestV1<{ status: string }>('/api/v1/engine/health'),
  engineRun: (seed?: number) =>
    requestV1<{ run_id: string; timestamp?: string; mta_version?: string; mmm_version?: string; data_snapshot_id?: string }>(
      seed ? `/api/v1/engine/run?seed=${seed}` : '/api/v1/engine/run',
      { method: 'POST' }
    ),
  mtaDiagnostics: () => requestV1<Record<string, unknown>>('/api/v1/mta/diagnostics'),
  mmmDiagnostics: () => requestV1<Record<string, unknown>>('/api/v1/mmm/diagnostics'),
  reconciliation: (params?: { start_date?: string; end_date?: string }) => {
    const q = new URLSearchParams();
    if (params?.start_date) q.set('start_date', params.start_date);
    if (params?.end_date) q.set('end_date', params.end_date);
    return requestV1<{ channel_alignment: Record<string, unknown>; overall_alignment_score: number; alignment_confidence: number }>(
      `/api/v1/reconciliation?${q}`
    );
  },
  decisions: (status?: string) =>
    requestV1<{ decisions: EnrichedDecision[]; total: number }>(
      status ? `/api/v1/decisions?status=${status}` : '/api/v1/decisions'
    ),
  modelInfo: () => requestV1<ModelInfo>('/api/v1/model-info'),
};

/** Base URL for SSE (same origin or VITE_API_URL without /api suffix for EventSource) */
const SSE_BASE = (() => {
  const base = import.meta.env.VITE_API_URL || '';
  if (base && base.startsWith('http')) return base.replace(/\/api\/?$/, '');
  return '';
})();

/** Subscribe to pipeline_finished SSE events; callback when event received. Returns cleanup function. */
export function subscribePipelineEvents(onPipelineFinished: (runId: string) => void): () => void {
  const url = SSE_BASE ? `${SSE_BASE}/api/v1/events/pipeline` : '/api/v1/events/pipeline';
  const es = new EventSource(url);
  es.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data) as { event?: string; run_id?: string };
      if (data.event === 'pipeline_finished' && data.run_id) onPipelineFinished(data.run_id);
    } catch {
      // ignore
    }
  };
  es.onerror = () => es.close();
  return () => es.close();
}

export const api = {
  health: () => request<{ status: string }>('/health'),
  metrics: (params?: { start_date?: string; end_date?: string; channel?: string }) => {
    const q = new URLSearchParams();
    if (params?.start_date) q.set('start_date', params.start_date);
    if (params?.end_date) q.set('end_date', params.end_date);
    if (params?.channel) q.set('channel', params.channel);
    return request<UnifiedMetricsResponse>(`/metrics/unified?${q}`);
  },
  decisions: (status?: string) =>
    request<DecisionsResponse>(status ? `/decisions?status=${status}` : '/decisions'),
  mmmStatus: () => request<MMMStatusResponse>('/model/mmm/status'),
  mmmResults: (runId?: string) =>
    request<MMMResultsResponse>(runId ? `/model/mmm/results?run_id=${runId}` : '/model/mmm/results'),
  simulate: (body: SimulateRequest) =>
    request<SimulateResponse>('/simulate', { method: 'POST', body: JSON.stringify(body) }),
  optimizerBudget: (totalBudget: number) =>
    request<BudgetAllocationResponse>(`/optimizer/budget?total_budget=${totalBudget}`),
  reportAttributionMmm: (params?: { start_date?: string; end_date?: string }) => {
    const q = new URLSearchParams();
    if (params?.start_date) q.set('start_date', params.start_date);
    if (params?.end_date) q.set('end_date', params.end_date);
    return request<AttributionMMMReportResponse>(`/report/attribution-mmm-comparison?${q}`);
  },
  runPipeline: (seed?: number) =>
    request<{ run_id: string; status: string; message: string }>(
      seed ? `/run?seed=${seed}` : '/run',
      { method: 'POST' }
    ),
  runPipelineSync: (seed?: number) =>
    request<{ run_id: string; status: string; message: string }>(
      seed ? `/run/sync?seed=${seed}` : '/run/sync',
      { method: 'POST' }
    ),
  copilotContext: (lookbackDays = 90, params?: { start_date?: string; end_date?: string }) => {
    const q = new URLSearchParams({ lookback_days: String(lookbackDays) });
    if (params?.start_date) q.set('start_date', params.start_date);
    if (params?.end_date) q.set('end_date', params.end_date);
    return request<CopilotContextResponse>(`/copilot/context?${q}`);
  },
  copilotSessions: () =>
    request<{ sessions: CopilotSessionItem[] }>('/copilot/sessions'),
  copilotCreateSession: () =>
    request<CopilotSessionItem>('/copilot/sessions', { method: 'POST' }),
  copilotSessionMessages: (sessionId: number) =>
    request<{ session_id: number; messages: CopilotMessageItem[] }>(`/copilot/sessions/${sessionId}/messages`),
  copilotAsk: (
    question: string,
    sessionId?: number,
    dateRange?: { start_date?: string; end_date?: string }
  ) =>
    request<CopilotAskResponse>('/copilot/ask', {
      method: 'POST',
      body: JSON.stringify({
        question,
        session_id: sessionId ?? null,
        start_date: dateRange?.start_date ?? null,
        end_date: dateRange?.end_date ?? null,
      }),
    }),
  /** Stream answer via SSE; onData(delta), onDone(answer, sources, model_versions_used). Pass dateRange so answer uses same range as "Data in scope". */
  copilotAskStream: async (
    question: string,
    sessionId: number | undefined,
    callbacks: {
      onData: (delta: string) => void;
      onDone: (answer: string, sources: string[], model_versions_used?: { mta_version?: string; mmm_version?: string }) => void;
      onError?: (err: string) => void;
    },
    dateRange?: { start_date?: string; end_date?: string }
  ): Promise<number | undefined> => {
    const API_BASE = import.meta.env.VITE_API_URL || '/api';
    const body: { question: string; session_id: number | null; start_date?: string | null; end_date?: string | null } =
      { question, session_id: sessionId ?? null };
    if (dateRange?.start_date && dateRange?.end_date) {
      body.start_date = dateRange.start_date;
      body.end_date = dateRange.end_date;
    }
    const res = await fetch(`${API_BASE}/copilot/ask/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      callbacks.onError?.(text);
      return undefined;
    }
    const reader = res.body?.getReader();
    if (!reader) {
      callbacks.onError?.('No response body');
      return undefined;
    }
    const dec = new TextDecoder();
    let buf = '';
    let newSessionId: number | undefined;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop() ?? '';
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const data = JSON.parse(line.slice(6)) as {
              delta?: string;
              done?: boolean;
              sources?: string[];
              answer?: string;
              model_versions_used?: { mta_version?: string; mmm_version?: string };
              session_id?: number;
            };
            if (data.delta) callbacks.onData(data.delta);
            if (data.done && data.sources)
              callbacks.onDone(data.answer ?? '', data.sources, data.model_versions_used);
            if (data.session_id != null) newSessionId = data.session_id;
          } catch {
            // skip malformed
          }
        }
      }
    }
    return newSessionId;
  },
};
