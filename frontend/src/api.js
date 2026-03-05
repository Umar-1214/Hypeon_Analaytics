import { getApiBase, getApiKey } from './runtimeConfig'
import { getTokenForRequest } from './apiAuth'

function apiBase() {
  return getApiBase() || ''
}

async function getAuthHeaders() {
  const h = { 'X-Organization-Id': 'default' }
  const token = await getTokenForRequest()
  if (token) h['Authorization'] = `Bearer ${token}`
  const apiKey = getApiKey()
  if (apiKey) h['X-API-Key'] = apiKey
  return h
}

function apiErrorMessage(res, err) {
  if (err && typeof err === 'object' && typeof err.detail === 'object' && err.detail?.message) return err.detail.message
  if (err?.message) return err.message
  if (res) {
    if (res.status === 502 || res.status === 503) return 'Backend not reachable. Start the backend on port 8001 and retry.'
    if (res.status === 404) return 'Not found. Ensure backend is running and routes are mounted.'
  }
  return res?.statusText || 'Request failed'
}

// ----- Current user's organization and datasets (call after login) -----
export async function fetchMe() {
  const res = await fetch(`${apiBase()}/api/v1/me`, { headers: await getAuthHeaders() })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(apiErrorMessage(res, err))
  }
  return res.json()
}

// ----- Dashboard API (cache-only, <300ms) -----
export async function fetchBusinessOverview(params = {}) {
  const sp = new URLSearchParams()
  if (params.client_id != null) sp.set('client_id', params.client_id)
  const res = await fetch(`${apiBase()}/api/v1/dashboard/business-overview?${sp}`, { headers: await getAuthHeaders() })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(apiErrorMessage(res, err))
  }
  return res.json()
}

export async function fetchCampaignPerformance(params = {}) {
  const sp = new URLSearchParams()
  if (params.client_id != null) sp.set('client_id', params.client_id)
  const res = await fetch(`${apiBase()}/api/v1/dashboard/campaign-performance?${sp}`, { headers: await getAuthHeaders() })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(apiErrorMessage(res, err))
  }
  return res.json()
}

export async function fetchFunnel(params = {}) {
  const sp = new URLSearchParams()
  if (params.client_id != null) sp.set('client_id', params.client_id)
  const res = await fetch(`${apiBase()}/api/v1/dashboard/funnel?${sp}`, { headers: await getAuthHeaders() })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(apiErrorMessage(res, err))
  }
  return res.json()
}

// Actions endpoint removed (no decision engine). Action Center shows empty list.
export async function fetchActions(params = {}) {
  return Promise.resolve({ items: [], count: 0 })
}

// ----- Analysis API (raw staging tables, in-depth breakdowns) -----
export async function fetchGoogleAdsAnalysis({ client_id, days, start_date, end_date } = {}) {
  const sp = new URLSearchParams()
  if (client_id != null) sp.set('client_id', client_id)
  if (days != null) sp.set('days', days)
  if (start_date) sp.set('start_date', start_date)
  if (end_date) sp.set('end_date', end_date)
  const res = await fetch(`${apiBase()}/api/v1/analysis/google-ads?${sp}`, { headers: await getAuthHeaders() })
  if (!res.ok) throw new Error(res.statusText)
  return res.json()
}

export async function fetchGoogleAnalyticsAnalysis({ client_id, days, start_date, end_date } = {}) {
  const sp = new URLSearchParams()
  if (client_id != null) sp.set('client_id', client_id)
  if (days != null) sp.set('days', days)
  if (start_date) sp.set('start_date', start_date)
  if (end_date) sp.set('end_date', end_date)
  const res = await fetch(`${apiBase()}/api/v1/analysis/google-analytics?${sp}`, { headers: await getAuthHeaders() })
  if (!res.ok) throw new Error(res.statusText)
  return res.json()
}

// ----- Copilot chat (LLM + run_sql on ADS/GA4 datasets) -----
export async function copilotChat({ message, session_id, client_id } = {}) {
  const res = await fetch(`${apiBase()}/api/v1/copilot/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(await getAuthHeaders()) },
    body: JSON.stringify({ message: message || '', session_id, client_id }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    const msg = (typeof err.detail === 'object' && err.detail?.message) || err.message || res.statusText
    throw new Error(msg || 'Request failed')
  }
  return res.json()
}

/** Stream copilot chat: calls onEvent for each SSE event (phase + message, then done/error). Returns { promise, cancel }. */
export function copilotChatStream({ message, session_id, client_id } = {}, onEvent) {
  const controller = new AbortController()
  const promise = (async () => {
    const res = await fetch(`${apiBase()}/api/v1/copilot/chat/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(await getAuthHeaders()) },
      body: JSON.stringify({ message: message || '', session_id, client_id }),
      signal: controller.signal,
    })
    if (!res.ok) {
      const err = await res.json().catch(() => ({}))
      const msg = (typeof err.detail === 'object' && err.detail?.message) || err.message || res.statusText
      throw new Error(msg || 'Request failed')
    }
    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buf = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buf += decoder.decode(value, { stream: true })
      const lines = buf.split('\n\n')
      buf = lines.pop() || ''
      for (const block of lines) {
        const m = block.match(/^data:\s*(.+)$/m)
        if (m) {
          try {
            const ev = JSON.parse(m[1])
            if (onEvent) onEvent(ev)
          } catch (_) {}
        }
      }
    }
    if (buf) {
      const m = buf.match(/^data:\s*(.+)$/m)
      if (m) try { if (onEvent) onEvent(JSON.parse(m[1])) } catch (_) {}
    }
  })()
  return { promise, cancel: () => controller.abort() }
}

export async function copilotChatHistory(session_id) {
  const res = await fetch(`${apiBase()}/api/v1/copilot/chat/history?session_id=${encodeURIComponent(session_id)}`, {
    headers: await getAuthHeaders(),
  })
  if (!res.ok) throw new Error(res.statusText)
  return res.json()
}

export async function fetchCopilotSessions() {
  const res = await fetch(`${apiBase()}/api/v1/copilot/sessions`, { headers: await getAuthHeaders() })
  if (!res.ok) throw new Error(res.statusText)
  return res.json()
}

/** Diagnostic: which session store is used (firestore vs memory) and current org. Used to explain why old chats may not appear. */
export async function fetchCopilotStoreInfo() {
  const res = await fetch(`${apiBase()}/api/v1/copilot/store-info`, { headers: await getAuthHeaders() })
  if (!res.ok) return null
  return res.json()
}

// ----- Existing -----
export async function fetchInsights(params = {}) {
  const sp = new URLSearchParams()
  if (params.client_id != null) sp.set('client_id', params.client_id)
  if (params.status) sp.set('status', params.status)
  if (params.limit) sp.set('limit', params.limit)
  const url = `${apiBase()}/insights?${sp}`
  const res = await fetch(url, { headers: await getAuthHeaders() })
  if (!res.ok) throw new Error(res.statusText)
  return res.json()
}

export async function applyRecommendation(insightId, status, userId = null) {
  const res = await fetch(`${apiBase()}/recommendations/apply`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(await getAuthHeaders()) },
    body: JSON.stringify({ insight_id: insightId, status, user_id: userId }),
  })
  if (!res.ok) throw new Error(res.statusText)
  return res.json()
}

export async function copilotQuery(insightId) {
  const res = await fetch(`${apiBase()}/copilot_query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(await getAuthHeaders()) },
    body: JSON.stringify({ insight_id: insightId }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail?.message || err.message || res.statusText)
  }
  return res.json()
}

export function copilotStream(insightId, onEvent) {
  const controller = new AbortController()
  const promise = (async () => {
    const res = await fetch(`${apiBase()}/copilot/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(await getAuthHeaders()) },
      body: JSON.stringify({ insight_id: insightId }),
      signal: controller.signal,
    })
    if (!res.ok) throw new Error(res.statusText)
    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buf = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buf += decoder.decode(value, { stream: true })
      const lines = buf.split('\n\n')
      buf = lines.pop() || ''
      for (const block of lines) {
        const m = block.match(/^data:\s*(.+)$/m)
        if (m) {
          try {
            const ev = JSON.parse(m[1])
            onEvent(ev)
          } catch (_) {}
        }
      }
    }
    if (buf) {
      const m = buf.match(/^data:\s*(.+)$/m)
      if (m) try { onEvent(JSON.parse(m[1])) } catch (_) {}
    }
  })()
  return { promise, cancel: () => controller.abort() }
}
