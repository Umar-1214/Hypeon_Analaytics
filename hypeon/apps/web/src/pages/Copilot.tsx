import { useCallback, useEffect, useRef, useState } from 'react'
import { api, CopilotMessageItem, CopilotSessionItem } from '../api'
import CopilotMessageContent from '../components/CopilotMessageContent'

const SUGGESTED_QUESTIONS = [
  'How are we doing?',
  'Spend by channel',
  'Revenue by channel',
  'Which channel performs best?',
  'What decisions do we have?',
  'How do I optimize budget?',
  'Is our attribution stable?',
  'Should we scale up?',
]

function formatSessionTitle(title: string | null | undefined, id: number): string {
  if (title && title.trim()) return title.length > 36 ? title.slice(0, 36) + '…' : title
  return `Chat ${id}`
}

function formatSessionDate(d: string): string {
  const date = new Date(d)
  const now = new Date()
  const sameDay = date.toDateString() === now.toDateString()
  if (sameDay) return date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })
  const yesterday = new Date(now)
  yesterday.setDate(yesterday.getDate() - 1)
  if (date.toDateString() === yesterday.toDateString()) return 'Yesterday'
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

export default function Copilot() {
  const [context, setContext] = useState<Awaited<ReturnType<typeof api.copilotContext>> | null>(null)
  const [sessions, setSessions] = useState<CopilotSessionItem[]>([])
  const [currentSessionId, setCurrentSessionId] = useState<number | null>(null)
  const [messages, setMessages] = useState<CopilotMessageItem[]>([])
  const [input, setInput] = useState('')
  const [streamingContent, setStreamingContent] = useState<string | null>(null)
  const [streamingSources, setStreamingSources] = useState<string[] | null>(null)
  const [streamingModelVersions, setStreamingModelVersions] = useState<{
    mta_version?: string
    mmm_version?: string
  } | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLTextAreaElement>(null)

  const loadSessions = useCallback(() => {
    api.copilotSessions().then((r) => setSessions(r.sessions)).catch(() => setSessions([]))
  }, [])

  const loadMessages = useCallback((sessionId: number) => {
    api
      .copilotSessionMessages(sessionId)
      .then((r) => setMessages(r.messages))
      .catch(() => setMessages([]))
  }, [])

  useEffect(() => {
    const end = new Date().toISOString().slice(0, 10)
    api
      .copilotContext(90, { start_date: '2025-01-01', end_date: end })
      .then(setContext)
      .catch(() => setContext(null))
    loadSessions()
  }, [loadSessions])

  useEffect(() => {
    if (currentSessionId != null) loadMessages(currentSessionId)
    else setMessages([])
  }, [currentSessionId, loadMessages])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, streamingContent])

  const startNewChat = () => {
    setCurrentSessionId(null)
    setMessages([])
    setInput('')
    setStreamingContent(null)
    setStreamingSources(null)
    setStreamingModelVersions(null)
    setError(null)
    api
      .copilotCreateSession()
      .then((s) => {
        setSessions((prev) => [s, ...prev])
        setCurrentSessionId(s.id)
      })
      .catch((e: Error) =>
        setError(e?.message ? `Failed to create session: ${e.message}` : 'Failed to create session')
      )
  }

  const selectSession = (s: CopilotSessionItem) => {
    setCurrentSessionId(s.id)
    setError(null)
  }

  const ask = async (q: string) => {
    const text = (q || input).trim()
    if (!text) return
    setLoading(true)
    setError(null)
    setStreamingContent('')
    setStreamingSources(null)
    setStreamingModelVersions(null)

    let sessionId: number | undefined = currentSessionId ?? undefined
    if (sessionId == null) {
      try {
        const s = await api.copilotCreateSession()
        setSessions((prev) => [s, ...prev])
        setCurrentSessionId(s.id)
        sessionId = s.id
      } catch {
        setError('Failed to create session')
        setLoading(false)
        return
      }
    }

    setInput('')
    setMessages((prev) => [...prev, { id: -1, role: 'user', content: text, created_at: new Date().toISOString() }])

    const dateRange =
      context?.start_date && context?.end_date
        ? { start_date: context.start_date, end_date: context.end_date }
        : undefined
    api
      .copilotAskStream(text, sessionId, {
        onData: (delta) => setStreamingContent((prev) => (prev ?? '') + delta),
        onDone: (_answer, sources, modelVersions) => {
          setStreamingContent(null)
          setStreamingSources(sources ?? null)
          setStreamingModelVersions(modelVersions ?? null)
          loadSessions()
          api.copilotSessionMessages(sessionId!).then((r) => setMessages(r.messages))
        },
        onError: (err) => setError(err),
      }, dateRange)
      .finally(() => setLoading(false))
  }

  const isEmpty = messages.length === 0 && !streamingContent && !loading
  const showSuggestions = isEmpty

  return (
    <div className="flex h-full min-h-0 bg-surface-50">
      {/* Sessions sidebar */}
      <aside className="w-64 shrink-0 flex flex-col border-r border-surface-200 bg-white">
        <div className="p-4 border-b border-surface-200">
          <button
            type="button"
            onClick={startNewChat}
            className="btn-primary w-full"
          >
            <span aria-hidden className="mr-2">+</span>
            New chat
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-2">
          <p className="text-overline font-semibold text-surface-500 px-3 mb-2">Recent</p>
          {sessions.length === 0 && (
            <p className="text-caption text-surface-400 px-3">No chats yet.</p>
          )}
          {sessions.map((s) => (
            <button
              key={s.id}
              type="button"
              onClick={() => selectSession(s)}
              className={`w-full text-left rounded-input px-3 py-2.5 text-body-md mb-0.5 flex flex-col gap-0.5 transition-colors ${
                currentSessionId === s.id
                  ? 'bg-surface-100 text-surface-900 font-medium'
                  : 'text-surface-600 hover:bg-surface-50'
              }`}
              title={s.title ?? `Session ${s.id}`}
            >
              <span className="truncate">{formatSessionTitle(s.title, s.id)}</span>
              <span className="text-caption text-surface-400">{formatSessionDate(s.created_at)}</span>
            </button>
          ))}
        </div>
        {context && (
          <div className="p-3 border-t border-surface-200 bg-surface-50/80">
            <p className="text-overline font-semibold text-surface-500 mb-1">Data in scope</p>
            <p className="text-caption text-surface-600">
              {context.start_date} → {context.end_date} · $
              {context.total_spend?.toLocaleString(undefined, { maximumFractionDigits: 0 })} spend
            </p>
          </div>
        )}
      </aside>

      <main className="flex-1 flex flex-col min-w-0">
        {error && (
          <div className="mx-4 mt-3 card border-error-200 bg-error-50 p-4 text-error-800 text-body-md">
            {error}
          </div>
        )}

        <div className="flex-1 overflow-y-auto">
          <div className="max-w-3xl mx-auto px-4 py-8">
            {showSuggestions && (
              <div className="text-center py-12">
                <h2 className="font-display text-display-sm font-semibold text-surface-900 mb-1">
                  HypeOn Copilot
                </h2>
                <p className="text-body-md text-surface-500 mb-8">
                  Ask about your dashboard in plain language. Data is fetched when you ask.
                </p>
                <p className="text-overline font-semibold text-surface-500 mb-3">Try asking</p>
                <div className="flex flex-wrap justify-center gap-2">
                  {SUGGESTED_QUESTIONS.map((q) => (
                    <button
                      key={q}
                      type="button"
                      onClick={() => ask(q)}
                      disabled={loading}
                      className="btn-secondary rounded-full px-4 py-2.5"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {!showSuggestions && (
              <div className="space-y-6">
                {messages.map((m) => (
                  <div
                    key={m.id}
                    className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}
                  >
                    <div
                      className={`max-w-[85%] rounded-card px-4 py-3 ${
                        m.role === 'user'
                          ? 'bg-brand-600 text-white'
                          : 'card py-4'
                      }`}
                    >
                      {m.role === 'user' ? (
                        <p className="text-body-md whitespace-pre-wrap">{m.content}</p>
                      ) : (
                        <div className="text-body-md">
                          <CopilotMessageContent content={m.content} />
                        </div>
                      )}
                    </div>
                  </div>
                ))}

                {streamingContent != null && (
                  <div className="flex justify-start">
                    <div className="max-w-[85%] card px-4 py-3">
                      <div className="text-body-md">
                        <CopilotMessageContent content={streamingContent} />
                        <span className="animate-pulse">▌</span>
                      </div>
                    </div>
                  </div>
                )}

                {streamingSources != null && streamingSources.length > 0 && (
                  <div className="flex justify-start">
                    <div className="max-w-[85%] text-caption text-surface-500 px-2">
                      Based on: {streamingSources.join(', ')}
                      {streamingModelVersions &&
                        (streamingModelVersions.mta_version || streamingModelVersions.mmm_version) && (
                          <span className="ml-2">
                            · MTA {streamingModelVersions.mta_version ?? '—'}, MMM{' '}
                            {streamingModelVersions.mmm_version ?? '—'}
                          </span>
                        )}
                    </div>
                  </div>
                )}

                <div ref={messagesEndRef} />
              </div>
            )}
          </div>
        </div>

        <div className="border-t border-surface-200 bg-white p-4">
          <div className="max-w-3xl mx-auto flex gap-3 items-end">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  ask(input)
                }
              }}
              placeholder="Ask anything about your dashboard..."
              rows={1}
              className="input-base flex-1 resize-none min-h-[48px] max-h-32 py-3"
              disabled={loading}
            />
            <button
              type="button"
              onClick={() => ask(input)}
              disabled={loading || !input.trim()}
              className="btn-primary shrink-0 min-h-[48px] px-5 py-3"
            >
              {loading ? '…' : 'Send'}
            </button>
          </div>
          <p className="text-caption text-surface-400 text-center mt-2 max-w-3xl mx-auto">
            Copilot uses your dashboard data and session context to answer follow-up questions.
          </p>
        </div>
      </main>
    </div>
  )
}
