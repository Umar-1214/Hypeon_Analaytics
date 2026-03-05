import React, { useState, useRef, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { copilotChat, copilotChatHistory, fetchCopilotSessions } from '../api'
import { useUserOrg } from '../contexts/UserOrgContext'
import DynamicDashboardRenderer from './DynamicDashboardRenderer'
import DashboardRendererErrorBoundary from './DashboardRendererErrorBoundary'

const COPILOT_SESSION_KEY = 'hypeon_copilot_session_id'

function formatSessionDate(ts) {
  if (ts == null) return ''
  const d = new Date(ts * 1000)
  const now = new Date()
  const diffDays = Math.floor((now - d) / (1000 * 60 * 60 * 24))
  if (diffDays === 0) return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  if (diffDays === 1) return 'Yesterday'
  if (diffDays < 7) return `${diffDays}d ago`
  return d.toLocaleDateString()
}

export default function CopilotPanel({ open, onClose, initialQuery = '', explainInsightId = null }) {
  const navigate = useNavigate()
  const { selectedClientId } = useUserOrg()
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState(initialQuery || '')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [sessions, setSessions] = useState([])
  const [historyOpen, setHistoryOpen] = useState(false)
  const sessionIdRef = useRef(null)
  const messagesEndRef = useRef(null)
  const inputRef = useRef(null)

  const scrollToBottom = () => messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  useEffect(() => scrollToBottom(), [messages, loading])

  useEffect(() => {
    if (open) {
      const t = setTimeout(() => inputRef.current?.focus(), 100)
      return () => clearTimeout(t)
    }
  }, [open])

  const loadSession = (sessionId) => {
    sessionIdRef.current = sessionId
    sessionStorage.setItem(COPILOT_SESSION_KEY, sessionId)
    setHistoryOpen(false)
    setError(null)
    copilotChatHistory(sessionId)
      .then(({ messages: history }) => {
        setMessages(
          (history || []).map((m) => ({
            role: m.role,
            text: m.content || '',
            layout: m.layout ?? null,
          }))
        )
      })
      .catch(() => setMessages([]))
  }

  useEffect(() => {
    if (!open) return
    fetchCopilotSessions()
      .then((r) => setSessions(r.sessions || []))
      .catch(() => setSessions([]))
    const stored = sessionStorage.getItem(COPILOT_SESSION_KEY)
    if (stored) {
      sessionIdRef.current = stored
      copilotChatHistory(stored)
        .then(({ messages: history }) => {
          if (history?.length) {
            setMessages(
              history.map((m) => ({
                role: m.role,
                text: m.content || '',
                layout: m.layout ?? null,
              }))
            )
          }
        })
        .catch(() => {})
    } else {
      sessionIdRef.current = null
      setMessages([])
    }
    if (initialQuery) setInput(initialQuery)
  }, [open, initialQuery])

  const send = async () => {
    const text = input.trim()
    if (!text || loading) return
    setError(null)
    setInput('')
    setMessages((prev) => [...prev, { role: 'user', text }])
    setLoading(true)
    try {
      const res = await copilotChat({
        message: text,
        session_id: sessionIdRef.current || undefined,
        client_id: selectedClientId,
      })
      if (res.session_id) {
        sessionIdRef.current = res.session_id
        sessionStorage.setItem(COPILOT_SESSION_KEY, res.session_id)
        fetchCopilotSessions().then((r) => setSessions(r.sessions || [])).catch(() => {})
      }
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', text: res.text || '', layout: res.layout || null },
      ])
    } catch (err) {
      setError(err.message || 'Something went wrong')
      setMessages((prev) => [...prev, { role: 'assistant', text: '', error: err.message }])
    } finally {
      setLoading(false)
    }
  }

  const clearChat = () => {
    sessionIdRef.current = null
    sessionStorage.removeItem(COPILOT_SESSION_KEY)
    setMessages([])
    setError(null)
  }

  const openFullCopilot = () => {
    onClose()
    navigate('/copilot')
  }

  if (!open) return null

  return (
    <div className="fixed inset-y-0 right-0 w-full max-w-lg flex flex-col z-50 bg-white/90 backdrop-blur-xl border-l border-slate-200 shadow-glass">
      <div className="flex items-center justify-between p-4 border-b border-slate-200 flex-shrink-0">
        <div className="flex items-center gap-2">
          <span className="text-brand-600 font-semibold" aria-hidden>◎</span>
          <h2 className="text-base font-semibold text-slate-800">Copilot</h2>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setHistoryOpen((o) => !o)}
            className="text-xs text-slate-500 hover:text-brand-600 transition-colors"
          >
            {historyOpen ? 'Hide history' : 'Previous chats'}
          </button>
          {messages.length > 0 && (
            <button
              type="button"
              onClick={clearChat}
              className="text-xs text-slate-500 hover:text-brand-600 transition-colors"
            >
              New chat
            </button>
          )}
          <button
            type="button"
            onClick={openFullCopilot}
            className="text-xs text-brand-600 hover:text-brand-700 font-medium"
          >
            Open in full
          </button>
          <button type="button" onClick={onClose} className="text-slate-500 hover:text-slate-700" aria-label="Close Copilot">
            ×
          </button>
        </div>
      </div>

      {historyOpen && (
        <div className="flex-shrink-0 border-b border-slate-200 max-h-44 overflow-auto bg-slate-50">
          <p className="text-xs font-semibold text-slate-500 px-4 pt-2 pb-1 uppercase tracking-wider">Previous chats</p>
          <ul className="pb-2">
            {sessions.length === 0 ? (
              <li className="px-4 py-2 text-sm text-slate-500">No previous chats</li>
            ) : (
              sessions.map((s) => (
                <li key={s.session_id}>
                  <button
                    type="button"
                    onClick={() => loadSession(s.session_id)}
                    className="w-full text-left px-4 py-2 text-sm text-slate-700 hover:bg-brand-50 truncate rounded-lg mx-2"
                  >
                    <span className="block truncate">{s.title || 'New chat'}</span>
                    <span className="text-xs text-slate-400">{formatSessionDate(s.updated_at)}</span>
                  </button>
                </li>
              ))
            )}
          </ul>
        </div>
      )}

      <div className="flex-1 overflow-auto flex flex-col gap-4 p-4 min-h-0">
        {messages.length === 0 && !loading && (
          <div className="text-sm text-slate-500 text-center py-8">
            <p className="font-medium text-slate-700">Ask about your marketing analytics</p>
            <p className="mt-1">Try: &quot;Summarize my top campaigns&quot; or &quot;Show me a table of spend by campaign&quot;</p>
          </div>
        )}
        {messages.map((msg, idx) => (
          <div
            key={idx}
            className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
          >
            <div
              className={`max-w-[85%] rounded-2xl px-4 py-2.5 ${
                msg.role === 'user'
                  ? 'bg-brand-600 text-white'
                  : 'glass-card border border-slate-200'
              }`}
            >
              {msg.error ? (
                <p className="text-sm text-red-600">{msg.error}</p>
              ) : msg.role === 'user' ? (
                <p className="text-sm whitespace-pre-wrap">{msg.text}</p>
              ) : (
                <>
                  {msg.text ? (
                    <div className="prose prose-sm max-w-none text-slate-700 prose-p:my-1 prose-ul:my-1 prose-li:my-0 prose-table:border-collapse prose-table:w-full prose-th:bg-slate-100 prose-td:border-b prose-td:border-slate-200">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.text}</ReactMarkdown>
                    </div>
                  ) : null}
                  {msg.layout?.widgets?.length > 0 && (
                    <div className="mt-3 rounded-xl border border-slate-200 bg-white/90 p-2">
                      <DashboardRendererErrorBoundary>
                        <DynamicDashboardRenderer layout={msg.layout} />
                      </DashboardRendererErrorBoundary>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        ))}
        {loading && (
          <div className="flex justify-start">
            <div className="glass-card rounded-2xl px-4 py-2.5 flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-brand-500 animate-pulse" />
              <span className="w-2 h-2 rounded-full bg-brand-500 animate-pulse" style={{ animationDelay: '150ms' }} />
              <span className="w-2 h-2 rounded-full bg-brand-500 animate-pulse" style={{ animationDelay: '300ms' }} />
              <span className="text-sm text-slate-500 ml-1">Thinking…</span>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {error && (
        <div className="px-4 py-2 bg-red-50/90 text-red-700 text-sm flex-shrink-0 border-t border-red-100 flex items-center justify-between gap-2">
          <span>{error}</span>
          <button
            type="button"
            onClick={() => {
              setError(null)
              const lastUser = [...messages].reverse().find((m) => m.role === 'user')
              if (lastUser?.text) {
                setInput(lastUser.text)
                inputRef.current?.focus()
              }
            }}
            className="rounded-lg border border-red-300 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50"
          >
            Retry
          </button>
        </div>
      )}
      <div className="p-4 border-t border-slate-200 flex-shrink-0 bg-white/80">
        <div className="flex gap-2">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && send()}
            placeholder="Message Copilot…"
            className="flex-1 rounded-xl border border-slate-200 px-3 py-2.5 text-sm placeholder-slate-400 focus:ring-2 focus:ring-brand-500/50 focus:border-brand-500 bg-white/90"
            disabled={loading}
            aria-label="Message Copilot"
          />
          <button
            type="button"
            onClick={send}
            disabled={loading || !input.trim()}
            className="rounded-xl bg-brand-600 text-white px-4 py-2.5 text-sm font-medium hover:bg-brand-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {loading ? '…' : 'Send'}
          </button>
        </div>
      </div>
    </div>
  )
}
