import React, { useState, useRef, useEffect } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { copilotChatStream, copilotChatHistory, fetchCopilotSessions, fetchCopilotStoreInfo } from './api'
import { useUserOrg } from './contexts/UserOrgContext'

const COPILOT_SESSION_KEY = 'hypeon_copilot_session_id'

/** Sidebar logo: put your image at frontend/public/logo.png (or .svg and use /logo.svg). Falls back to default if missing. */
const SIDEBAR_LOGO = '/images/hypeon.png'

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

function HypeOnGradientLogo({ size = 'lg' }) {
  const id = React.useId().replace(/:/g, '')
  const dims = size === 'xl' ? { box: 'w-24 h-24', svg: 48 } : size === 'lg' ? { box: 'w-16 h-16', svg: 32 } : size === 'sidebar' ? { box: 'w-12 h-12', svg: 24 } : size === 'md' ? { box: 'w-10 h-10', svg: 20 } : { box: 'w-6 h-6', svg: 12 }
  return (
    <div
      className={`flex items-center justify-center flex-shrink-0 ${dims.box}`}
      aria-hidden
    >
      <svg
        width={dims.svg}
        height={dims.svg}
        viewBox="0 0 24 24"
        fill="none"
        className="flex-shrink-0"
      >
        <defs>
          <linearGradient id={`logoGrad-${id}`} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#7c3aed" />
            <stop offset="50%" stopColor="#ec4899" />
            <stop offset="100%" stopColor="#f97316" />
          </linearGradient>
        </defs>
        <path
          d="M7 17L17 7M17 7h-7m7 0v7"
          stroke={`url(#logoGrad-${id})`}
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </div>
  )
}

function SidebarLogo({ size = 'sm' }) {
  const [imgFailed, setImgFailed] = React.useState(false)
  const boxClass = size === 'xl' ? 'w-24 h-24' : size === 'lg' ? 'w-16 h-16' : size === 'sidebar' ? 'w-12 h-12' : size === 'md' ? 'w-10 h-10' : 'w-6 h-6'
  if (!imgFailed && SIDEBAR_LOGO) {
    return (
      <img
        src={SIDEBAR_LOGO}
        alt="Logo"
        className={`object-contain flex-shrink-0 ${boxClass}`}
        onError={() => setImgFailed(true)}
      />
    )
  }
  return <HypeOnGradientLogo size={size} />
}

export default function CopilotChat() {
  const location = useLocation()
  const navigate = useNavigate()
  const { selectedClientId } = useUserOrg()
  const initialSessionId = location.state?.sessionId || null
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [streamStatus, setStreamStatus] = useState(null)
  const [error, setError] = useState(null)
  const streamRef = useRef(null)
  const [sessions, setSessions] = useState([])
  const [activeSessionId, setActiveSessionId] = useState(initialSessionId || sessionStorage.getItem(COPILOT_SESSION_KEY))
  const sessionIdRef = useRef(initialSessionId || sessionStorage.getItem(COPILOT_SESSION_KEY))
  const messagesEndRef = useRef(null)
  const listEndRef = useRef(null)
  const inputRef = useRef(null)
  const streamingTextRef = useRef('')
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [searchQuery, setSearchQuery] = useState('')
  const [model, setModel] = useState('basic')
  const [storeInfo, setStoreInfo] = useState(null)
  const [sessionsError, setSessionsError] = useState(null)

  const scrollToBottom = () => messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })

  useEffect(() => {
    scrollToBottom()
  }, [messages, loading])

  const loadSessions = () => {
    setSessionsError(null)
    fetchCopilotSessions()
      .then((r) => {
        setSessions(r.sessions || [])
        setSessionsError(null)
      })
      .catch((err) => {
        setSessions([])
        setSessionsError(err?.message || 'Could not load chat history')
      })
  }

  useEffect(() => {
    fetchCopilotStoreInfo().then((info) => info && setStoreInfo(info)).catch(() => {})
    loadSessions()
  }, [])

  const loadSession = (sid) => {
    sessionIdRef.current = sid
    setActiveSessionId(sid)
    sessionStorage.setItem(COPILOT_SESSION_KEY, sid)
    setError(null)
    setMessages([])
    navigate('/copilot', { state: { sessionId: sid }, replace: true })
    copilotChatHistory(sid)
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

  const startNewChat = () => {
    sessionIdRef.current = null
    setActiveSessionId(null)
    sessionStorage.removeItem(COPILOT_SESSION_KEY)
    setMessages([])
    setError(null)
    setInput('')
    navigate('/copilot', { state: {}, replace: true })
  }

  useEffect(() => {
    if (initialSessionId) {
      sessionIdRef.current = initialSessionId
      setActiveSessionId(initialSessionId)
      copilotChatHistory(initialSessionId)
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
    } else if (!initialSessionId) {
      const stored = sessionStorage.getItem(COPILOT_SESSION_KEY)
      if (stored) {
        sessionIdRef.current = stored
        setActiveSessionId(stored)
        copilotChatHistory(stored).then(({ messages: history }) => {
          if (history?.length) setMessages(history.map((m) => ({ role: m.role, text: m.content || '', layout: m.layout ?? null })))
        }).catch(() => {})
      } else {
        sessionIdRef.current = null
        setActiveSessionId(null)
        setMessages([])
      }
    }
  }, [initialSessionId])

  const send = async () => {
    const text = input.trim()
    if (!text || loading) return
    setError(null)
    setStreamStatus(null)
    setInput('')
    setMessages((prev) => [...prev, { role: 'user', text }])
    setLoading(true)
    if (streamRef.current?.cancel) streamRef.current.cancel()
    streamingTextRef.current = ''
    const { promise, cancel } = copilotChatStream(
      { message: text, session_id: sessionIdRef.current || undefined, client_id: selectedClientId },
      (ev) => {
        if (ev.phase === 'analyzing' || ev.phase === 'discovering' || ev.phase === 'generating_sql' || ev.phase === 'running_query' || ev.phase === 'formatting') {
          setStreamStatus(ev.message || 'Processing…')
        } else if (ev.phase === 'answer_chunk' && ev.chunk) {
          streamingTextRef.current += ev.chunk
          setStreamStatus(null) // clear "Formatting results…" so user sees answer streaming
          setMessages((prev) => {
            const last = prev[prev.length - 1]
            if (last?.role === 'assistant' && last?.streaming) {
              return [...prev.slice(0, -1), { ...last, text: streamingTextRef.current }]
            }
            return [...prev, { role: 'assistant', text: streamingTextRef.current, streaming: true }]
          })
        } else if (ev.phase === 'done') {
          if (ev.session_id) {
            sessionIdRef.current = ev.session_id
            setActiveSessionId(ev.session_id)
            sessionStorage.setItem(COPILOT_SESSION_KEY, ev.session_id)
            loadSessions()
          }
          setMessages((prev) => {
            const last = prev[prev.length - 1]
            const finalText = ev.answer ?? streamingTextRef.current ?? ''
            if (last?.role === 'assistant' && last?.streaming) {
              return [...prev.slice(0, -1), { role: 'assistant', text: finalText, data: ev.data || null }]
            }
            return [...prev, { role: 'assistant', text: finalText, data: ev.data || null }]
          })
          setStreamStatus(null)
          setLoading(false)
        } else if (ev.phase === 'error') {
          setError(ev.error || 'Something went wrong')
          setMessages((prev) => [...prev, { role: 'assistant', text: '', error: ev.error }])
          setStreamStatus(null)
          setLoading(false)
        }
      }
    )
    streamRef.current = { cancel }
    try {
      await promise
    } catch (err) {
      if (err.name === 'AbortError') return
      setError(err.message || 'Something went wrong')
      setMessages((prev) => [...prev, { role: 'assistant', text: '', error: err.message }])
      setStreamStatus(null)
      setLoading(false)
    } finally {
      streamRef.current = null
    }
  }

  const exampleCards = [
    {
      title: "ROAS Analysis",
      description: "Analyze return on ad spend and campaign profitability at a glance.",
      icon: "chart",
      question: "Give me a quick overview of our ROAS performance."
    },
    {
      title: "Campaign Performance",
      description: "Understand how your campaigns are performing across all channels.",
      icon: "brand",
      question: "Analyze our campaign performance across all channels."
    },
    {
      title: "CPC Performance",
      description: "Track cost-per-click trends and identify optimization opportunities.",
      icon: "trend",
      question: "How is our CPC trending and where can we optimize?"
    },
    {
      title: "LTV Analysis",
      description: "Measure customer lifetime value and long-term profitability.",
      icon: "search",
      question: "What is our customer lifetime value and how can we improve it?"
    }
  ];

  const filteredSessions = searchQuery.trim()
    ? sessions.filter((s) => (s.title || 'New chat').toLowerCase().includes(searchQuery.toLowerCase()))
    : sessions

  return (
    <div className="flex h-full min-h-0 overflow-hidden copilot-page-bg animate-copilot-fade-in">
      {/* Left sidebar: glass panel, fixed */}
      <aside
        className={`copilot-sidebar flex flex-col min-h-0 ${sidebarOpen ? '' : 'copilot-sidebar-collapsed'}`}
      >
        <div className={`topRow ${!sidebarOpen ? 'railTop' : ''}`}>
          <div className={`headerLeft ${!sidebarOpen ? 'justify-center' : ''}`}>
            {sidebarOpen ? (
              <div className="logoWrapper">
                <SidebarLogo size="sidebar" />
              </div>
            ) : (
              <button
                type="button"
                className="logoMini"
                onClick={() => setSidebarOpen(true)}
                aria-label="Open sidebar"
              >
                <span className="logoImg">
                  <SidebarLogo size="sidebar" />
                </span>
                <span className="logoMiniArrow" aria-hidden>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4"><path d="M9 18l6-6-6-6" strokeLinecap="round" strokeLinejoin="round" /></svg>
                </span>
              </button>
            )}
            {sidebarOpen && (
              <span className="brandText truncate"></span>
            )}
          </div>
          <button
            type="button"
            onClick={() => setSidebarOpen((o) => !o)}
            className="topToggle"
            aria-label={sidebarOpen ? 'Close sidebar' : 'Open sidebar'}
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className={`w-4 h-4 transition-transform ${sidebarOpen ? '' : 'rotate-180'}`}>
              <path d="M15 19l-7-7 7-7" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </button>
        </div>
        <div className={`section ${!sidebarOpen ? 'railTop' : ''}`}>
          <button
            type="button"
            onClick={startNewChat}
            className={`newChat ${!sidebarOpen ? 'railIconBtn' : ''}`}
          >
            <span className="iconWrap" aria-hidden>+</span>
            <span className="newChatText">New chat</span>
          </button>
          <div className={`menuBtn ${!sidebarOpen ? 'railIconBtn' : ''}`}>
            <span className="iconWrap shrink-0" aria-hidden>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4"><path d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>
            </span>
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search chats"
              aria-label="Search chats"
            />
          </div>
        </div>
        <div className="chatsWrapper flex-1 min-h-0 overflow-hidden flex flex-col">
          {sidebarOpen && (
            <>
              <p className="sectionTitle">YOUR CHATS</p>
              {storeInfo?.store === 'memory' && (
                <p className="px-2 py-1 text-xs text-amber-600 dark:text-amber-400" title="Backend is using in-memory store; history is lost on restart. Configure Firestore for persistent chat history.">
                  History not saved (in-memory)
                </p>
              )}
              {sessionsError && (
                <p className="px-2 py-1 text-xs text-red-600 dark:text-red-400" title={sessionsError}>
                  {sessionsError}
                </p>
              )}
              {filteredSessions.length === 0 && !sessionsError ? (
                <p className="px-2 py-1.5 text-xs text-slate-500 italic">No recent chats</p>
              ) : filteredSessions.length > 0 ? (
                <ul className="list">
                  {filteredSessions.map((s) => (
                    <li key={s.session_id}>
                      <button
                        type="button"
                        onClick={() => loadSession(s.session_id)}
                        className={`listItem ${activeSessionId === s.session_id ? 'active' : ''}`}
                        title={s.title || 'New chat'}
                      >
                        <span className="shrink-0" aria-hidden>
                          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-3.5 h-3.5"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
                        </span>
                        <span className="chatTitle">{s.title || 'New chat'}</span>
                        <span className="text-[10px] shrink-0 text-slate-400">{formatSessionDate(s.updated_at)}</span>
                      </button>
                    </li>
                  ))}
                </ul>
              ) : null}
            </>
          )}
        </div>
        <div ref={listEndRef} />
        {sidebarOpen && (
          <button
            type="button"
            onClick={() => navigate('/dashboard')}
            className="backToAnalytics"
          >
            <span className="iconWrap shrink-0" aria-hidden>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4">
                <path d="M3 3v18h18" /><path d="M18 17V9" /><path d="M13 17V5" /><path d="M8 17v-3" />
              </svg>
            </span>
            Back to Analytics
          </button>
        )}
        <div className={`footer sidebar-avatar-wrap ${!sidebarOpen ? 'railBottom justify-center' : ''}`}>
          <div className={`avatar ${!sidebarOpen ? 'avatarRail' : ''}`}>YM</div>
          {sidebarOpen && (
            <div className="min-w-0 flex-1">
              <p className="username truncate">Yash Malviya</p>
              <p className="plan mt-0.5">basic</p>
            </div>
          )}
        </div>
      </aside>

      {/* Main chat area – margin so content is not under fixed sidebar */}
      <div
        className={`flex-1 flex flex-col min-h-0 min-w-0 overflow-hidden bg-transparent font-copilot animate-copilot-main-in transition-[margin] duration-300 ${
          sidebarOpen ? 'ml-[240px]' : 'ml-[50px]'
        }`}
      >
        <div className="flex-1 min-h-0 overflow-y-auto relative px-6 pt-8 pb-4">
          <div className="w-full max-w-2xl mx-auto px-4 sm:px-6">
            {messages.length === 0 && !loading && (
              <div className="absolute inset-0 flex flex-col items-center justify-center text-center px-6 font-copilot animate-copilot-fade-in">
                <div className="mb-6 shrink-0" aria-hidden>
                  <SidebarLogo size="xl" />
                </div>
                <h2 className="text-xl sm:text-2xl font-bold text-slate-800 tracking-tight">Ask anything about your analytics</h2>
                <p className="mt-2 text-slate-500 text-sm max-w-md mx-auto">
                Get instant AI-powered insights, predictions, and automated reporting from your enterprise data.
                </p>
                <p className="mt-6 text-[10px] font-semibold text-slate-400 uppercase tracking-wider">GET STARTED WITH AN EXAMPLE BELOW</p>
                <div className="mt-4 w-full max-w-2xl grid grid-cols-1 sm:grid-cols-2 gap-3">
                  {exampleCards.map(({ title, description, icon, question }, idx) => (
                    <button
                      key={title}
                      type="button"
                      onClick={() => setInput(question)}
                      style={{ animationDelay: `${idx * 50}ms` }}
                      className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-left shadow-sm hover:shadow-md hover:border-slate-300 transition-all duration-200 flex items-start gap-3 group animate-copilot-slide-up opacity-0"
                    >
                      <span className="w-9 h-9 rounded-lg flex items-center justify-center bg-slate-100 text-slate-500 group-hover:bg-brand-50 group-hover:text-brand-600 transition-colors shrink-0" aria-hidden>
                        {icon === 'chart' && <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4"><path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/></svg>}
                        {icon === 'brand' && <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>}
                        {icon === 'trend' && <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>}
                        {icon === 'search' && <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4"><path d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>}
                      </span>
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-semibold text-slate-800">{title}</p>
                        <p className="text-xs text-slate-500 mt-0.5">{description}</p>
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg, idx) => (
              <div
                key={idx}
                style={{ animationDelay: `${idx * 25}ms` }}
                className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'} mb-6 ${msg.role === 'user' ? 'animate-copilot-slide-in-right opacity-0' : 'animate-copilot-slide-in-left opacity-0'}`}
              >
                <div
                  className={`max-w-[85%] min-w-0 rounded-2xl px-4 py-3 ${
                    msg.role === 'user'
                      ? 'bg-gradient-to-r from-brand-500 to-brand-600 text-white shadow-lg shadow-brand-500/20'
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
                      {msg.data && Array.isArray(msg.data) && msg.data.length > 0 && (
                        <div className="mt-4">
                          <details className="group">
                            <summary className="text-sm font-medium text-slate-600 cursor-pointer list-none flex items-center gap-2 py-1 hover:text-slate-800">
                              <span className="inline-block w-4 h-4 rounded border border-slate-300 group-open:rotate-90 transition-transform" aria-hidden />
                              Detailed data ({msg.data.length} row{msg.data.length !== 1 ? 's' : ''})
                            </summary>
                            <div className="mt-2 overflow-x-auto rounded-lg border border-slate-200 bg-slate-50/50">
                              <table className="min-w-full text-sm copilot-data-table">
                                <thead className="bg-slate-100">
                                  <tr>
                                    {Object.keys(msg.data[0]).map((k) => (
                                      <th key={k} className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">{k}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {msg.data.slice(0, 25).map((row, i) => (
                                    <tr key={i} className="border-t border-slate-200 hover:bg-slate-50/80">
                                      {Object.values(row).map((v, j) => (
                                        <td key={j} className="px-3 py-2 text-slate-600">{String(v ?? '')}</td>
                                      ))}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                              {msg.data.length > 25 && (
                                <p className="px-3 py-2 text-xs text-slate-500 border-t border-slate-200">Showing first 25 of {msg.data.length} rows</p>
                              )}
                            </div>
                          </details>
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>
            ))}

            {loading && (
              <div className="flex justify-start mb-6 animate-copilot-fade-in">
                <div className="glass-card rounded-2xl px-4 py-3 flex items-center gap-3 min-w-[200px]">
                  <span className="flex gap-1 shrink-0">
                    <span className="inline-block w-2 h-2 rounded-full bg-brand-500 animate-pulse" />
                    <span className="inline-block w-2 h-2 rounded-full bg-brand-500 animate-pulse" style={{ animationDelay: '150ms' }} />
                    <span className="inline-block w-2 h-2 rounded-full bg-brand-500 animate-pulse" style={{ animationDelay: '300ms' }} />
                  </span>
                  <span className="text-sm text-slate-600 font-medium">{streamStatus || 'Thinking…'}</span>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        </div>

        {error && (
          <div className="flex-shrink-0 px-6 py-2 bg-red-50/90 border-t border-red-100 text-red-700 text-sm flex items-center justify-between gap-3">
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
              className="rounded-lg border border-red-300 bg-white px-3 py-1.5 text-sm font-medium text-red-700 hover:bg-red-50"
            >
              Retry
            </button>
          </div>
        )}

        <div className="flex-shrink-0 px-6 py-4 flex justify-center">
          <div className="w-full max-w-2xl mx-auto">
            <div className="rounded-2xl border border-slate-200 bg-white shadow-card transition-all duration-300 focus-within:ring-2 focus-within:ring-brand-500/30 focus-within:border-brand-400 px-3 py-2">
              <div className="flex items-center gap-2">
              
                <input
                  ref={inputRef}
                  type="text"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && send()}
                  placeholder="Describe what you want to analyze..."
                  className="flex-1 min-w-0 py-3 text-sm placeholder-slate-400 bg-transparent focus:outline-none"
                  disabled={loading}
                  aria-label="Message"
                />
                <button
                  type="button"
                  onClick={send}
                  disabled={loading || !input.trim()}
                  className="w-10 h-10 rounded-full bg-gradient-to-r from-brand-500 to-brand-600 text-white flex items-center justify-center transition-all hover:opacity-90 disabled:opacity-40 disabled:cursor-not-allowed shadow-md"
                  aria-label="Send"
                >
                  {loading ? <span className="text-sm">…</span> : (
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M12 19V5"/><path d="M5 12l7-7 7 7"/></svg>
                  )}
                </button>
              </div>
              
            </div>
            <p className="mt-2 text-[10px] text-slate-400 text-center">
              AI COPILOT CAN MAKE MISTAKES. VERIFY IMPORTANT INFO.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
