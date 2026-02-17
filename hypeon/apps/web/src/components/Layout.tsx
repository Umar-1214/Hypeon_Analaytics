import { ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

const navItems = [
  { to: '/', label: 'Overview', description: 'Data & model health' },
  { to: '/metrics', label: 'Metrics', description: 'Spend & revenue' },
  { to: '/decisions', label: 'Decisions', description: 'Recommendations' },
  { to: '/report', label: 'Attribution vs MMM', description: 'Alignment report' },
  { to: '/copilot', label: 'Copilot', description: 'Ask in plain language', highlight: true },
]

export default function Layout({ children }: { children: ReactNode }) {
  const location = useLocation()
  const isCopilot = location.pathname === '/copilot'

  return (
    <div className="h-screen flex flex-col md:flex-row bg-surface-50 overflow-hidden">
      {/* Fixed sidebar: does not scroll; only main content scrolls */}
      <aside className="w-full md:w-60 lg:w-64 shrink-0 flex flex-col h-full bg-surface-900 text-surface-100 border-b md:border-b-0 md:border-r border-surface-800">
        <div className="p-5 border-b border-surface-800 shrink-0">
          <div className="flex items-baseline gap-2">
            <span className="font-display font-semibold text-lg tracking-tight text-white">HypeOn</span>
            <span className="text-overline font-medium text-surface-400 tracking-wider">Analytics</span>
          </div>
          <p className="text-caption text-surface-500 mt-1.5">Marketing attribution & optimization</p>
        </div>
        <nav className="flex-1 min-h-0 p-3 space-y-0.5 overflow-y-auto">
          <span className="px-3 py-1.5 text-overline font-semibold uppercase tracking-wider text-surface-500">
            Navigation
          </span>
          {navItems.map(({ to, label, description, highlight }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `flex flex-col gap-0.5 px-3 py-2.5 rounded-input text-body-md transition-colors ${
                  highlight
                    ? isActive
                      ? 'bg-brand-600 text-white'
                      : 'text-surface-300 hover:bg-surface-800 hover:text-white'
                    : isActive
                    ? 'bg-surface-800 text-white font-medium'
                    : 'text-surface-400 hover:bg-surface-800/80 hover:text-surface-200'
                }`
              }
            >
              <span className="font-medium">{label}</span>
              <span className="text-caption opacity-90">{description}</span>
            </NavLink>
          ))}
        </nav>
        <div className="p-3 border-t border-surface-800 shrink-0">
          <p className="text-caption text-surface-500">
            Enterprise-grade attribution · MTA & MMM
          </p>
        </div>
      </aside>
      {/* Main: fills from sidebar to right edge; only this area scrolls (non-Copilot) */}
      <main className="flex-1 flex flex-col min-h-0 min-w-0 bg-surface-50 overflow-hidden">
        {isCopilot ? (
          children
        ) : (
          <div className="flex-1 min-h-0 min-w-0 w-full overflow-auto">
            {children}
          </div>
        )}
      </main>
    </div>
  )
}
