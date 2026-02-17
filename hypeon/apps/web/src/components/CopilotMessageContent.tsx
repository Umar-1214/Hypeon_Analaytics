import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

const tableClass =
  'w-full border-collapse text-body-md my-3 rounded-input overflow-hidden border border-surface-200'

export default function CopilotMessageContent({ content }: { content: string }) {
  return (
    <div className="copilot-markdown prose prose-sm max-w-none prose-headings:font-display prose-headings:font-semibold prose-headings:text-surface-900 prose-p:text-surface-700 prose-li:text-surface-700 prose-strong:text-surface-900">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          table: ({ children }) => (
            <div className="overflow-x-auto my-3 rounded-input border border-surface-200 shadow-card">
              <table className={tableClass}>{children}</table>
            </div>
          ),
          thead: ({ children }) => (
            <thead className="bg-surface-50 text-surface-700 font-semibold">{children}</thead>
          ),
          tbody: ({ children }) => (
            <tbody className="bg-white divide-y divide-surface-100">{children}</tbody>
          ),
          tr: ({ children }) => (
            <tr className="border-b border-surface-100 last:border-0">{children}</tr>
          ),
          th: ({ children }) => (
            <th className="text-left py-2.5 px-4 text-surface-600 font-semibold">{children}</th>
          ),
          td: ({ children }) => (
            <td className="py-2.5 px-4 text-surface-800">{children}</td>
          ),
          p: ({ children }) => <p className="my-2 leading-relaxed">{children}</p>,
          ul: ({ children }) => <ul className="list-disc pl-5 my-2 space-y-1">{children}</ul>,
          ol: ({ children }) => <ol className="list-decimal pl-5 my-2 space-y-1">{children}</ol>,
          strong: ({ children }) => (
            <strong className="font-semibold text-surface-900">{children}</strong>
          ),
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  )
}
