import StatusBlock from './StatusBlock.jsx'
import { relativeTime } from '../format.js'

export default function AlertLog({ alerts, loading, error, apiUrl }) {
  const items = alerts ?? []

  return (
    <section className="panel">
      <h2 className="panel__title">
        <span className="panel__title-tick panel__title-tick--red" />
        Alert log
      </h2>

      <StatusBlock
        loading={loading}
        error={error}
        empty={items.length === 0}
        emptyText="No failures logged. The pipeline has been quiet."
        apiUrl={apiUrl}
      />

      {!loading && !error && items.length > 0 && (
        <ul className="alert-list">
          {items.map((a) => (
            <li key={a.id} className="alert-list__row">
              <div className="alert-list__head">
                <span className="alert-list__source">
                  {a.dag_id}.{a.task_id}
                </span>
                <span className="alert-list__time">{relativeTime(a.alerted_at)}</span>
              </div>
              <p className="alert-list__error">{a.error_message}</p>
              {a.log_url && (
                <a className="alert-list__link" href={a.log_url} target="_blank" rel="noreferrer">
                  view logs →
                </a>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
