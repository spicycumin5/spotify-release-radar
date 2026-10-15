import StatusBlock from './StatusBlock.jsx'
import { relativeTime, dateOnly } from '../format.js'

export default function ReleasesFeed({ releases, loading, error, apiUrl }) {
  const items = releases ?? []

  return (
    <section className="panel panel--feed">
      <h2 className="panel__title">
        <span className="panel__title-tick panel__title-tick--amber" />
        On air — new releases
      </h2>

      <StatusBlock
        loading={loading}
        error={error}
        empty={items.length === 0}
        emptyText="No releases yet. The pipeline hasn't caught a signal from a followed artist."
        apiUrl={apiUrl}
      />

      {!loading && !error && items.length > 0 && (
        <ul className="feed">
          {items.map((r) => (
            <li key={`${r.artist_id}-${r.album_id}`} className="feed__item">
              <div className="feed__meta">
                <span className="feed__artist">{r.artist_name}</span>
                <span className="feed__album">{r.album_name}</span>
              </div>
              <div className="feed__tags">
                <span className="badge">{r.album_type}</span>
                <span className="feed__date">{dateOnly(r.release_date)}</span>
                <span className="feed__discovered">discovered {relativeTime(r.discovered_at)}</span>
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
