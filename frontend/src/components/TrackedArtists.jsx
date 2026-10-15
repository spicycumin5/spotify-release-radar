import StatusBlock from './StatusBlock.jsx'
import { relativeTime } from '../format.js'

export default function TrackedArtists({ artists, loading, error, apiUrl }) {
  const items = artists ?? []

  return (
    <section className="panel">
      <h2 className="panel__title">
        <span className="panel__title-tick panel__title-tick--amber" />
        Tracked artists
      </h2>

      <StatusBlock
        loading={loading}
        error={error}
        empty={items.length === 0}
        emptyText="No artists tracked yet. sync_followed_artists hasn't run."
        apiUrl={apiUrl}
      />

      {!loading && !error && items.length > 0 && (
        <ul className="artist-list">
          {items.map((a) => (
            <li key={a.artist_id} className="artist-list__row">
              <span className="artist-list__name">{a.artist_name}</span>
              <span className="artist-list__checked">checked {relativeTime(a.last_checked_at)}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
