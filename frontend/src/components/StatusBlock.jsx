export default function StatusBlock({ loading, error, empty, emptyText, apiUrl }) {
  if (loading) return <p className="status status--muted">Scanning…</p>
  if (error) {
    return (
      <p className="status status--error">
        Couldn't load data from <code>{apiUrl}</code>. Make sure{' '}
        <code>uvicorn api.main:app --reload --port 8000</code> is running and Postgres is reachable.
      </p>
    )
  }
  if (empty) return <p className="status status--muted">{emptyText}</p>
  return null
}
