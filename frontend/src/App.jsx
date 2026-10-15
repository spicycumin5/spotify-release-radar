import { useEffect, useRef, useState } from 'react'
import { api } from './api.js'
import Masthead from './components/Masthead.jsx'
import ReleasesFeed from './components/ReleasesFeed.jsx'
import TrackedArtists from './components/TrackedArtists.jsx'
import AlertLog from './components/AlertLog.jsx'
import './App.css'

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

function useResource(fetcher, refreshKey) {
  const [state, setState] = useState({ data: null, loading: true, error: null })

  useEffect(() => {
    let cancelled = false
    setState((s) => ({ ...s, loading: true, error: null }))
    fetcher()
      .then((data) => {
        if (!cancelled) setState({ data, loading: false, error: null })
      })
      .catch((err) => {
        if (!cancelled) setState({ data: null, loading: false, error: err.message })
      })
    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey])

  return state
}

export default function App() {
  const [refreshKey, setRefreshKey] = useState(0)
  const [lastUpdated, setLastUpdated] = useState(null)
  const wasLoading = useRef(true)

  const releases = useResource(() => api.releases(50), refreshKey)
  const artists = useResource(() => api.artists(), refreshKey)
  const alerts = useResource(() => api.alerts(50), refreshKey)

  const anyLoading = releases.loading || artists.loading || alerts.loading
  const allFailed = releases.error && artists.error && alerts.error

  useEffect(() => {
    if (wasLoading.current && !anyLoading) setLastUpdated(new Date())
    wasLoading.current = anyLoading
  }, [anyLoading])

  return (
    <div className="app">
      <Masthead
        live={!anyLoading && !allFailed}
        refreshing={anyLoading}
        onRefresh={() => setRefreshKey((k) => k + 1)}
        lastUpdated={lastUpdated}
      />

      <main className="layout">
        <ReleasesFeed
          releases={releases.data}
          loading={releases.loading}
          error={releases.error}
          apiUrl={API_URL}
        />

        <div className="layout__side">
          <TrackedArtists
            artists={artists.data}
            loading={artists.loading}
            error={artists.error}
            apiUrl={API_URL}
          />
          <AlertLog alerts={alerts.data} loading={alerts.loading} error={alerts.error} apiUrl={API_URL} />
        </div>
      </main>
    </div>
  )
}
