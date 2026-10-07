const BASE_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

async function get(path) {
  const res = await fetch(`${BASE_URL}${path}`)
  if (!res.ok) {
    throw new Error(`${path} responded ${res.status}`)
  }
  return res.json()
}

export const api = {
  releases: (limit = 50) => get(`/releases?limit=${limit}`),
  artists: () => get('/artists'),
  alerts: (limit = 50) => get(`/alerts?limit=${limit}`),
}
