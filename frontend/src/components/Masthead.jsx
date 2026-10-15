import Meter from './Meter.jsx'
import { relativeTime } from '../format.js'

export default function Masthead({ live, onRefresh, refreshing, lastUpdated }) {
  return (
    <header className="masthead">
      <div className="masthead__id">
        <span className={`dot${live ? ' dot--live' : ''}`} />
        <div>
          <h1>Release Radar</h1>
          <p className="masthead__subtitle">spotify pipeline monitor</p>
        </div>
      </div>

      <Meter active={refreshing} />

      <div className="masthead__controls">
        <span className="masthead__updated">
          {lastUpdated ? `updated ${relativeTime(lastUpdated)}` : 'connecting…'}
        </span>
        <button className="refresh-btn" onClick={onRefresh} disabled={refreshing}>
          {refreshing ? 'Refreshing…' : 'Refresh'}
        </button>
      </div>
    </header>
  )
}
