const BARS = 14

export default function Meter({ active }) {
  return (
    <div className={`meter${active ? ' meter--active' : ''}`} role="presentation" aria-hidden="true">
      {Array.from({ length: BARS }).map((_, i) => (
        <span key={i} className="meter__bar" style={{ animationDelay: `${i * 70}ms` }} />
      ))}
    </div>
  )
}
