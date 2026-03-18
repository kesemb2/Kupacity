import React, { useState, useEffect, useCallback } from 'react';
import { fetchScrapeLogs, triggerScrape, triggerTicketScan, getDebugScreenshotUrl, getDebugScreenshotTicketsUrl, fetchDebugScreenshots, getDebugScreenshotFileUrl, clearDebugScreenshots, fetchBlockedSeatsStats } from '../api/client';

function ScrapePage() {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [scraping, setScraping] = useState(false);
  const [message, setMessage] = useState(null);

  const loadLogs = useCallback(() => {
    fetchScrapeLogs()
      .then(setLogs)
      .catch(() => setLogs([]))
      .finally(() => setLoading(false));
  }, []);

  // Find the running log (if any)
  const runningLog = logs.find((l) => l.status === 'running');

  useEffect(() => {
    loadLogs();
    const interval = setInterval(loadLogs, runningLog ? 3000 : 10000);
    return () => clearInterval(interval);
  }, [loadLogs, runningLog]);

  const handleTrigger = (chain) => {
    setScraping(true);
    setMessage(null);
    triggerScrape(chain)
      .then((res) => {
        setMessage(res.message || 'סריקה הופעלה');
        setTimeout(loadLogs, 2000);
      })
      .catch(() => setMessage('שגיאה בהפעלת הסריקה'))
      .finally(() => setScraping(false));
  };

  const handleTicketScan = (chain) => {
    setScraping(true);
    setMessage(null);
    triggerTicketScan(chain)
      .then((res) => {
        setMessage(res.message || 'סריקת כיסאות הופעלה');
        setTimeout(loadLogs, 2000);
      })
      .catch(() => setMessage('שגיאה בהפעלת סריקת הכיסאות'))
      .finally(() => setScraping(false));
  };

  const statusColor = (status) => {
    if (status === 'success') return '#22c55e';
    if (status === 'error' || status === 'failed') return '#ef4444';
    if (status === 'running') return '#3b82f6';
    return '#eab308';
  };

  const statusLabel = (status) => {
    if (status === 'running') return 'רץ...';
    return status;
  };

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען נתונים...</div>;
  }

  return (
    <div>
      {/* Trigger Section */}
      <div style={{
        background: 'linear-gradient(135deg, #1e3a5f 0%, #1e293b 100%)',
        borderRadius: 12,
        padding: '24px',
        border: '1px solid #334155',
        marginBottom: 24,
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <div>
            <h2 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>הפעלת סריקה</h2>
            <div style={{ color: '#94a3b8', fontSize: 14 }}>
              סורק את אתרי בתי הקולנוע ומעדכן סרטים, הקרנות וכרטיסים
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          <button
            onClick={() => handleTrigger()}
            disabled={scraping || !!runningLog}
            style={{
              ...triggerBtnStyle,
              background: (scraping || runningLog) ? '#475569' : '#3b82f6',
              opacity: (scraping || runningLog) ? 0.7 : 1,
              cursor: (scraping || runningLog) ? 'not-allowed' : 'pointer',
            }}
          >
            {(scraping || runningLog) ? 'רץ...' : 'סרוק הכל (במקביל)'}
          </button>
          <button
            onClick={() => handleTrigger('hot_cinema')}
            disabled={scraping || !!runningLog}
            style={{
              ...triggerBtnStyle,
              background: (scraping || runningLog) ? '#475569' : '#dc2626',
              opacity: (scraping || runningLog) ? 0.7 : 1,
              cursor: (scraping || runningLog) ? 'not-allowed' : 'pointer',
            }}
          >
            הוט סינמה בלבד
          </button>
          <button
            onClick={() => handleTrigger('movieland')}
            disabled={scraping || !!runningLog}
            style={{
              ...triggerBtnStyle,
              background: (scraping || runningLog) ? '#475569' : '#7c3aed',
              opacity: (scraping || runningLog) ? 0.7 : 1,
              cursor: (scraping || runningLog) ? 'not-allowed' : 'pointer',
            }}
          >
            מובילנד בלבד
          </button>
        </div>
        <div style={{ borderTop: '1px solid #475569', marginTop: 14, paddingTop: 14 }}>
          <div style={{ color: '#94a3b8', fontSize: 13, marginBottom: 10 }}>
            סריקת כיסאות בלבד (ללא סריקת סרטים/הקרנות)
          </div>
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <button
              onClick={() => handleTicketScan('hot_cinema')}
              disabled={scraping || !!runningLog}
              style={{
                ...triggerBtnStyle,
                background: (scraping || runningLog) ? '#475569' : '#b91c1c',
                opacity: (scraping || runningLog) ? 0.7 : 1,
                cursor: (scraping || runningLog) ? 'not-allowed' : 'pointer',
              }}
            >
              כיסאות הוט סינמה
            </button>
            <button
              onClick={() => handleTicketScan('movieland')}
              disabled={scraping || !!runningLog}
              style={{
                ...triggerBtnStyle,
                background: (scraping || runningLog) ? '#475569' : '#6d28d9',
                opacity: (scraping || runningLog) ? 0.7 : 1,
                cursor: (scraping || runningLog) ? 'not-allowed' : 'pointer',
              }}
            >
              כיסאות מובילנד
            </button>
          </div>
        </div>
      </div>

      {message && (
        <div style={{
          background: 'rgba(59,130,246,0.1)',
          border: '1px solid #3b82f6',
          borderRadius: 8,
          padding: '12px 16px',
          marginBottom: 16,
          color: '#93c5fd',
          fontSize: 14,
        }}>
          {message}
        </div>
      )}

      {/* Blocked Seats Stats */}
      <BlockedSeatsPanel />

      {/* Debug Screenshots */}
      <DebugScreenshotsGallery />

      {/* Live Progress Indicators - show all running logs */}
      {logs.filter(l => l.status === 'running' && l.progress).map((log) => (
        <ProgressCard key={log.id} progress={log.progress} chainName={log.chain_name} />
      ))}

      {/* Logs Table */}
      <div style={{
        background: '#1e293b',
        borderRadius: 12,
        border: '1px solid #334155',
        overflow: 'hidden',
      }}>
        <div style={{ padding: '16px 20px', borderBottom: '1px solid #334155' }}>
          <h3 style={{ fontSize: 16, fontWeight: 600 }}>לוגים אחרונים</h3>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                <th style={thStyle}>תאריך</th>
                <th style={thStyle}>רשת</th>
                <th style={thStyle}>סטטוס</th>
                <th style={thStyle}>סרטים</th>
                <th style={thStyle}>הקרנות</th>
                <th style={thStyle}>משך (שניות)</th>
                <th style={thStyle}>שגיאה</th>
              </tr>
            </thead>
            <tbody>
              {logs.length === 0 ? (
                <tr>
                  <td colSpan={7} style={{ textAlign: 'center', padding: 32, color: '#64748b' }}>
                    אין לוגים עדיין. הפעל סריקה ראשונה!
                  </td>
                </tr>
              ) : (
                logs.map((log) => (
                  <tr key={log.id} style={{ borderBottom: '1px solid #1e293b' }}>
                    <td style={tdStyle}>
                      {log.created_at ? new Date(log.created_at).toLocaleString('he-IL') : '-'}
                    </td>
                    <td style={tdStyle}>{log.chain_name}</td>
                    <td style={tdStyle}>
                      <span style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 6,
                        padding: '2px 10px',
                        borderRadius: 12,
                        fontSize: 12,
                        fontWeight: 600,
                        background: `${statusColor(log.status)}22`,
                        color: statusColor(log.status),
                      }}>
                        {log.status === 'running' && (
                          <span style={{
                            display: 'inline-block',
                            width: 6,
                            height: 6,
                            borderRadius: '50%',
                            background: '#3b82f6',
                            animation: 'pulse-dot 1.5s ease-in-out infinite',
                          }} />
                        )}
                        {statusLabel(log.status)}
                      </span>
                    </td>
                    <td style={tdStyle}>{log.movies_found ?? '-'}</td>
                    <td style={tdStyle}>{log.screenings_found ?? '-'}</td>
                    <td style={tdStyle}>{log.duration_seconds != null ? log.duration_seconds.toFixed(1) : '-'}</td>
                    <td style={{ ...tdStyle, color: '#ef4444', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {log.error_message || '-'}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pulse animation */}
      <style>{`
        @keyframes pulse-dot {
          0%, 100% { opacity: 1; transform: scale(1); }
          50% { opacity: 0.4; transform: scale(1.4); }
        }
        @keyframes progress-stripe {
          0% { background-position: 0 0; }
          100% { background-position: 40px 0; }
        }
      `}</style>
    </div>
  );
}


// ── Screenshot parsing & labels ──────────────────────────────────────────

const STEP_LABELS = {
  page: 'עמוד ראשי',
  branch: 'עמוד סניף',
  dropdown: 'תפריט סניפים',
  booking: 'דף הזמנה',
  step1_booking: 'דף הזמנה',
  step2_plus_click: 'אחרי +',
  step3_continue: 'אחרי המשך',
  step4_seat_map: 'מפת כיסאות',
  step4_seat_map_shortcut: 'מפת כיסאות (קיצור)',
  step5_annotated: 'כיסאות מסומנים',
  seats: 'מפת כיסאות',
};

const CHAIN_LABELS = {
  hot: 'הוט סינמה',
  mvl: 'מובילנד',
};

const CHAIN_COLORS = {
  hot: '#dc2626',
  mvl: '#7c3aed',
};

// All known step keywords (both single and compound)
const STEP_KEYWORDS_SINGLE = ['page', 'branch', 'dropdown', 'booking', 'seats'];
const STEP_KEYWORDS_COMPOUND = [
  'step1_booking', 'step2_plus_click', 'step3_continue',
  'step4_seat_map', 'step4_seat_map_shortcut', 'step5_annotated',
];

function parseScreenshotFilename(filename) {
  // Format: {chain}_{branch}_{movie}_{time}_{step}_{timestamp}.png
  // Timestamp is always 6 digits (HHMMSS) at the end before .png
  // Step is the second-to-last segment (or compound like step4_seat_map)
  const name = filename.replace('.png', '');
  const parts = name.split('_');

  let chain = 'unknown';
  let branch = '';
  let movie = '';
  let time = '';
  let step = '';

  if (parts[0] === 'hot' || parts[0] === 'mvl') {
    chain = parts[0];
  } else {
    // Legacy format - just return basic info
    return { chain: 'unknown', branch: '', movie: '', time: '', step: parts[0] || '' };
  }

  // Last part is always timestamp (6 digits)
  // Work backwards to find the step
  const lastPart = parts[parts.length - 1];
  let timestampIdx = parts.length - 1;
  if (/^\d{6}$/.test(lastPart)) {
    timestampIdx = parts.length - 1;
  }

  // Find the step by scanning from the end (before timestamp)
  let stepStartIdx = -1;
  let stepEndIdx = timestampIdx;

  // Try compound step names first (scan backwards for step patterns)
  for (let i = timestampIdx - 1; i >= 1; i--) {
    // Try 3-part compound: step4_seat_map_shortcut
    if (i >= 3) {
      const compound3 = parts[i - 2] + '_' + parts[i - 1] + '_' + parts[i];
      if (STEP_KEYWORDS_COMPOUND.includes(compound3)) {
        step = compound3;
        stepStartIdx = i - 2;
        break;
      }
    }
    // Try 2-part compound: step1_booking, step4_seat_map, etc.
    if (i >= 2) {
      const compound2 = parts[i - 1] + '_' + parts[i];
      if (STEP_KEYWORDS_COMPOUND.includes(compound2)) {
        step = compound2;
        stepStartIdx = i - 1;
        break;
      }
    }
    // Try single keyword
    if (STEP_KEYWORDS_SINGLE.includes(parts[i])) {
      step = parts[i];
      stepStartIdx = i;
      break;
    }
  }

  // Everything between chain (idx 0) and step is context
  // Context order: branch, movie parts..., time (4 digits)
  if (stepStartIdx > 1) {
    const contextParts = parts.slice(1, stepStartIdx);

    // Last context part might be a time (4 digits like 2030)
    if (contextParts.length > 0) {
      const lastCtx = contextParts[contextParts.length - 1];
      if (/^\d{4}$/.test(lastCtx)) {
        time = lastCtx.substring(0, 2) + ':' + lastCtx.substring(2);
        contextParts.pop();
      }
    }

    // First context part is branch, rest is movie
    if (contextParts.length >= 1) {
      branch = contextParts[0];
    }
    if (contextParts.length >= 2) {
      movie = contextParts.slice(1).join(' ');
    }
  } else if (stepStartIdx === 1) {
    // No context between chain and step
  } else {
    // Couldn't find step - treat everything except chain and timestamp as context
    const contextParts = parts.slice(1, timestampIdx);
    if (contextParts.length >= 1) {
      branch = contextParts[0];
    }
    if (contextParts.length >= 2) {
      movie = contextParts.slice(1).join(' ');
    }
  }

  return { chain, branch, movie, time, step };
}

function getStepLabel(step) {
  return STEP_LABELS[step] || step;
}


// ── Blocked Seats Learning Panel ─────────────────────────────────────────

function BlockedSeatsPanel() {
  const [expanded, setExpanded] = useState(false);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (expanded && !data) {
      setLoading(true);
      fetchBlockedSeatsStats()
        .then(setData)
        .catch(() => setData(null))
        .finally(() => setLoading(false));
    }
  }, [expanded, data]);

  const summary = data?.summary;
  const halls = data?.halls || [];

  return (
    <div style={{
      background: '#1e293b',
      borderRadius: 12,
      border: '1px solid #334155',
      marginBottom: 24,
      overflow: 'hidden',
    }}>
      <div
        onClick={() => setExpanded(!expanded)}
        style={{
          padding: '14px 20px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          cursor: 'pointer',
          userSelect: 'none',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: 16, fontWeight: 600 }}>
            מושבים חסומים - מצב למידה
          </span>
          {summary && (
            <span style={{
              background: summary.total_blocked_seats > 0 ? '#22c55e22' : '#eab30822',
              color: summary.total_blocked_seats > 0 ? '#22c55e' : '#eab308',
              padding: '2px 8px',
              borderRadius: 10,
              fontSize: 12,
              fontWeight: 600,
            }}>
              {summary.total_blocked_seats > 0
                ? `${summary.total_blocked_seats} חסומים`
                : `${summary.halls_tracked} אולמות במעקב`}
            </span>
          )}
        </div>
        <span style={{ color: '#64748b', fontSize: 18 }}>
          {expanded ? '\u25B2' : '\u25BC'}
        </span>
      </div>

      {expanded && (
        <div style={{ padding: '0 20px 16px' }}>
          {loading ? (
            <div style={{ color: '#64748b', fontSize: 14, textAlign: 'center', padding: 16 }}>
              טוען...
            </div>
          ) : !data || halls.length === 0 ? (
            <div style={{ color: '#64748b', fontSize: 14, textAlign: 'center', padding: 16 }}>
              אין נתונים עדיין. המערכת צריכה לפחות 2 סריקות כרטיסים כדי להתחיל לזהות מושבים חסומים.
            </div>
          ) : (
            <>
              {/* Summary cards */}
              <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap' }}>
                <div style={blockedStatStyle}>
                  <div style={{ fontSize: 22, fontWeight: 800, color: '#60a5fa' }}>{summary.halls_tracked}</div>
                  <div style={{ fontSize: 12, color: '#94a3b8' }}>אולמות במעקב</div>
                </div>
                <div style={blockedStatStyle}>
                  <div style={{ fontSize: 22, fontWeight: 800, color: '#f59e0b' }}>{summary.total_scans}</div>
                  <div style={{ fontSize: 12, color: '#94a3b8' }}>סריקות סה"כ</div>
                </div>
                <div style={blockedStatStyle}>
                  <div style={{ fontSize: 22, fontWeight: 800, color: summary.total_blocked_seats > 0 ? '#22c55e' : '#64748b' }}>
                    {summary.total_blocked_seats}
                  </div>
                  <div style={{ fontSize: 12, color: '#94a3b8' }}>מושבים חסומים</div>
                </div>
              </div>

              {/* Halls table */}
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                  <thead>
                    <tr style={{ borderBottom: '1px solid #334155' }}>
                      <th style={thStyle}>קולנוע</th>
                      <th style={thStyle}>עיר</th>
                      <th style={thStyle}>אולם</th>
                      <th style={thStyle}>סריקות</th>
                      <th style={thStyle}>מושבים במעקב</th>
                      <th style={thStyle}>חסומים</th>
                      <th style={thStyle}>עדכון אחרון</th>
                    </tr>
                  </thead>
                  <tbody>
                    {halls.map((h, i) => (
                      <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
                        <td style={tdStyle}>{h.cinema}</td>
                        <td style={tdStyle}>{h.city}</td>
                        <td style={tdStyle}>{h.hall}</td>
                        <td style={tdStyle}>
                          <span style={{
                            color: h.scan_count >= 2 ? '#22c55e' : '#eab308',
                            fontWeight: 600,
                          }}>
                            {h.scan_count}
                          </span>
                          {h.scan_count < 2 && (
                            <span style={{ color: '#64748b', fontSize: 11, marginRight: 4 }}>
                              (צריך {2 - h.scan_count} עוד)
                            </span>
                          )}
                        </td>
                        <td style={tdStyle}>{h.tracked_seats}</td>
                        <td style={tdStyle}>
                          <span style={{
                            fontWeight: 700,
                            color: h.blocked_count > 0 ? '#ef4444' : '#64748b',
                          }}>
                            {h.blocked_count}
                          </span>
                        </td>
                        <td style={{ ...tdStyle, fontSize: 12, color: '#64748b' }}>
                          {h.updated_at ? new Date(h.updated_at).toLocaleString('he-IL') : '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Refresh button */}
              <div style={{ marginTop: 12, textAlign: 'center' }}>
                <button
                  onClick={() => {
                    setLoading(true);
                    fetchBlockedSeatsStats()
                      .then(setData)
                      .catch(() => {})
                      .finally(() => setLoading(false));
                  }}
                  style={screenshotBtnStyle}
                >
                  רענן
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

const blockedStatStyle = {
  background: '#0f172a',
  borderRadius: 8,
  padding: '12px 20px',
  textAlign: 'center',
  flex: '1 1 100px',
  border: '1px solid #1e293b',
};


// ── Hierarchical gallery component ───────────────────────────────────────

function DebugScreenshotsGallery() {
  const [expanded, setExpanded] = useState(false);
  const [screenshots, setScreenshots] = useState([]);
  const [loading, setLoading] = useState(false);
  const [expandedChains, setExpandedChains] = useState({});
  const [expandedMovies, setExpandedMovies] = useState({});

  const loadScreenshots = useCallback(() => {
    setLoading(true);
    fetchDebugScreenshots()
      .then(setScreenshots)
      .catch(() => setScreenshots([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (expanded) loadScreenshots();
  }, [expanded, loadScreenshots]);

  const handleClear = () => {
    if (!window.confirm('למחוק את כל הסקרינשוטים?')) return;
    clearDebugScreenshots()
      .then(() => setScreenshots([]))
      .catch(() => {});
  };

  // Group screenshots into hierarchy: chain -> group -> items
  // Group by movie if available, otherwise by branch, otherwise "ניווט"
  const grouped = {};
  for (const s of screenshots) {
    const parsed = parseScreenshotFilename(s.filename);
    const chainKey = parsed.chain || 'unknown';

    // Build a descriptive group key
    let groupKey;
    if (parsed.movie) {
      // Has a movie — group by movie name (with branch as sub-info shown per item)
      groupKey = parsed.movie;
    } else if (parsed.branch && parsed.step && ['page', 'branch', 'dropdown'].includes(parsed.step)) {
      // Navigation screenshot — group under "ניווט" (navigation)
      groupKey = 'ניווט';
    } else if (parsed.branch) {
      groupKey = parsed.branch;
    } else {
      groupKey = 'ניווט';
    }

    if (!grouped[chainKey]) grouped[chainKey] = {};
    if (!grouped[chainKey][groupKey]) grouped[chainKey][groupKey] = [];
    grouped[chainKey][groupKey].push({ ...s, parsed });
  }

  const toggleChain = (chain) => {
    setExpandedChains(prev => ({ ...prev, [chain]: !prev[chain] }));
  };

  const toggleMovie = (key) => {
    setExpandedMovies(prev => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <div style={{
      background: '#1e293b',
      borderRadius: 12,
      border: '1px solid #334155',
      marginBottom: 24,
      overflow: 'hidden',
    }}>
      <div
        onClick={() => setExpanded(!expanded)}
        style={{
          padding: '14px 20px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          cursor: 'pointer',
          userSelect: 'none',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: 16, fontWeight: 600 }}>
            סקרינשוטים Debug
          </span>
          {screenshots.length > 0 && (
            <span style={{
              background: '#3b82f622',
              color: '#60a5fa',
              padding: '2px 8px',
              borderRadius: 10,
              fontSize: 12,
              fontWeight: 600,
            }}>
              {screenshots.length}
            </span>
          )}
        </div>
        <span style={{ color: '#64748b', fontSize: 18 }}>
          {expanded ? '\u25B2' : '\u25BC'}
        </span>
      </div>

      {expanded && (
        <div style={{ padding: '0 20px 16px' }}>
          {/* Action buttons */}
          <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
            <button
              onClick={() => window.open(getDebugScreenshotUrl(), '_blank')}
              style={screenshotBtnStyle}
            >
              אתר ראשי
            </button>
            <button
              onClick={() => window.open(getDebugScreenshotTicketsUrl(), '_blank')}
              style={screenshotBtnStyle}
            >
              כרטיסים (אחרון)
            </button>
            <button onClick={loadScreenshots} style={screenshotBtnStyle}>
              {loading ? '...' : 'רענן'}
            </button>
            {screenshots.length > 0 && (
              <button onClick={handleClear} style={{
                ...screenshotBtnStyle,
                background: 'rgba(239,68,68,0.15)',
                color: '#fca5a5',
                borderColor: '#7f1d1d',
              }}>
                נקה הכל
              </button>
            )}
          </div>

          {/* Hierarchical tree */}
          {screenshots.length === 0 ? (
            <div style={{ color: '#64748b', fontSize: 14, textAlign: 'center', padding: 16 }}>
              {loading ? 'טוען...' : 'אין סקרינשוטים. הרץ סריקה עם ticket updates.'}
            </div>
          ) : (
            Object.entries(grouped).map(([chainKey, movies]) => {
              const chainLabel = CHAIN_LABELS[chainKey] || chainKey;
              const chainColor = CHAIN_COLORS[chainKey] || '#64748b';
              const chainOpen = expandedChains[chainKey] !== false; // open by default
              const chainCount = Object.values(movies).reduce((sum, arr) => sum + arr.length, 0);

              return (
                <div key={chainKey} style={{ marginBottom: 8 }}>
                  {/* Chain header */}
                  <div
                    onClick={() => toggleChain(chainKey)}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 10,
                      padding: '10px 12px',
                      background: `${chainColor}15`,
                      borderRadius: 8,
                      cursor: 'pointer',
                      userSelect: 'none',
                      border: `1px solid ${chainColor}33`,
                    }}
                  >
                    <span style={{ color: '#64748b', fontSize: 14 }}>
                      {chainOpen ? '\u25BC' : '\u25B6'}
                    </span>
                    <span style={{
                      fontSize: 15,
                      fontWeight: 700,
                      color: chainColor,
                    }}>
                      {chainLabel}
                    </span>
                    <span style={{
                      background: `${chainColor}22`,
                      color: chainColor,
                      padding: '1px 8px',
                      borderRadius: 10,
                      fontSize: 11,
                      fontWeight: 600,
                    }}>
                      {chainCount}
                    </span>
                  </div>

                  {chainOpen && (
                    <div style={{ paddingRight: 16, marginTop: 4 }}>
                      {Object.entries(movies).map(([movieKey, items]) => {
                        const fullKey = `${chainKey}__${movieKey}`;
                        const movieOpen = expandedMovies[fullKey] !== false;

                        return (
                          <div key={fullKey} style={{ marginBottom: 4 }}>
                            {/* Movie header */}
                            <div
                              onClick={() => toggleMovie(fullKey)}
                              style={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: 8,
                                padding: '6px 10px',
                                cursor: 'pointer',
                                userSelect: 'none',
                                borderRadius: 6,
                              }}
                            >
                              <span style={{ color: '#475569', fontSize: 12 }}>
                                {movieOpen ? '\u25BC' : '\u25B6'}
                              </span>
                              <span style={{ fontSize: 14, fontWeight: 600, color: '#e2e8f0' }}>
                                {movieKey}
                              </span>
                              <span style={{
                                color: '#64748b',
                                fontSize: 12,
                              }}>
                                ({items.length})
                              </span>
                            </div>

                            {movieOpen && (
                              <div style={{
                                display: 'grid',
                                gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
                                gap: 8,
                                paddingRight: 20,
                                marginTop: 4,
                                marginBottom: 8,
                              }}>
                                {items.map((s) => (
                                  <div
                                    key={s.filename}
                                    title={s.filename}
                                    onClick={() => window.open(getDebugScreenshotFileUrl(s.filename), '_blank')}
                                    style={{
                                      background: '#0f172a',
                                      borderRadius: 8,
                                      padding: '10px 14px',
                                      cursor: 'pointer',
                                      border: '1px solid #1e293b',
                                      transition: 'border-color 0.2s',
                                    }}
                                    onMouseEnter={(e) => e.currentTarget.style.borderColor = chainColor}
                                    onMouseLeave={(e) => e.currentTarget.style.borderColor = '#1e293b'}
                                  >
                                    {/* Step label + time */}
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                                      <span style={{ fontSize: 13, fontWeight: 600, color: '#e2e8f0' }}>
                                        {getStepLabel(s.parsed.step)}
                                      </span>
                                      {s.parsed.time && (
                                        <span style={{
                                          fontSize: 13,
                                          fontWeight: 700,
                                          color: '#60a5fa',
                                          fontFamily: 'monospace',
                                        }}>
                                          {s.parsed.time}
                                        </span>
                                      )}
                                    </div>
                                    {/* Branch + movie info */}
                                    <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 4 }}>
                                      {s.parsed.branch && (
                                        <span style={{
                                          fontSize: 11,
                                          color: chainColor,
                                          background: `${chainColor}15`,
                                          padding: '1px 6px',
                                          borderRadius: 4,
                                          fontWeight: 600,
                                        }}>
                                          {s.parsed.branch}
                                        </span>
                                      )}
                                      {s.parsed.movie && (
                                        <span style={{
                                          fontSize: 11,
                                          color: '#94a3b8',
                                        }}>
                                          {s.parsed.movie}
                                        </span>
                                      )}
                                    </div>
                                    <div style={{ fontSize: 11, color: '#475569' }}>
                                      {s.size_kb} KB · {new Date(s.created_at * 1000).toLocaleTimeString('he-IL')}
                                    </div>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}


function ProgressCard({ progress, chainName }) {
  const { phase, current, total, detail } = progress;
  const pct = total > 0 ? Math.round((current / total) * 100) : null;

  return (
    <div style={{
      background: 'linear-gradient(135deg, #1e3a5f 0%, #172554 100%)',
      borderRadius: 12,
      padding: '20px 24px',
      border: '1px solid #2563eb44',
      marginBottom: 24,
    }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{
            display: 'inline-block',
            width: 10,
            height: 10,
            borderRadius: '50%',
            background: '#3b82f6',
            animation: 'pulse-dot 1.5s ease-in-out infinite',
          }} />
          <span style={{ fontSize: 16, fontWeight: 700, color: '#e2e8f0' }}>{phase}</span>
          {chainName && (
            <span style={{
              fontSize: 12,
              color: '#94a3b8',
              background: '#334155',
              padding: '2px 8px',
              borderRadius: 6,
            }}>
              {chainName}
            </span>
          )}
        </div>
        {pct !== null && (
          <span style={{ fontSize: 22, fontWeight: 800, color: '#60a5fa' }}>
            {pct}%
          </span>
        )}
      </div>

      {/* Progress bar */}
      {pct !== null && (
        <div style={{
          width: '100%',
          height: 12,
          borderRadius: 6,
          background: '#0f172a',
          overflow: 'hidden',
          marginBottom: 10,
        }}>
          <div style={{
            width: `${pct}%`,
            height: '100%',
            borderRadius: 6,
            background: 'linear-gradient(90deg, #2563eb, #3b82f6, #60a5fa)',
            backgroundSize: '40px 40px',
            backgroundImage: 'linear-gradient(45deg, rgba(255,255,255,0.1) 25%, transparent 25%, transparent 50%, rgba(255,255,255,0.1) 50%, rgba(255,255,255,0.1) 75%, transparent 75%, transparent)',
            animation: 'progress-stripe 1s linear infinite',
            transition: 'width 0.5s ease',
          }} />
        </div>
      )}

      {/* Detail line */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: 14, color: '#94a3b8' }}>
          {detail && <span style={{ color: '#cbd5e1' }}>{detail}</span>}
        </span>
        {total > 0 && (
          <span style={{ fontSize: 13, color: '#64748b' }}>
            {current} / {total}
          </span>
        )}
        {total === 0 && current > 0 && (
          <span style={{ fontSize: 13, color: '#64748b' }}>
            {current} עובדו
          </span>
        )}
      </div>
    </div>
  );
}


const triggerBtnStyle = {
  padding: '12px 24px',
  borderRadius: 8,
  border: 'none',
  fontSize: 15,
  fontWeight: 600,
  fontFamily: 'Heebo, sans-serif',
  color: '#fff',
  transition: 'all 0.2s',
};

const screenshotBtnStyle = {
  padding: '10px 20px',
  borderRadius: 8,
  border: '1px solid #334155',
  cursor: 'pointer',
  fontSize: 14,
  fontWeight: 600,
  fontFamily: 'Heebo, sans-serif',
  background: 'rgba(99,102,241,0.15)',
  color: '#a5b4fc',
  transition: 'all 0.2s',
};

const thStyle = {
  textAlign: 'right',
  padding: '10px 16px',
  color: '#94a3b8',
  fontWeight: 600,
  fontSize: 13,
  whiteSpace: 'nowrap',
};

const tdStyle = {
  padding: '10px 16px',
  color: '#e2e8f0',
  whiteSpace: 'nowrap',
};

export default ScrapePage;
