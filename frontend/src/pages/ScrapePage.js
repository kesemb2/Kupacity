import React, { useState, useEffect, useCallback } from 'react';
import { fetchScrapeLogs, triggerScrape } from '../api/client';

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

  useEffect(() => {
    loadLogs();
    const interval = setInterval(loadLogs, 10000);
    return () => clearInterval(interval);
  }, [loadLogs]);

  const handleTrigger = () => {
    setScraping(true);
    setMessage(null);
    triggerScrape()
      .then((res) => {
        setMessage(res.message || 'סריקה הופעלה');
        setTimeout(loadLogs, 3000);
      })
      .catch(() => setMessage('שגיאה בהפעלת הסריקה'))
      .finally(() => setScraping(false));
  };

  const statusColor = (status) => {
    if (status === 'success') return '#22c55e';
    if (status === 'error' || status === 'failed') return '#ef4444';
    return '#eab308';
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
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <div>
          <h2 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>הפעלת סריקה</h2>
          <div style={{ color: '#94a3b8', fontSize: 14 }}>
            סורק את אתר הוט סינמה ומעדכן סרטים, הקרנות וכרטיסים
          </div>
        </div>
        <button
          onClick={handleTrigger}
          disabled={scraping}
          style={{
            padding: '12px 32px',
            borderRadius: 8,
            border: 'none',
            cursor: scraping ? 'not-allowed' : 'pointer',
            fontSize: 15,
            fontWeight: 600,
            fontFamily: 'Heebo, sans-serif',
            background: scraping ? '#475569' : '#3b82f6',
            color: '#fff',
            transition: 'all 0.2s',
            opacity: scraping ? 0.7 : 1,
          }}
        >
          {scraping ? 'מפעיל...' : 'הפעל סריקה'}
        </button>
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
                        display: 'inline-block',
                        padding: '2px 10px',
                        borderRadius: 12,
                        fontSize: 12,
                        fontWeight: 600,
                        background: `${statusColor(log.status)}22`,
                        color: statusColor(log.status),
                      }}>
                        {log.status}
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
    </div>
  );
}

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
