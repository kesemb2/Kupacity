import React, { useState, useEffect, useCallback } from 'react';
import { fetchAllowedMovies, addAllowedMovie, updateAllowedMovie, deleteAllowedMovie, fetchUnmatchedTitles } from '../api/client';

function AdminPage() {
  const [allowedMovies, setAllowedMovies] = useState([]);
  const [unmatchedMovies, setUnmatchedMovies] = useState([]);
  const [newTitle, setNewTitle] = useState('');
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [message, setMessage] = useState(null);
  const [showUnmatched, setShowUnmatched] = useState(false);

  const loadData = useCallback(() => {
    Promise.all([
      fetchAllowedMovies().catch(() => []),
      fetchUnmatchedTitles().catch(() => []),
    ]).then(([allowed, unmatched]) => {
      setAllowedMovies(allowed);
      setUnmatchedMovies(unmatched);
      setLoading(false);
    });
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleAdd = (title) => {
    const t = title || newTitle;
    if (!t.trim()) return;
    setAdding(true);
    setMessage(null);
    addAllowedMovie(t.trim())
      .then((res) => {
        if (res.error) {
          setMessage({ type: 'error', text: res.error + (res.existing ? ` (${res.existing})` : '') });
        } else {
          setMessage({ type: 'success', text: `"${res.title}" נוסף לרשימה` });
          setNewTitle('');
          loadData();
        }
      })
      .catch(() => setMessage({ type: 'error', text: 'שגיאה בהוספת הסרט' }))
      .finally(() => setAdding(false));
  };

  const handleToggle = (id, currentActive) => {
    updateAllowedMovie(id, !currentActive)
      .then(() => loadData())
      .catch(() => setMessage({ type: 'error', text: 'שגיאה בעדכון' }));
  };

  const handleDelete = (id, title) => {
    if (!window.confirm(`למחוק את "${title}" מהרשימה?`)) return;
    deleteAllowedMovie(id)
      .then(() => {
        setMessage({ type: 'success', text: `"${title}" הוסר מהרשימה` });
        loadData();
      })
      .catch(() => setMessage({ type: 'error', text: 'שגיאה במחיקה' }));
  };

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען נתונים...</div>;
  }

  return (
    <div>
      {/* Header */}
      <div style={{
        background: 'linear-gradient(135deg, #1e3a5f 0%, #1e293b 100%)',
        borderRadius: 12,
        padding: '24px',
        border: '1px solid #334155',
        marginBottom: 24,
      }}>
        <h2 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>ניהול סרטים</h2>
        <div style={{ color: '#94a3b8', fontSize: 14 }}>
          הגדר אילו סרטים המערכת עוקבת אחריהם. רק סרטים ברשימה יקבלו נתוני כרטיסים.
          כאשר הרשימה ריקה, כל הסרטים עוברים ללא סינון.
        </div>
      </div>

      {/* Add Movie Form */}
      <div style={{
        background: '#1e293b',
        borderRadius: 12,
        padding: '20px 24px',
        border: '1px solid #334155',
        marginBottom: 24,
      }}>
        <h3 style={{ fontSize: 16, fontWeight: 600, marginBottom: 12 }}>הוספת סרט</h3>
        <div style={{ display: 'flex', gap: 10 }}>
          <input
            type="text"
            value={newTitle}
            onChange={(e) => setNewTitle(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
            placeholder="שם הסרט (לדוגמא: צעקה 7)"
            style={{
              flex: 1,
              padding: '10px 16px',
              borderRadius: 8,
              border: '1px solid #475569',
              background: '#0f172a',
              color: '#e2e8f0',
              fontSize: 15,
              fontFamily: 'Heebo, sans-serif',
              outline: 'none',
            }}
          />
          <button
            onClick={() => handleAdd()}
            disabled={adding || !newTitle.trim()}
            style={{
              ...btnStyle,
              background: (adding || !newTitle.trim()) ? '#475569' : '#22c55e',
              opacity: (adding || !newTitle.trim()) ? 0.7 : 1,
              cursor: (adding || !newTitle.trim()) ? 'not-allowed' : 'pointer',
            }}
          >
            {adding ? 'מוסיף...' : '+ הוסף סרט'}
          </button>
        </div>
      </div>

      {/* Message */}
      {message && (
        <div style={{
          background: message.type === 'error' ? 'rgba(239,68,68,0.1)' : 'rgba(34,197,94,0.1)',
          border: `1px solid ${message.type === 'error' ? '#ef4444' : '#22c55e'}`,
          borderRadius: 8,
          padding: '12px 16px',
          marginBottom: 16,
          color: message.type === 'error' ? '#fca5a5' : '#86efac',
          fontSize: 14,
        }}>
          {message.text}
        </div>
      )}

      {/* Allowed Movies List */}
      <div style={{
        background: '#1e293b',
        borderRadius: 12,
        border: '1px solid #334155',
        marginBottom: 24,
        overflow: 'hidden',
      }}>
        <div style={{
          padding: '16px 24px',
          borderBottom: '1px solid #334155',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}>
          <h3 style={{ fontSize: 16, fontWeight: 600, margin: 0 }}>
            סרטים מאושרים ({allowedMovies.length})
          </h3>
          {allowedMovies.length === 0 && (
            <span style={{ fontSize: 13, color: '#eab308' }}>
              הרשימה ריקה — כל הסרטים עוברים ללא סינון
            </span>
          )}
        </div>

        {allowedMovies.length > 0 ? (
          <div>
            {/* Table Header */}
            <div style={{
              display: 'grid',
              gridTemplateColumns: '1fr 1fr 100px 100px 80px',
              gap: 12,
              padding: '10px 24px',
              background: '#0f172a',
              fontSize: 12,
              fontWeight: 600,
              color: '#64748b',
              textTransform: 'uppercase',
            }}>
              <span>שם הסרט</span>
              <span>שם מנורמל</span>
              <span>הקרנות</span>
              <span>סטטוס</span>
              <span></span>
            </div>
            {/* Table Rows */}
            {allowedMovies.map((m) => (
              <div key={m.id} style={{
                display: 'grid',
                gridTemplateColumns: '1fr 1fr 100px 100px 80px',
                gap: 12,
                padding: '12px 24px',
                borderBottom: '1px solid #1e293b',
                alignItems: 'center',
                opacity: m.is_active ? 1 : 0.5,
              }}>
                <span style={{ fontSize: 14, fontWeight: 600, color: '#e2e8f0' }}>
                  {m.title}
                </span>
                <span style={{ fontSize: 13, color: '#94a3b8', direction: 'ltr', textAlign: 'right' }}>
                  {m.title_normalized}
                </span>
                <span style={{ fontSize: 13 }}>
                  {m.has_data ? (
                    <span style={{ color: '#22c55e' }}>{m.screening_count} הקרנות</span>
                  ) : (
                    <span style={{ color: '#64748b' }}>אין נתונים</span>
                  )}
                </span>
                <button
                  onClick={() => handleToggle(m.id, m.is_active)}
                  style={{
                    background: m.is_active ? 'rgba(34,197,94,0.15)' : 'rgba(239,68,68,0.15)',
                    color: m.is_active ? '#22c55e' : '#ef4444',
                    border: 'none',
                    borderRadius: 6,
                    padding: '4px 12px',
                    fontSize: 12,
                    fontWeight: 600,
                    fontFamily: 'Heebo, sans-serif',
                    cursor: 'pointer',
                  }}
                >
                  {m.is_active ? 'פעיל' : 'מושבת'}
                </button>
                <button
                  onClick={() => handleDelete(m.id, m.title)}
                  style={{
                    background: 'rgba(239,68,68,0.1)',
                    color: '#ef4444',
                    border: 'none',
                    borderRadius: 6,
                    padding: '4px 12px',
                    fontSize: 12,
                    fontWeight: 600,
                    fontFamily: 'Heebo, sans-serif',
                    cursor: 'pointer',
                  }}
                >
                  מחק
                </button>
              </div>
            ))}
          </div>
        ) : (
          <div style={{ padding: '32px 24px', textAlign: 'center', color: '#64748b' }}>
            לא נוספו סרטים עדיין. הוסף סרט למעלה כדי להתחיל לסנן.
          </div>
        )}
      </div>

      {/* Unmatched Movies */}
      {unmatchedMovies.length > 0 && (
        <div style={{
          background: '#1e293b',
          borderRadius: 12,
          border: '1px solid #334155',
          overflow: 'hidden',
        }}>
          <button
            onClick={() => setShowUnmatched(!showUnmatched)}
            style={{
              width: '100%',
              padding: '16px 24px',
              background: 'transparent',
              border: 'none',
              borderBottom: showUnmatched ? '1px solid #334155' : 'none',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              cursor: 'pointer',
              color: '#e2e8f0',
              fontFamily: 'Heebo, sans-serif',
            }}
          >
            <h3 style={{ fontSize: 16, fontWeight: 600, margin: 0 }}>
              סרטים לא מזוהים ({unmatchedMovies.length})
            </h3>
            <span style={{ color: '#64748b', fontSize: 20 }}>
              {showUnmatched ? '−' : '+'}
            </span>
          </button>

          {showUnmatched && (
            <div>
              <div style={{ padding: '8px 24px', color: '#94a3b8', fontSize: 13 }}>
                סרטים שנסרקו מאתרי הקולנוע אבל לא תואמים לרשימה המאושרת.
                לחץ "הוסף" כדי להוסיף אותם.
              </div>
              {unmatchedMovies.map((m) => (
                <div key={m.id} style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '10px 24px',
                  borderBottom: '1px solid #1e293b',
                }}>
                  <div>
                    <span style={{ fontSize: 14, color: '#e2e8f0' }}>{m.title}</span>
                    {m.screening_count > 0 && (
                      <span style={{ fontSize: 12, color: '#64748b', marginRight: 8 }}>
                        ({m.screening_count} הקרנות)
                      </span>
                    )}
                  </div>
                  <button
                    onClick={() => handleAdd(m.title)}
                    style={{
                      background: 'rgba(59,130,246,0.15)',
                      color: '#60a5fa',
                      border: 'none',
                      borderRadius: 6,
                      padding: '4px 16px',
                      fontSize: 12,
                      fontWeight: 600,
                      fontFamily: 'Heebo, sans-serif',
                      cursor: 'pointer',
                    }}
                  >
                    הוסף
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

const btnStyle = {
  padding: '10px 24px',
  borderRadius: 8,
  border: 'none',
  fontSize: 15,
  fontWeight: 600,
  fontFamily: 'Heebo, sans-serif',
  color: '#fff',
  transition: 'all 0.2s',
};

export default AdminPage;
