import React, { useState, useEffect } from 'react';
import './index.css';
import Dashboard from './pages/Dashboard';
import MoviesPage from './pages/MoviesPage';
import CinemasPage from './pages/CinemasPage';
import CitiesPage from './pages/CitiesPage';
import MovieDetail from './pages/MovieDetail';
import ScrapePage from './pages/ScrapePage';
import { checkHealth } from './api/client';

const NAV_ITEMS = [
  { key: 'dashboard', label: 'דאשבורד' },
  { key: 'movies', label: 'סרטים' },
  { key: 'cinemas', label: 'סניפים' },
  { key: 'cities', label: 'ערים' },
  { key: 'scrape', label: 'סריקה' },
];

function App() {
  const [page, setPage] = useState('dashboard');
  const [selectedMovieId, setSelectedMovieId] = useState(null);
  const [backendStatus, setBackendStatus] = useState('checking'); // 'checking' | 'online' | 'offline'

  useEffect(() => {
    const check = () => {
      checkHealth().then(({ ok }) => setBackendStatus(ok ? 'online' : 'offline'));
    };
    check();
    const interval = setInterval(check, 30000);
    return () => clearInterval(interval);
  }, []);

  const navigateToMovie = (id) => {
    setSelectedMovieId(id);
    setPage('movieDetail');
  };

  const renderPage = () => {
    switch (page) {
      case 'dashboard':
        return <Dashboard onMovieClick={navigateToMovie} />;
      case 'movies':
        return <MoviesPage onMovieClick={navigateToMovie} />;
      case 'cinemas':
        return <CinemasPage />;
      case 'cities':
        return <CitiesPage />;
      case 'scrape':
        return <ScrapePage />;
      case 'movieDetail':
        return <MovieDetail movieId={selectedMovieId} onBack={() => setPage('movies')} />;
      default:
        return <Dashboard onMovieClick={navigateToMovie} />;
    }
  };

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <header style={{
        background: 'linear-gradient(135deg, #1e293b 0%, #0f172a 100%)',
        borderBottom: '1px solid #334155',
        padding: '0 24px',
        position: 'sticky',
        top: 0,
        zIndex: 100,
      }}>
        <div style={{
          maxWidth: 1400,
          margin: '0 auto',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          height: 64,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span style={{ fontSize: 28 }}>🎬</span>
            <h1 style={{
              fontSize: 20,
              fontWeight: 700,
              background: 'linear-gradient(135deg, #60a5fa, #a78bfa)',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent',
            }}>
              הוט סינמה - כרטיסים
            </h1>
          </div>

          <nav style={{ display: 'flex', gap: 4 }}>
            {NAV_ITEMS.map(item => (
              <button
                key={item.key}
                onClick={() => setPage(item.key)}
                style={{
                  padding: '8px 20px',
                  borderRadius: 8,
                  border: 'none',
                  cursor: 'pointer',
                  fontSize: 14,
                  fontWeight: 500,
                  fontFamily: 'Heebo, sans-serif',
                  transition: 'all 0.2s',
                  background: page === item.key ? '#3b82f6' : 'transparent',
                  color: page === item.key ? '#fff' : '#94a3b8',
                }}
              >
                {item.label}
              </button>
            ))}
          </nav>
        </div>
      </header>

      {/* Main */}
      <main style={{
        flex: 1,
        maxWidth: 1400,
        margin: '0 auto',
        padding: '24px',
        width: '100%',
      }}>
        {renderPage()}
      </main>

      {/* Footer */}
      <footer style={{
        textAlign: 'center',
        padding: '16px',
        color: '#64748b',
        fontSize: 13,
        borderTop: '1px solid #1e293b',
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        gap: 16,
      }}>
        <span>הוט סינמה • נתוני כרטיסים מתעדכנים אוטומטית</span>
        <span style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 6,
          padding: '4px 12px',
          borderRadius: 20,
          fontSize: 12,
          fontWeight: 500,
          background: backendStatus === 'online' ? 'rgba(34,197,94,0.15)' :
                      backendStatus === 'offline' ? 'rgba(239,68,68,0.15)' :
                      'rgba(234,179,8,0.15)',
          color: backendStatus === 'online' ? '#22c55e' :
                 backendStatus === 'offline' ? '#ef4444' :
                 '#eab308',
        }}>
          <span style={{
            width: 8,
            height: 8,
            borderRadius: '50%',
            background: backendStatus === 'online' ? '#22c55e' :
                        backendStatus === 'offline' ? '#ef4444' :
                        '#eab308',
            animation: backendStatus === 'online' ? 'none' : 'pulse 1.5s infinite',
          }} />
          {backendStatus === 'online' ? 'שרת מחובר' :
           backendStatus === 'offline' ? 'שרת מנותק' :
           'בודק חיבור...'}
        </span>
      </footer>
    </div>
  );
}

export default App;
