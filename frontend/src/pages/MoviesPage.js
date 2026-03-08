import React, { useState, useEffect } from 'react';
import { fetchMovies } from '../api/client';
import DataTable from '../components/DataTable';

const formatCurrency = (val) => `₪${(val || 0).toLocaleString()}`;
const formatNumber = (val) => (val || 0).toLocaleString();

function MoviesPage({ onMovieClick }) {
  const [movies, setMovies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [sortBy, setSortBy] = useState('total_revenue');

  useEffect(() => {
    fetchMovies().then(data => {
      setMovies(data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const filtered = movies
    .filter(m =>
      m.title.toLowerCase().includes(search.toLowerCase()) ||
      (m.title_he && m.title_he.includes(search))
    )
    .sort((a, b) => (b[sortBy] || 0) - (a[sortBy] || 0));

  const columns = [
    { header: '#', render: (_, idx) => idx + 1, align: 'center' },
    {
      header: 'סרט',
      render: (row) => (
        <div>
          <div style={{ fontWeight: 600 }}>{row.title_he || row.title}</div>
          {row.title_he && <div style={{ color: '#64748b', fontSize: 12 }}>{row.title}</div>}
        </div>
      ),
    },
    { header: 'ז\'אנר', key: 'genre' },
    { header: 'דירוג', key: 'rating' },
    { header: 'במאי', key: 'director' },
    { header: 'הקרנות', render: (row) => formatNumber(row.screenings_count), align: 'center' },
    { header: 'כרטיסים', render: (row) => formatNumber(row.total_tickets_sold), align: 'center' },
    {
      header: 'הכנסות',
      render: (row) => (
        <span style={{ color: '#10b981', fontWeight: 600 }}>
          {formatCurrency(row.total_revenue)}
        </span>
      ),
      align: 'left',
    },
    {
      header: 'תפוסה',
      render: (row) => (
        <div style={{
          background: '#334155',
          borderRadius: 4,
          overflow: 'hidden',
          width: 60,
          height: 8,
          display: 'inline-block',
          position: 'relative',
        }}>
          <div style={{
            background: row.avg_occupancy > 70 ? '#10b981' : row.avg_occupancy > 40 ? '#f59e0b' : '#ef4444',
            width: `${Math.min(100, row.avg_occupancy)}%`,
            height: '100%',
            borderRadius: 4,
          }} />
        </div>
      ),
      align: 'center',
    },
  ];

  // Fix the render with index
  const columnsWithIndex = columns.map(col => {
    if (col.header === '#') {
      return {
        ...col,
        render: (row) => filtered.indexOf(row) + 1,
      };
    }
    return col;
  });

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען סרטים...</div>;
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
        <h2 style={{ fontSize: 22, fontWeight: 700 }}>סרטים</h2>
        <div style={{ display: 'flex', gap: 12 }}>
          <input
            type="text"
            placeholder="חיפוש סרט..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            style={{
              background: '#1e293b',
              border: '1px solid #334155',
              borderRadius: 8,
              padding: '8px 16px',
              color: '#e2e8f0',
              fontSize: 14,
              fontFamily: 'Heebo, sans-serif',
              outline: 'none',
              width: 220,
            }}
          />
          <select
            value={sortBy}
            onChange={e => setSortBy(e.target.value)}
            style={{
              background: '#1e293b',
              border: '1px solid #334155',
              borderRadius: 8,
              padding: '8px 16px',
              color: '#e2e8f0',
              fontSize: 14,
              fontFamily: 'Heebo, sans-serif',
              outline: 'none',
            }}
          >
            <option value="total_revenue">מיון: הכנסות</option>
            <option value="total_tickets_sold">מיון: כרטיסים</option>
            <option value="screenings_count">מיון: הקרנות</option>
            <option value="avg_occupancy">מיון: תפוסה</option>
          </select>
        </div>
      </div>

      <DataTable
        columns={columnsWithIndex}
        data={filtered}
        onRowClick={(row) => onMovieClick(row.id)}
      />
    </div>
  );
}

export default MoviesPage;
