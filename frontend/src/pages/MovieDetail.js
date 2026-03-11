import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line,
} from 'recharts';
import { fetchMovieDetail } from '../api/client';
import ChartCard from '../components/ChartCard';
import DataTable from '../components/DataTable';

const formatNumber = (val) => (val || 0).toLocaleString();

function MovieDetail({ movieId, onBack }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (movieId) {
      fetchMovieDetail(movieId).then(d => {
        setData(d);
        setLoading(false);
      }).catch(() => setLoading(false));
    }
  }, [movieId]);

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען פרטי סרט...</div>;
  }

  if (!data || !data.movie) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#ef4444' }}>סרט לא נמצא</div>;
  }

  const { movie, by_cinema, by_date, screenings } = data;

  const totalTickets = by_cinema.reduce((s, c) => s + c.tickets_sold, 0);
  const totalScreenings = by_cinema.reduce((s, c) => s + c.screenings, 0);

  const cinemaColumns = [
    { header: 'סניף', key: 'cinema' },
    { header: 'עיר', key: 'city' },
    { header: 'הקרנות', render: r => formatNumber(r.screenings), align: 'center' },
    {
      header: 'כרטיסים',
      render: r => <span style={{ color: '#3b82f6', fontWeight: 600 }}>{formatNumber(r.tickets_sold)}</span>,
      align: 'center',
    },
  ];

  return (
    <div>
      <button
        onClick={onBack}
        style={{
          background: 'none',
          border: 'none',
          color: '#3b82f6',
          cursor: 'pointer',
          fontSize: 14,
          fontFamily: 'Heebo, sans-serif',
          marginBottom: 16,
          padding: 0,
        }}
      >
        ← חזרה לרשימת הסרטים
      </button>

      {/* Movie Header */}
      <div style={{
        background: 'linear-gradient(135deg, #1e3a5f 0%, #1e293b 100%)',
        borderRadius: 12,
        padding: '24px 32px',
        border: '1px solid #334155',
        marginBottom: 24,
      }}>
        <h2 style={{ fontSize: 26, fontWeight: 700, marginBottom: 4 }}>
          {movie.title_he || movie.title}
        </h2>
        {movie.title_he && (
          <div style={{ color: '#94a3b8', fontSize: 16, marginBottom: 12 }}>{movie.title}</div>
        )}
        <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap', color: '#94a3b8', fontSize: 14 }}>
          {movie.director && <span>במאי: {movie.director}</span>}
          {movie.genre && <span>ז'אנר: {movie.genre}</span>}
          {movie.duration_minutes > 0 && <span>אורך: {movie.duration_minutes} דקות</span>}
          {movie.rating && <span>דירוג: {movie.rating}</span>}
          {movie.release_date && <span>תאריך יציאה: {movie.release_date}</span>}
        </div>
        <div style={{ display: 'flex', gap: 40, marginTop: 16 }}>
          <div>
            <div style={{ color: '#94a3b8', fontSize: 13 }}>כרטיסים</div>
            <div style={{ color: '#3b82f6', fontSize: 24, fontWeight: 700 }}>{formatNumber(totalTickets)}</div>
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: 13 }}>הקרנות</div>
            <div style={{ color: '#f59e0b', fontSize: 24, fontWeight: 700 }}>{formatNumber(totalScreenings)}</div>
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: 13 }}>סניפים</div>
            <div style={{ color: '#8b5cf6', fontSize: 24, fontWeight: 700 }}>{by_cinema.length}</div>
          </div>
        </div>
      </div>

      {/* Charts */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <ChartCard title="כרטיסים יומיים">
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={by_date}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="date" tick={{ fill: '#94a3b8', fontSize: 11 }}
                     tickFormatter={d => d ? d.slice(5) : ''} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }}
                     tickFormatter={v => v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v} />
              <Tooltip
                contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
                formatter={(v) => [formatNumber(v), '']}
              />
              <Line type="monotone" dataKey="tickets_sold" stroke="#3b82f6" strokeWidth={2.5}
                    dot={{ fill: '#3b82f6', r: 4 }} name="כרטיסים" />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="כרטיסים לפי סניף (טופ 10)">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={by_cinema.slice(0, 10)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }}
                     tickFormatter={v => v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v} />
              <YAxis type="category" dataKey="cinema" width={160}
                     tick={{ fill: '#e2e8f0', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
                formatter={(v) => formatNumber(v)}
              />
              <Bar dataKey="tickets_sold" fill="#3b82f6" radius={[0, 4, 4, 0]} name="כרטיסים" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Cinema Table */}
      <h3 style={{ fontSize: 18, fontWeight: 600, marginBottom: 12 }}>פירוט לפי סניפים</h3>
      <DataTable columns={cinemaColumns} data={by_cinema} />

      {/* Individual Screenings Table */}
      {screenings && screenings.length > 0 && (
        <>
          <h3 style={{ fontSize: 18, fontWeight: 600, marginBottom: 12, marginTop: 24 }}>פירוט הקרנות</h3>
          <DataTable
            columns={[
              {
                header: 'תאריך',
                render: r => {
                  if (!r.showtime) return '-';
                  const d = new Date(r.showtime);
                  return d.toLocaleDateString('he-IL', { day: '2-digit', month: '2-digit' });
                },
                align: 'center',
              },
              {
                header: 'שעה',
                render: r => {
                  if (!r.showtime) return '-';
                  const d = new Date(r.showtime);
                  return d.toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit' });
                },
                align: 'center',
              },
              { header: 'סניף', key: 'cinema' },
              { header: 'עיר', key: 'city' },
              { header: 'אולם', key: 'hall', align: 'center' },
              { header: 'פורמט', key: 'format', align: 'center' },
              {
                header: 'סה"כ מקומות',
                render: r => r.total_seats > 0
                  ? formatNumber(r.total_seats)
                  : <span style={{ color: '#64748b' }}>—</span>,
                align: 'center',
              },
              {
                header: 'נמכרו',
                render: r => r.total_seats > 0
                  ? (
                    <span style={{ color: r.tickets_sold > 0 ? '#f59e0b' : '#64748b', fontWeight: 600 }}>
                      {formatNumber(r.tickets_sold)}
                    </span>
                  )
                  : <span style={{ color: '#64748b' }}>—</span>,
                align: 'center',
              },
              {
                header: 'תפוסה',
                render: r => {
                  if (!r.total_seats || r.total_seats === 0) {
                    return <span style={{ color: '#64748b', fontSize: 12 }}>—</span>;
                  }
                  const occ = r.occupancy || 0;
                  const color = occ > 75 ? '#ef4444' : occ > 40 ? '#f59e0b' : '#22c55e';
                  return (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'center' }}>
                      <div style={{
                        width: 50, height: 6, background: '#334155', borderRadius: 3, overflow: 'hidden',
                      }}>
                        <div style={{ width: `${Math.min(occ, 100)}%`, height: '100%', background: color, borderRadius: 3 }} />
                      </div>
                      <span style={{ color, fontSize: 12, fontWeight: 600 }}>{occ}%</span>
                    </div>
                  );
                },
                align: 'center',
              },
            ]}
            data={screenings}
          />
        </>
      )}
    </div>
  );
}

export default MovieDetail;
