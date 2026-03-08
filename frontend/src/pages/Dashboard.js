import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, LineChart, Line, Legend,
} from 'recharts';
import {
  fetchDashboardSummary, fetchRevenueByDate, fetchTopMovies,
  fetchRevenueByChain, fetchOccupancyByFormat,
} from '../api/client';
import StatCard from '../components/StatCard';
import ChartCard from '../components/ChartCard';

const COLORS = ['#3b82f6', '#8b5cf6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444', '#ec4899', '#6366f1'];

const formatCurrency = (val) => `₪${(val || 0).toLocaleString()}`;
const formatNumber = (val) => (val || 0).toLocaleString();

function Dashboard({ onMovieClick }) {
  const [summary, setSummary] = useState(null);
  const [revenueByDate, setRevenueByDate] = useState([]);
  const [topMovies, setTopMovies] = useState([]);
  const [chainData, setChainData] = useState([]);
  const [formatData, setFormatData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetchDashboardSummary(),
      fetchRevenueByDate(14),
      fetchTopMovies(8),
      fetchRevenueByChain(),
      fetchOccupancyByFormat(),
    ]).then(([s, rd, tm, cd, fd]) => {
      setSummary(s);
      setRevenueByDate(rd);
      setTopMovies(tm);
      setChainData(cd);
      setFormatData(fd);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען נתונים...</div>;
  }

  return (
    <div>
      {/* Stats Row */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 24 }}>
        <StatCard title="סה״כ הכנסות" value={formatCurrency(summary?.total_revenue)} icon="💰" color="#10b981" />
        <StatCard title="כרטיסים שנמכרו" value={formatNumber(summary?.total_tickets_sold)} icon="🎟️" color="#3b82f6" />
        <StatCard title="סרטים" value={summary?.total_movies} icon="🎬" color="#8b5cf6" />
        <StatCard title="הקרנות" value={formatNumber(summary?.total_screenings)} icon="📽️" color="#f59e0b" />
        <StatCard title="בתי קולנוע" value={summary?.total_cinemas} icon="🏛️" color="#06b6d4" />
      </div>

      {/* Top Movie Highlight */}
      {summary?.top_movie && (
        <div style={{
          background: 'linear-gradient(135deg, #1e3a5f 0%, #1e293b 100%)',
          borderRadius: 12,
          padding: '20px 24px',
          border: '1px solid #334155',
          marginBottom: 24,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}>
          <div>
            <div style={{ color: '#94a3b8', fontSize: 13 }}>הסרט המוביל</div>
            <div style={{ fontSize: 22, fontWeight: 700, marginTop: 4 }}>
              {summary.top_movie.title_he || summary.top_movie.title}
            </div>
          </div>
          <div style={{ textAlign: 'left' }}>
            <div style={{ color: '#10b981', fontSize: 24, fontWeight: 700 }}>
              {formatCurrency(summary.top_movie.revenue)}
            </div>
            <div style={{ color: '#94a3b8', fontSize: 13 }}>
              {formatNumber(summary.top_movie.tickets_sold)} כרטיסים
            </div>
          </div>
        </div>
      )}

      {/* Charts Row 1 */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16, marginBottom: 24 }}>
        <ChartCard title="הכנסות יומיות (14 ימים אחרונים)">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={revenueByDate}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="date" tick={{ fill: '#94a3b8', fontSize: 11 }}
                     tickFormatter={d => d ? d.slice(5) : ''} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }}
                     tickFormatter={v => `₪${(v / 1000).toFixed(0)}K`} />
              <Tooltip
                contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
                formatter={(v) => [formatCurrency(v), '']}
                labelStyle={{ color: '#e2e8f0' }}
              />
              <Line type="monotone" dataKey="revenue" stroke="#3b82f6" strokeWidth={2.5}
                    dot={{ fill: '#3b82f6', r: 4 }} name="הכנסות" />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="הכנסות לפי רשת">
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={chainData} dataKey="revenue" nameKey="name_he" cx="50%" cy="50%"
                   outerRadius={100} label={({ name_he, percent }) => `${name_he} ${(percent * 100).toFixed(0)}%`}
                   labelLine={{ stroke: '#64748b' }}>
                {chainData.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
                formatter={(v) => formatCurrency(v)}
              />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Charts Row 2 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        <ChartCard title="טופ סרטים לפי הכנסות">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={topMovies} layout="vertical"
                      margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }}
                     tickFormatter={v => `₪${(v / 1000).toFixed(0)}K`} />
              <YAxis type="category" dataKey="title_he" width={140}
                     tick={{ fill: '#e2e8f0', fontSize: 12 }} />
              <Tooltip
                contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
                formatter={(v) => formatCurrency(v)}
              />
              <Bar dataKey="total_revenue" fill="#3b82f6" radius={[0, 4, 4, 0]} name="הכנסות" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="תפוסה ממוצעת לפי פורמט">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={formatData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="format" tick={{ fill: '#e2e8f0', fontSize: 12 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }}
                     tickFormatter={v => `${v}%`} />
              <Tooltip
                contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
                formatter={(v, name) => [name === 'avg_occupancy' ? `${v}%` : formatCurrency(v), '']}
              />
              <Legend />
              <Bar dataKey="avg_occupancy" fill="#8b5cf6" radius={[4, 4, 0, 0]} name="תפוסה %" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </div>
  );
}

export default Dashboard;
