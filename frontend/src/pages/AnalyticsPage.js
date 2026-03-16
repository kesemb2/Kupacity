import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
} from 'recharts';
import {
  fetchTicketsByHour, fetchOccupancyByDayOfWeek, fetchMovieTrends,
  fetchDeadScreenings, fetchBranchEfficiency, fetchGenreStats,
} from '../api/client';
import ChartCard from '../components/ChartCard';
import StatCard from '../components/StatCard';

const formatNumber = (val) => (val || 0).toLocaleString();

const COLORS = ['#3b82f6', '#8b5cf6', '#06b6d4', '#f59e0b', '#ef4444', '#22c55e', '#ec4899', '#f97316'];

const tooltipStyle = { background: '#1e293b', border: '1px solid #475569', borderRadius: 8 };

function AnalyticsPage() {
  const [hourData, setHourData] = useState([]);
  const [dayData, setDayData] = useState([]);
  const [trends, setTrends] = useState([]);
  const [deadData, setDeadData] = useState(null);
  const [efficiency, setEfficiency] = useState([]);
  const [genreData, setGenreData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetchTicketsByHour(),
      fetchOccupancyByDayOfWeek(),
      fetchMovieTrends(),
      fetchDeadScreenings(10),
      fetchBranchEfficiency(),
      fetchGenreStats(),
    ]).then(([h, d, t, dead, eff, g]) => {
      setHourData(h);
      setDayData(d);
      setTrends(t);
      setDeadData(dead);
      setEfficiency(eff);
      setGenreData(g);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען ניתוחים...</div>;
  }

  // Find peak hour
  const peakHour = hourData.reduce((max, h) => (h.avg_occupancy > (max?.avg_occupancy || 0) ? h : max), null);

  // Find best day
  const bestDay = dayData.reduce((max, d) => (d.avg_occupancy > (max?.avg_occupancy || 0) ? d : max), null);

  return (
    <div>
      {/* Stats Row */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 24 }}>
        {peakHour && (
          <StatCard
            title="שעת השיא"
            value={peakHour.hour_display}
            subtitle={`${peakHour.avg_occupancy}% תפוסה ממוצעת`}
            icon="⏰"
            color="#f59e0b"
          />
        )}
        {bestDay && (
          <StatCard
            title="היום הכי מלא"
            value={`יום ${bestDay.day_name}`}
            subtitle={`${bestDay.avg_occupancy}% תפוסה ממוצעת`}
            icon="📅"
            color="#22c55e"
          />
        )}
        {deadData && (
          <StatCard
            title="הקרנות מתות (<10%)"
            value={`${deadData.dead_pct}%`}
            subtitle={`${formatNumber(deadData.dead_count)} מתוך ${formatNumber(deadData.total_screenings)}`}
            icon="💀"
            color="#ef4444"
          />
        )}
        {efficiency.length > 0 && (
          <StatCard
            title="סניף יעיל ביותר"
            value={efficiency[0].cinema}
            subtitle={`${efficiency[0].avg_occupancy}% תפוסה • ${efficiency[0].city}`}
            icon="🏆"
            color="#8b5cf6"
          />
        )}
      </div>

      {/* Row 1: Golden Hour + Day of Week */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <ChartCard title="שעת הזהב — תפוסה לפי שעת הקרנה">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={hourData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="hour_display" tick={{ fill: '#e2e8f0', fontSize: 11 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} tickFormatter={v => `${v}%`} />
              <Tooltip contentStyle={tooltipStyle} formatter={(v, name) => [name === 'כרטיסים' ? formatNumber(v) : `${v}%`, name]} />
              <Bar dataKey="avg_occupancy" fill="#f59e0b" radius={[4, 4, 0, 0]} name="תפוסה %" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="תפוסה לפי יום בשבוע">
          <ResponsiveContainer width="100%" height={300}>
            <RadarChart data={dayData} cx="50%" cy="50%" outerRadius="70%">
              <PolarGrid stroke="#334155" />
              <PolarAngleAxis dataKey="day_name" tick={{ fill: '#e2e8f0', fontSize: 13 }} />
              <PolarRadiusAxis tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <Radar name="תפוסה %" dataKey="avg_occupancy" stroke="#22c55e" fill="#22c55e" fillOpacity={0.3} />
            </RadarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Row 2: Movie Trends */}
      <ChartCard title="מגמות סרטים — 3 ימים אחרונים מול 3 ימים קודמים" style={{ marginBottom: 24 }}>
        {trends.length === 0 ? (
          <div style={{ textAlign: 'center', padding: 40, color: '#64748b' }}>אין מספיק נתונים למגמות</div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #334155' }}>
                  <th style={{ padding: '10px 16px', textAlign: 'right', color: '#94a3b8', fontSize: 13 }}>סרט</th>
                  <th style={{ padding: '10px 16px', textAlign: 'center', color: '#94a3b8', fontSize: 13 }}>3 ימים אחרונים</th>
                  <th style={{ padding: '10px 16px', textAlign: 'center', color: '#94a3b8', fontSize: 13 }}>3 ימים קודמים</th>
                  <th style={{ padding: '10px 16px', textAlign: 'center', color: '#94a3b8', fontSize: 13 }}>שינוי</th>
                  <th style={{ padding: '10px 16px', textAlign: 'center', color: '#94a3b8', fontSize: 13 }}>מגמה</th>
                </tr>
              </thead>
              <tbody>
                {trends.map((t, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
                    <td style={{ padding: '10px 16px', fontWeight: 500 }}>{t.title_he || t.title}</td>
                    <td style={{ padding: '10px 16px', textAlign: 'center' }}>{formatNumber(t.recent_tickets)}</td>
                    <td style={{ padding: '10px 16px', textAlign: 'center', color: '#94a3b8' }}>{formatNumber(t.previous_tickets)}</td>
                    <td style={{
                      padding: '10px 16px', textAlign: 'center', fontWeight: 600,
                      color: t.trend === 'up' ? '#22c55e' : t.trend === 'down' ? '#ef4444' : '#94a3b8',
                    }}>
                      {t.change_pct > 0 ? '+' : ''}{t.change_pct}%
                    </td>
                    <td style={{ padding: '10px 16px', textAlign: 'center', fontSize: 20 }}>
                      {t.trend === 'up' ? '📈' : t.trend === 'down' ? '📉' : '➡️'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </ChartCard>

      {/* Row 3: Genre Stats + Branch Efficiency */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <ChartCard title="ז'אנרים — כרטיסים ותפוסה">
          {genreData.length === 0 ? (
            <div style={{ textAlign: 'center', padding: 40, color: '#64748b' }}>אין נתוני ז'אנרים</div>
          ) : (
            <div style={{ display: 'flex', gap: 16, alignItems: 'center' }}>
              <ResponsiveContainer width="50%" height={280}>
                <PieChart>
                  <Pie
                    data={genreData}
                    dataKey="total_tickets"
                    nameKey="genre"
                    cx="50%" cy="50%"
                    outerRadius={100}
                    label={({ genre, percent }) => `${genre} ${(percent * 100).toFixed(0)}%`}
                    labelLine={{ stroke: '#64748b' }}
                  >
                    {genreData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  </Pie>
                  <Tooltip contentStyle={tooltipStyle} formatter={v => formatNumber(v)} />
                </PieChart>
              </ResponsiveContainer>
              <div style={{ flex: 1 }}>
                {genreData.slice(0, 6).map((g, i) => (
                  <div key={i} style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    padding: '8px 0', borderBottom: '1px solid #334155',
                  }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ width: 10, height: 10, borderRadius: '50%', background: COLORS[i % COLORS.length], display: 'inline-block' }} />
                      <span style={{ fontSize: 13 }}>{g.genre}</span>
                    </div>
                    <div style={{ textAlign: 'left', fontSize: 12 }}>
                      <span style={{ color: '#e2e8f0' }}>{formatNumber(g.total_tickets)}</span>
                      <span style={{ color: '#64748b', marginRight: 8 }}> • {g.avg_occupancy}%</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </ChartCard>

        <ChartCard title="דירוג סניפים לפי יעילות">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={efficiency.slice(0, 10)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} tickFormatter={v => `${v}%`} />
              <YAxis type="category" dataKey="cinema" width={130} tick={{ fill: '#e2e8f0', fontSize: 11 }} />
              <Tooltip
                contentStyle={tooltipStyle}
                formatter={(v) => [`${v}%`, '']}
                labelFormatter={(label) => {
                  const item = efficiency.find(e => e.cinema === label);
                  return item ? `${label} (${item.city})` : label;
                }}
              />
              <Bar dataKey="avg_occupancy" fill="#8b5cf6" radius={[0, 4, 4, 0]} name="תפוסה %" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Row 4: Dead Screenings */}
      {deadData && deadData.screenings.length > 0 && (
        <ChartCard title={`הקרנות מתות — פחות מ-${deadData.threshold}% תפוסה (${deadData.dead_count} הקרנות)`}>
          <div style={{ overflowX: 'auto', maxHeight: 400, overflowY: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #334155', position: 'sticky', top: 0, background: '#1e293b' }}>
                  <th style={{ padding: '10px 12px', textAlign: 'right', color: '#94a3b8', fontSize: 12 }}>סרט</th>
                  <th style={{ padding: '10px 12px', textAlign: 'right', color: '#94a3b8', fontSize: 12 }}>סניף</th>
                  <th style={{ padding: '10px 12px', textAlign: 'right', color: '#94a3b8', fontSize: 12 }}>עיר</th>
                  <th style={{ padding: '10px 12px', textAlign: 'center', color: '#94a3b8', fontSize: 12 }}>שעה</th>
                  <th style={{ padding: '10px 12px', textAlign: 'center', color: '#94a3b8', fontSize: 12 }}>אולם</th>
                  <th style={{ padding: '10px 12px', textAlign: 'center', color: '#94a3b8', fontSize: 12 }}>נמכרו</th>
                  <th style={{ padding: '10px 12px', textAlign: 'center', color: '#94a3b8', fontSize: 12 }}>מושבים</th>
                  <th style={{ padding: '10px 12px', textAlign: 'center', color: '#94a3b8', fontSize: 12 }}>תפוסה</th>
                </tr>
              </thead>
              <tbody>
                {deadData.screenings.map((s, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
                    <td style={{ padding: '8px 12px', fontSize: 13 }}>{s.movie}</td>
                    <td style={{ padding: '8px 12px', fontSize: 13 }}>{s.cinema}</td>
                    <td style={{ padding: '8px 12px', fontSize: 13, color: '#94a3b8' }}>{s.city}</td>
                    <td style={{ padding: '8px 12px', fontSize: 12, textAlign: 'center', color: '#94a3b8' }}>
                      {s.showtime ? new Date(s.showtime).toLocaleString('he-IL', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }) : ''}
                    </td>
                    <td style={{ padding: '8px 12px', fontSize: 12, textAlign: 'center' }}>{s.hall}</td>
                    <td style={{ padding: '8px 12px', fontSize: 13, textAlign: 'center', fontWeight: 600, color: '#ef4444' }}>{s.tickets_sold}</td>
                    <td style={{ padding: '8px 12px', fontSize: 13, textAlign: 'center' }}>{s.total_seats}</td>
                    <td style={{ padding: '8px 12px', fontSize: 13, textAlign: 'center', fontWeight: 600, color: '#ef4444' }}>{s.occupancy}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </ChartCard>
      )}
    </div>
  );
}

export default AnalyticsPage;
