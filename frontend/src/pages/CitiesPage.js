import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts';
import { fetchCities } from '../api/client';
import DataTable from '../components/DataTable';
import ChartCard from '../components/ChartCard';

const formatCurrency = (val) => `₪${(val || 0).toLocaleString()}`;
const formatNumber = (val) => (val || 0).toLocaleString();

function CitiesPage() {
  const [cities, setCities] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchCities().then(data => {
      setCities(data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const columns = [
    {
      header: 'עיר',
      render: r => <span style={{ fontWeight: 600 }}>{r.city_he || r.city}</span>,
    },
    { header: 'בתי קולנוע', render: r => r.cinemas_count, align: 'center' },
    { header: 'הקרנות', render: r => formatNumber(r.screenings_count), align: 'center' },
    { header: 'כרטיסים', render: r => formatNumber(r.total_tickets_sold), align: 'center' },
    {
      header: 'הכנסות',
      render: r => <span style={{ color: '#10b981', fontWeight: 600 }}>{formatCurrency(r.total_revenue)}</span>,
      align: 'left',
    },
  ];

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען נתוני ערים...</div>;
  }

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 700, marginBottom: 20 }}>ערים</h2>

      <ChartCard title="הכנסות לפי עיר" style={{ marginBottom: 24 }}>
        <ResponsiveContainer width="100%" height={350}>
          <BarChart data={cities} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }}
                   tickFormatter={v => `₪${(v / 1000).toFixed(0)}K`} />
            <YAxis type="category" dataKey="city_he" width={120}
                   tick={{ fill: '#e2e8f0', fontSize: 12 }} />
            <Tooltip
              contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
              formatter={(v) => formatCurrency(v)}
            />
            <Bar dataKey="total_revenue" fill="#06b6d4" radius={[0, 4, 4, 0]} name="הכנסות" />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      <DataTable columns={columns} data={cities} />
    </div>
  );
}

export default CitiesPage;
