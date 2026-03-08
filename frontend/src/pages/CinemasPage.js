import React, { useState, useEffect } from 'react';
import { fetchCinemas } from '../api/client';
import DataTable from '../components/DataTable';
import StatCard from '../components/StatCard';

const formatCurrency = (val) => `₪${(val || 0).toLocaleString()}`;
const formatNumber = (val) => (val || 0).toLocaleString();

function CinemasPage() {
  const [cinemas, setCinemas] = useState([]);
  const [loading, setLoading] = useState(true);
  const [chainFilter, setChainFilter] = useState('all');

  useEffect(() => {
    fetchCinemas().then(data => {
      setCinemas(data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const chains = [...new Set(cinemas.map(c => c.chain))];
  const filtered = chainFilter === 'all' ? cinemas : cinemas.filter(c => c.chain === chainFilter);

  const totalRevenue = filtered.reduce((s, c) => s + c.total_revenue, 0);
  const totalTickets = filtered.reduce((s, c) => s + c.total_tickets_sold, 0);

  const columns = [
    {
      header: 'בית קולנוע',
      render: (row) => (
        <div>
          <div style={{ fontWeight: 600 }}>{row.name_he || row.name}</div>
          <div style={{ color: '#64748b', fontSize: 12 }}>{row.chain_he || row.chain}</div>
        </div>
      ),
    },
    { header: 'עיר', render: r => r.city_he || r.city },
    { header: 'אולמות', key: 'halls_count', align: 'center' },
    { header: 'הקרנות', render: r => formatNumber(r.screenings_count), align: 'center' },
    { header: 'כרטיסים', render: r => formatNumber(r.total_tickets_sold), align: 'center' },
    {
      header: 'הכנסות',
      render: r => <span style={{ color: '#10b981', fontWeight: 600 }}>{formatCurrency(r.total_revenue)}</span>,
      align: 'left',
    },
  ];

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען בתי קולנוע...</div>;
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
        <h2 style={{ fontSize: 22, fontWeight: 700 }}>בתי קולנוע</h2>
        <select
          value={chainFilter}
          onChange={e => setChainFilter(e.target.value)}
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
          <option value="all">כל הרשתות</option>
          {chains.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>

      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 20 }}>
        <StatCard title="בתי קולנוע" value={filtered.length} color="#3b82f6" />
        <StatCard title="סה״כ הכנסות" value={formatCurrency(totalRevenue)} color="#10b981" />
        <StatCard title="סה״כ כרטיסים" value={formatNumber(totalTickets)} color="#8b5cf6" />
      </div>

      <DataTable columns={columns} data={filtered} />
    </div>
  );
}

export default CinemasPage;
