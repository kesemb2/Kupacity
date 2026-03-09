import React, { useState, useEffect } from 'react';
import { fetchCinemas } from '../api/client';
import DataTable from '../components/DataTable';
import StatCard from '../components/StatCard';

const formatNumber = (val) => (val || 0).toLocaleString();

function CinemasPage() {
  const [cinemas, setCinemas] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchCinemas().then(data => {
      setCinemas(data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const totalTickets = cinemas.reduce((s, c) => s + c.total_tickets_sold, 0);
  const totalScreenings = cinemas.reduce((s, c) => s + c.screenings_count, 0);

  const columns = [
    {
      header: 'סניף',
      render: (row) => (
        <div>
          <div style={{ fontWeight: 600 }}>{row.name_he || row.name}</div>
        </div>
      ),
    },
    { header: 'עיר', render: r => r.city_he || r.city },
    { header: 'אולמות', key: 'halls_count', align: 'center' },
    { header: 'הקרנות', render: r => formatNumber(r.screenings_count), align: 'center' },
    {
      header: 'כרטיסים',
      render: r => <span style={{ color: '#3b82f6', fontWeight: 600 }}>{formatNumber(r.total_tickets_sold)}</span>,
      align: 'center',
    },
  ];

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: '#64748b' }}>טוען סניפים...</div>;
  }

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 700, marginBottom: 20 }}>סניפי הוט סינמה</h2>

      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 20 }}>
        <StatCard title="סניפים" value={cinemas.length} color="#3b82f6" />
        <StatCard title="סה״כ כרטיסים" value={formatNumber(totalTickets)} color="#8b5cf6" />
        <StatCard title="סה״כ הקרנות" value={formatNumber(totalScreenings)} color="#f59e0b" />
      </div>

      <DataTable columns={columns} data={cinemas} />
    </div>
  );
}

export default CinemasPage;
