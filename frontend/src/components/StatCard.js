import React from 'react';

const StatCard = ({ title, value, subtitle, color = '#3b82f6', icon }) => (
  <div style={{
    background: '#1e293b',
    borderRadius: 12,
    padding: '20px 24px',
    border: '1px solid #334155',
    flex: '1 1 200px',
    minWidth: 200,
  }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
      <div>
        <div style={{ color: '#94a3b8', fontSize: 13, marginBottom: 8 }}>{title}</div>
        <div style={{ fontSize: 28, fontWeight: 700, color }}>{value}</div>
        {subtitle && (
          <div style={{ color: '#64748b', fontSize: 12, marginTop: 4 }}>{subtitle}</div>
        )}
      </div>
      {icon && <span style={{ fontSize: 32, opacity: 0.6 }}>{icon}</span>}
    </div>
  </div>
);

export default StatCard;
