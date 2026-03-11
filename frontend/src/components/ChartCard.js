import React from 'react';

const ChartCard = ({ title, children, style = {} }) => (
  <div style={{
    background: '#1e293b',
    borderRadius: 12,
    border: '1px solid #334155',
    padding: '20px 24px',
    ...style,
  }}>
    <h3 style={{
      fontSize: 15,
      fontWeight: 600,
      color: '#e2e8f0',
      marginBottom: 16,
    }}>
      {title}
    </h3>
    {children}
  </div>
);

export default ChartCard;
