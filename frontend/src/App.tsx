import React, { useState } from 'react';
import axios from 'axios';

type CrimeRecord = Record<string, any>;

function App() {
  const [query, setQuery] = useState<string>('$limit=100');
  const [data, setData] = useState<CrimeRecord[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.get<CrimeRecord[]>('http://localhost:5000/crimes', {
        params: Object.fromEntries(new URLSearchParams(query)),
      });
      setData(res.data);
    } catch (err: any) {
      setError(err.message || 'Erreur');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: 20 }}>
      <h2>NYC Crimes – Interface SoQL</h2>

      <input
        style={{ width: '80%', padding: 8, fontSize: 16 }}
        value={query}
        onChange={e => setQuery(e.target.value)}
        placeholder="$select=..., $where=..., $limit=..."
      />
      <button onClick={fetchData} style={{ marginLeft: 10, padding: 10 }}>
        Rechercher
      </button>

      {loading && <p>Chargement...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}

      {data.length > 0 && (
        <table style={{ marginTop: 20, borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr>
              {Object.keys(data[0]).map((key) => (
                <th key={key} style={{ border: '1px solid #ccc', padding: 5 }}>{key}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((record, idx) => (
              <tr key={idx}>
                {Object.values(record).map((val, i) => (
                  <td key={i} style={{ border: '1px solid #eee', padding: 5 }}>
                    {typeof val === 'object' ? JSON.stringify(val) : val}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

export default App;
