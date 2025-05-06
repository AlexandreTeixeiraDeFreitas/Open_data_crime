import React, { useRef, useEffect, useState } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

type CrimesTypes = {
  id: number,
  latitude: number,
  longitude	: number,
  date_crime: Date,
  hr_crime: string,
  quatier: string,
  type_crime: string
}

const DynamicMap: React.FC = () => {
  const mapRef = useRef<HTMLDivElement | null>(null);
  const [crimes, setCrimes] = useState<CrimesTypes[] | []>([])
  useEffect(() => {
    const fetchCrimesData = async (url: string) => {
      try {
        const request = await fetch(url);
        const response = await request.json();
        if (response) {
          return response.map((r: any) => ({
          id: r.cmplnt_num,
          latitude: parseFloat(r.latitude),
          longitude: parseFloat(r.longitude),
          date_crime: new Date(r.cmplnt_fr_dt),
          hr_crime: r.cmplnt_fr_tm,
          quatier: r.boro_nm,
          type_crime: r.law_cat_cd,
          })) as CrimesTypes[];
        } else {
          return [];
        }
      } catch (error) {
        console.error("Error fetching crimes data:", error);
        return [];
      }
    };

    (async () => {
      const url = "https://data.cityofnewyork.us/resource/5uac-w243.json?$limit=1000&$offset=1";
      const data = await fetchCrimesData(url);
      setCrimes(data);
    })();
    if (mapRef.current) {
      const map = L.map(mapRef.current).setView([40.7128, -74.0060], 13);
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      }).addTo(map);
      crimes.forEach((crime) => {
        if (crime.latitude && crime.longitude) {
          L.marker([crime.latitude, crime.longitude])
            .addTo(map)
            .bindPopup(`
              <strong>Type:</strong> ${crime.type_crime}<br />
              <strong>Date:</strong> ${crime.date_crime.toLocaleDateString()}<br />
              <strong>Time:</strong> ${crime.hr_crime}<br />
              <strong>Neighborhood:</strong> ${crime.quatier}
            `);
        }
      });
      return () => {
        map.remove();
      };
    }
  }, []);
  
  return <div ref={mapRef} style={{ height: '100%', width: '100%', position: 'fixed' }}></div>;
};

export default DynamicMap;