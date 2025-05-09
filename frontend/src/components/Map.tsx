import React, { useRef, useEffect, useState } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import Filter from './Filter';
type CrimesTypes = {
  cmplnt_num: number,
  latitude: number,
  longitude	: number,
  cmplnt_fr_dt: Date,
  cmplnt_fr_tm: string,
  boro_nm: string,
  pd_desc: string
}

type ParamType = {
  selectedParam: string,
  searchParam: string
}

const url = "http://localhost:5000/crimes";

const DynamicMap: React.FC = () => {
  const mapRef = useRef<HTMLDivElement | null>(null);
  const [crimes, setCrimes] = useState<CrimesTypes[] | []>([])
  const [_, setParam] = useState<ParamType>({
    selectedParam: '',
    searchParam: ''
  })
  const fetchCrimesData = async (url: string, option?: ParamType) => {
    try {
      const request = await fetch(`${url}?${option?.searchParam ? "$where=" + option.selectedParam + " like '%25" + option.searchParam + "%25'" : ''}`);
      const response = await request.json();
      if (response) {
        return response.map((r: any) => ({
          cmplnt_num: r.cmplnt_num,
          latitude: parseFloat(r.latitude),
          longitude: parseFloat(r.longitude),
          cmplnt_fr_dt: new Date(r.cmplnt_fr_dt),
          cmplnt_fr_tm: r.cmplnt_fr_tm,
          boro_nm: r.boro_nm,
          pd_desc: r.pd_desc,
        })) as CrimesTypes[];
      } else {
        return [];
      }
    } catch (error) {
      console.error("Error fetching crimes data:", error);
      return [];
    }
  };

  useEffect(()=> {
    (async () => {
      const data = await fetchCrimesData(url);
      setCrimes(data);
    })();
  }, [])

  useEffect(() => {
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
              <strong>Type:</strong> ${crime.pd_desc}<br />
              <strong>Date:</strong> ${crime.cmplnt_fr_dt.toLocaleDateString()}<br />
              <strong>Time:</strong> ${crime.cmplnt_fr_tm}<br />
              <strong>Neighborhood:</strong> ${crime.boro_nm}
            `);
        }
      });
      return () => {
        map.remove();
      };
    }
  }, [crimes]);

  const handleFilterChange = (searchTerm: string, optionSelected: string, event: React.ChangeEvent) => {
    event.preventDefault();
    const updatedParam = { searchParam: searchTerm.toUpperCase(), selectedParam: optionSelected };
    setParam(updatedParam);

    // Add a threshold to limit the request
    if (searchTerm.length < 4) {
      console.warn("Search term must be at least 3 characters long.");
      return;
    }

    (async () => {
      const data = await fetchCrimesData(url, updatedParam);
      setCrimes(data);
    })();
  };

  const option = [
    {
      name: 'latitude',
      value: 'latitude'
    },
    {
      name: 'date',
      value: 'cmplnt_fr_dt',
    },
    {
      name: 'hour',
      value: 'cmplnt_fr_tm'
    },
    {
      name: 'city',
      value: 'boro_nm'
    },
    {
      name: 'type of the crime',
      value: 'pd_desc'
    }
  ]
  
  return (
    <>
      <Filter 
        options={option} 
        onFilterChange={handleFilterChange}
      />
      <div 
        ref={mapRef} 
        style={{ height: '100%', width: '100%', position: 'fixed' }}
      />
    </>
  );
};

export default DynamicMap;