import React, { useState } from 'react';

interface AskPredictProps {
    onSubmit: (city: string, date: string) => void;
}

const AskPredict: React.FC<AskPredictProps> = ({ onSubmit }) => {
    const [city, setCity] = useState('');
    const [date, setDate] = useState('');

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        if (city && date) {
            onSubmit(city, date);
        } else {
            alert('Please fill in both fields.');
        }
    };

    return (
        <div>
            <h2>Crime Prediction</h2>
            <form onSubmit={handleSubmit}>
                <div>
                    <label htmlFor="city">City:</label>
                    <input
                        type="text"
                        id="city"
                        value={city}
                        onChange={(e) => setCity(e.target.value)}
                        placeholder="Enter city name"
                    />
                </div>
                <div>
                    <label htmlFor="date">Date:</label>
                    <input
                        type="date"
                        id="date"
                        value={date}
                        onChange={(e) => setDate(e.target.value)}
                    />
                </div>
                <button type="submit">Predict</button>
            </form>
        </div>
    );
};

export default AskPredict;