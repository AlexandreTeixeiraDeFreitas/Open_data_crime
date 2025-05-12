import React from 'react';

interface AskPredictProps {
    onSubmit: (city: string, date: string) => Promise<void>; // Explicitly typed function
    city: string,
    setCity: (city: string) => void,
    date: string,
    setDate: (date: string) => void,
}

const AskPredict = ({ onSubmit, city, setCity, date, setDate }: AskPredictProps) => {
    
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