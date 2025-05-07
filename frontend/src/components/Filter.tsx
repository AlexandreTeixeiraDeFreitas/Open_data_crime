import React, { useState } from 'react';

type OptionsTypes = {
    name: string, 
    value: string,
}

interface FilterProps {
    options: OptionsTypes[];
    onFilterChange: (searchTerm: string, selectedOption: string, event: React.ChangeEvent) => void;
}

const Filter: React.FC<FilterProps> = ({ options, onFilterChange }) => {
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedOption, setSelectedOption] = useState('');

    const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const value = event.target.value;
        setSearchTerm(value);
        onFilterChange(value, selectedOption, event);
    };

    const handleSelectChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
        const value = event.target.value;
        setSelectedOption(value);
        onFilterChange(searchTerm, value, event);
    };

    return (
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
            <input
                type="text"
                placeholder="Search..."
                value={searchTerm}
                onChange={handleSearchChange}
                style={{ padding: '0.5rem', flex: 1 }}
            />
            <select
                value={selectedOption}
                onChange={handleSelectChange}
                style={{ padding: '0.5rem' }}
            >
                <option value="All">All</option>
                {options.map((option) => (
                    <option key={option.name} value={option.value}>
                        {option.name}
                    </option>
                ))}
            </select>
        </div>
    );
};

export default Filter;