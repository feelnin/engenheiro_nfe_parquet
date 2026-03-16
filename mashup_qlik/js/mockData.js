export const getMockData = () => {
    const data = [];
    const regions = ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul'];
    const groups = ['Postos', 'Indústrias', 'Distribuidores'];
    
    for (let i = 1; i <= 20; i++) {
        const receita = Math.random() * 500000 + 50000;
        const margem = (Math.random() * 20) - 5; // Margens entre -5% e 15%
        
        data.push({
            id: i,
            cliente: `Cliente ${String.fromCharCode(64 + i)}`,
            regiao: regions[Math.floor(Math.random() * regions.length)],
            grupo: groups[Math.floor(Math.random() * groups.length)],
            receita: receita,
            custo: receita * (1 - (margem/100)),
            margem: margem
        });
    }
    return data;
};

export const getMarginColor = (margin) => {
    if (margin > 10) return '#0B3D91';
    if (margin >= 5) return '#9E9E9E';
    if (margin >= 0) return '#F58220';
    return '#D32F2F';
};