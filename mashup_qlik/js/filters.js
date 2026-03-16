import { updateKpis } from './kpis.js';
import { renderChartsAndTable } from './charts.js';

export const initFilters = (allData) => {
    const filterRegiao = document.getElementById('filter-regiao');
    
    // Extrai regiões únicas para popular o select
    const regioes = [...new Set(allData.map(d => d.regiao))];
    regioes.forEach(r => filterRegiao.innerHTML += `<option value="${r}">${r}</option>`);

    // Evento de filtragem
    filterRegiao.addEventListener('change', (e) => {
        const val = e.target.value;
        const filteredData = val === "Todas" ? allData : allData.filter(d => d.regiao === val);
        
        updateKpis(filteredData);
        renderChartsAndTable(filteredData);
    });
};