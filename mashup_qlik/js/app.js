import { buildLayout } from './layout.js';
import { getMockData } from './mockData.js';
import { updateKpis } from './kpis.js';
import { renderChartsAndTable } from './charts.js';
import { initFilters } from './filters.js';

// 1. Constrói o HTML (Shell da UI)
buildLayout();

// 2. Busca os dados (Simulando chamada API/Qlik)
const data = getMockData();

// 3. Inicializa Lógicas de UI
initFilters(data);
updateKpis(data);
renderChartsAndTable(data);