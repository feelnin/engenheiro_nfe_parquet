import { renderHeader } from '../components/header.js';
import { renderFilterBar } from '../components/filterBar.js';
import { renderKpiCard } from '../components/kpiCard.js';
import { renderRankingChart } from '../components/rankingChart.js';
import { renderRegionChart } from '../components/regionChart.js';
import { renderStateChart } from '../components/stateChart.js';
import { renderGroupChart } from '../components/groupChart.js';
import { renderDetailTable } from '../components/detailTable.js';

export const buildLayout = () => {
    const app = document.getElementById('app');
    app.innerHTML = `
        ${renderHeader()}
        <main class="container">
            ${renderFilterBar()}
            <section class="kpi-grid" id="kpis-container">
                ${renderKpiCard('Receita Total', 'R$ 0,00', false, 'kpi-receita')}
                ${renderKpiCard('Margem Média', '0%', false, 'kpi-margem')}
                ${renderKpiCard('Dif. Custos', 'R$ 0,00', false, 'kpi-custo')}
                ${renderKpiCard('Margem Negativa', '0', true, 'kpi-negativos')}
            </section>
            <section class="charts-grid">
                ${renderRankingChart()}
                ${renderRegionChart()}
                ${renderStateChart()}
                ${renderGroupChart()}
            </section>
            ${renderDetailTable()}
        </main>
    `;
};