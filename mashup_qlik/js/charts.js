import { getMarginColor } from './mockData.js';

let instances = {}; // Guarda instâncias para destruir ao atualizar (filtros)

export const renderChartsAndTable = (data) => {
    // Top Clientes Chart
    const topData = [...data].sort((a,b) => b.margem - a.margem).slice(0, 5);
    if(instances.ranking) instances.ranking.destroy();
    instances.ranking = new Chart(document.getElementById('rankingChart'), {
        type: 'bar',
        data: {
            labels: topData.map(d => d.cliente),
            datasets: [{ data: topData.map(d => d.margem), backgroundColor: topData.map(d => getMarginColor(d.margem)) }]
        },
        options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
    });

    // Region Chart
    if(instances.region) instances.region.destroy();
    instances.region = new Chart(document.getElementById('regionChart'), {
        type: 'bar',
        data: { labels: ['Sul', 'Nordeste', 'Sudeste'], datasets: [{ data: [12, 5, -2], backgroundColor: ['#0B3D91', '#F58220', '#D32F2F'] }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
    });

    // --- OS NOVOS GRÁFICOS AGORA ESTÃO AQUI DENTRO ---

    // State Chart (Margem por Estado)
    if(instances.state) instances.state.destroy();
    const stateCanvas = document.getElementById('stateChart'); 
    if (stateCanvas) {
        instances.state = new Chart(stateCanvas, {
            type: 'bar',
            data: { 
                labels: ['SP', 'RJ', 'MG', 'BA', 'PR'], 
                datasets: [{ 
                    label: 'Margem (%)',
                    data: [15, -5, 8, 12, 3], 
                    backgroundColor: ['#0B3D91', '#D32F2F', '#F58220', '#0B3D91', '#F58220'] 
                }] 
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
        });
    }

    // Group Chart (Margem por Grupo)
    if(instances.group) instances.group.destroy();
    const groupCanvas = document.getElementById('groupChart');
    if (groupCanvas) {
        instances.group = new Chart(groupCanvas, {
            type: 'doughnut', 
            data: { 
                labels: ['Varejo', 'Atacado', 'Distribuição'], 
                datasets: [{ 
                    data: [45, 30, 25], 
                    backgroundColor: ['#0B3D91', '#F58220', '#D32F2F'] 
                }] 
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }

    // Atualizar Tabela
    const tbody = document.getElementById('table-body');
    if (tbody) { // Adicionei uma checagem de segurança aqui também!
        tbody.innerHTML = data.map(d => `
            <tr>
                <td>${d.cliente}</td>
                <td>${d.regiao}</td>
                <td class="num">${new Intl.NumberFormat('pt-BR', {style: 'currency', currency: 'BRL'}).format(d.receita)}</td>
                <td class="num"><span class="badge" style="background-color:${getMarginColor(d.margem)}">${d.margem.toFixed(1)}%</span></td>
            </tr>
        `).join('');
    }
};