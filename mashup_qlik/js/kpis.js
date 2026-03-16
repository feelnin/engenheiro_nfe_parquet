export const updateKpis = (data) => {
    let totalRev = 0, totalCost = 0, negClients = 0;
    
    data.forEach(d => {
        totalRev += d.receita;
        totalCost += d.custo;
        if (d.margem < 0) negClients++;
    });

    const avgMargin = totalRev ? ((totalRev - totalCost) / totalRev) * 100 : 0;
    
    document.getElementById('kpi-receita').innerText = new Intl.NumberFormat('pt-BR', {style: 'currency', currency: 'BRL'}).format(totalRev);
    document.getElementById('kpi-margem').innerText = avgMargin.toFixed(1) + '%';
    document.getElementById('kpi-negativos').innerText = negClients;
};