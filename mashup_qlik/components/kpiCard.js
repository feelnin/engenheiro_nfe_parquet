export const renderKpiCard = (title, value, isAlert = false, id = "") => `
    <div class="card kpi-card ${isAlert ? 'alert' : ''}">
        <div class="kpi-title">${title}</div>
        <div class="kpi-value" id="${id}">${value}</div>
    </div>
`;