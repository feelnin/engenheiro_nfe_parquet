export const renderDetailTable = () => `
    <div class="card">
        <h2 class="card-title">Detalhamento</h2>
        <div class="table-responsive">
            <table>
                <thead>
                    <tr>
                        <th>Cliente</th>
                        <th>Região</th>
                        <th class="num">Receita</th>
                        <th class="num">Margem</th>
                    </tr>
                </thead>
                <tbody id="table-body"></tbody>
            </table>
        </div>
    </div>
`;