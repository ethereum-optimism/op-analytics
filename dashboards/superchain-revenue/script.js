// Use configuration from config.js
const CHAIN_COLORS = {};
const CHAIN_ICONS = {};
const BLOCK_EXPLORERS = {};

// Initialize from config when available
if (typeof DASHBOARD_CONFIG !== 'undefined') {
    Object.entries(DASHBOARD_CONFIG.chains).forEach(([chainName, config]) => {
        CHAIN_COLORS[chainName] = config.color;
        CHAIN_ICONS[chainName] = config.icon;
        BLOCK_EXPLORERS[chainName] = config.explorer;
    });
}

// Add "Other" category
CHAIN_COLORS['Other'] = '#6c757d';

// Global Data Storage
let revenueData = [];
let transactionData = [];
let marketShareData = [];
let currentTimeRange = 'monthly';
let earningsChart = null;

// DOM Elements
const loadingElement = document.getElementById('loading');
const monthlyEarningsElement = document.getElementById('monthly-earnings');
const earningsSubtitleElement = document.getElementById('earnings-subtitle');
const marketShareElement = document.getElementById('market-share');
const marketShareSubtitleElement = document.getElementById('market-share-subtitle');
const financialTableElement = document.getElementById('financial-table');
const chainLogosElement = document.getElementById('chain-logos');
const transactionsTableElement = document.getElementById('transactions-table');
const monthlyBtn = document.getElementById('monthly-btn');
const quarterlyBtn = document.getElementById('quarterly-btn');
const tooltip = document.getElementById('tooltip');

// Utility Functions
function parseNumericValue(value) {
    if (typeof value === 'number') return value;
    if (typeof value === 'string') {
        // Remove commas and quotes, then parse as float
        const cleaned = value.replace(/[",]/g, '');
        const parsed = parseFloat(cleaned);
        return isNaN(parsed) ? 0 : parsed;
    }
    return 0;
}

function formatCurrency(amount, decimals = 2) {
    const numericAmount = parseNumericValue(amount);
    return `${numericAmount.toFixed(decimals)} ETH`;
}

function formatPercent(value, decimals = 1) {
    return `${value.toFixed(decimals)}%`;
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'short' 
    });
}

function formatDateTime(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'short', 
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function calculatePercentChange(current, previous) {
    if (!previous || previous === 0) return 0;
    return ((current - previous) / previous) * 100;
}

function showTooltip(event, content) {
    tooltip.innerHTML = content;
    tooltip.style.left = event.pageX + 10 + 'px';
    tooltip.style.top = event.pageY + 10 + 'px';
    tooltip.classList.add('visible');
}

function hideTooltip() {
    tooltip.classList.remove('visible');
}

// Data Loading Functions
async function loadCSVData(filename) {
    try {
        const response = await fetch(filename);
        const csvText = await response.text();
        
        return new Promise((resolve, reject) => {
            Papa.parse(csvText, {
                header: true,
                skipEmptyLines: true,
                dynamicTyping: false, // Disable automatic type conversion to handle manually
                complete: (results) => {
                    if (results.errors.length > 0) {
                        console.warn(`Parsing warnings for ${filename}:`, results.errors);
                    }
                    resolve(results.data);
                },
                error: (error) => reject(error)
            });
        });
    } catch (error) {
        console.error(`Error loading ${filename}:`, error);
        throw error;
    }
}

async function loadAllData() {
    try {
        const config = typeof DASHBOARD_CONFIG !== 'undefined' ? DASHBOARD_CONFIG : {};
        const dataFiles = config.dataFiles || {
            revenue: 'revenue_data.csv',
            transactions: 'op_collective_revenue_transactions.csv',
            marketShare: 'Superchain_market_share.csv'
        };

        const [revenue, transactions, marketShare] = await Promise.all([
            loadCSVData(dataFiles.revenue),
            loadCSVData(dataFiles.transactions),
            loadCSVData(dataFiles.marketShare)
        ]);

        revenueData = revenue.map(row => ({
            ...row,
            dt_month: new Date(row.dt_month),
            l2_fees_as_superchain: parseNumericValue(row.l2_fees_as_superchain),
            l1_costs_as_superchain: parseNumericValue(row.l1_costs_as_superchain),
            net_onchain_profit_as_superchain: parseNumericValue(row.net_onchain_profit_as_superchain),
            revshare_estimated: parseNumericValue(row.revshare_estimated),
            chain_governor_profit_estimated: parseNumericValue(row.chain_governor_profit_estimated)
        })).sort((a, b) => a.dt_month - b.dt_month);

        transactionData = transactions.filter(row => row.is_op_transfer === true || row.is_op_transfer === 'true')
            .map(row => ({
                ...row,
                dt_month: new Date(row.dt_month),
                tx_block_time: new Date(row.tx_block_time),
                value_decimal: parseNumericValue(row.value_decimal)
            })).sort((a, b) => b.tx_block_time - a.tx_block_time);

        marketShareData = marketShare.map(row => ({
            ...row,
            dt_month: new Date(row.dt_month),
            value: parseNumericValue(row.value)
        })).sort((a, b) => a.dt_month - b.dt_month);

        console.log('Data loaded successfully:', {
            revenue: revenueData.length,
            transactions: transactionData.length,
            marketShare: marketShareData.length
        });

    } catch (error) {
        console.error('Error loading data:', error);
        throw error;
    }
}

// High-Level Metrics Functions
function updateHighLevelMetrics() {
    if (revenueData.length === 0 || marketShareData.length === 0) return;

    // Get latest month data
    const latestMonth = new Date(Math.max(...revenueData.map(d => d.dt_month)));
    const previousMonth = new Date(latestMonth);
    previousMonth.setMonth(previousMonth.getMonth() - 1);

    // Calculate monthly earnings
    const latestMonthData = revenueData.filter(d => 
        d.dt_month.getTime() === latestMonth.getTime()
    );
    
    const previousMonthData = revenueData.filter(d => 
        d.dt_month.getTime() === previousMonth.getTime()
    );

    const currentEarnings = latestMonthData.reduce((sum, row) => sum + parseNumericValue(row.revshare_estimated), 0);
    const previousEarnings = previousMonthData.reduce((sum, row) => sum + parseNumericValue(row.revshare_estimated), 0);
    const earningsChange = calculatePercentChange(currentEarnings, previousEarnings);

    monthlyEarningsElement.textContent = formatCurrency(currentEarnings);
    earningsSubtitleElement.textContent = `As of ${formatDate(latestMonth)} | ${earningsChange >= 0 ? '+' : ''}${earningsChange.toFixed(1)}% vs prior month`;
    earningsSubtitleElement.className = `metric-subtitle ${earningsChange >= 0 ? 'text-success' : 'text-error'}`;

    // Calculate market share
    const latestMarketShare = marketShareData
        .filter(d => d.Metric === 'l2_chain_fees_share')
        .sort((a, b) => b.dt_month - a.dt_month)[0];

    const previousMarketShare = marketShareData
        .filter(d => d.Metric === 'l2_chain_fees_share')
        .sort((a, b) => b.dt_month - a.dt_month)[1];

    if (latestMarketShare) {
        marketShareElement.textContent = formatPercent(latestMarketShare.value);
        
        if (previousMarketShare) {
            const shareChange = latestMarketShare.value - previousMarketShare.value;
            marketShareSubtitleElement.textContent = `${shareChange >= 0 ? '+' : ''}${shareChange.toFixed(1)} pp vs prior month`;
            marketShareSubtitleElement.className = `metric-subtitle ${shareChange >= 0 ? 'text-success' : 'text-error'}`;
        }
    }
}

// Chart Functions
function createEarningsChart() {
    const ctx = document.getElementById('earnings-chart').getContext('2d');
    
    // Group data by month
    const monthlyData = {};
    revenueData.forEach(row => {
        const monthKey = row.dt_month.toISOString().substring(0, 7);
        if (!monthlyData[monthKey]) {
            monthlyData[monthKey] = {};
        }
        monthlyData[monthKey][row.display_name] = parseNumericValue(row.revshare_estimated);
    });

    // Get latest month for filtering top chains
    const latestMonth = Object.keys(monthlyData).sort().pop();
    const latestMonthData = monthlyData[latestMonth] || {};
    
    // Determine top chains (> 0.1 ETH or top 10)
    const chainTotals = Object.entries(latestMonthData)
        .sort((a, b) => b[1] - a[1]);
    
    const topChains = chainTotals
        .filter(([chain, amount]) => amount > 0.1)
        .slice(0, 10)
        .map(([chain]) => chain);
    
    // Prepare datasets
    const months = Object.keys(monthlyData).sort();
    const datasets = topChains.map(chain => ({
        label: chain,
        data: months.map(month => monthlyData[month][chain] || 0),
        backgroundColor: CHAIN_COLORS[chain] || CHAIN_COLORS['Other'],
        borderColor: CHAIN_COLORS[chain] || CHAIN_COLORS['Other'],
        borderWidth: 1
    }));

    // Add "Other" category if needed
    const otherData = months.map(month => {
        const monthData = monthlyData[month];
        return Object.entries(monthData)
            .filter(([chain]) => !topChains.includes(chain))
            .reduce((sum, [, amount]) => sum + amount, 0);
    });

    if (otherData.some(value => value > 0)) {
        datasets.push({
            label: 'Other',
            data: otherData,
            backgroundColor: CHAIN_COLORS['Other'],
            borderColor: CHAIN_COLORS['Other'],
            borderWidth: 1
        });
    }

    if (earningsChart) {
        earningsChart.destroy();
    }

    earningsChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: months.map(month => formatDate(month + '-01')),
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#ffffff',
                        usePointStyle: true
                    }
                },
                tooltip: {
                    backgroundColor: '#1a1a1a',
                    titleColor: '#ffffff',
                    bodyColor: '#cccccc',
                    borderColor: '#333333',
                    borderWidth: 1,
                    callbacks: {
                        afterBody: function(context) {
                            const dataIndex = context[0].dataIndex;
                            const month = months[dataIndex];
                            const monthData = monthlyData[month];
                            const chainName = context[0].dataset.label;
                            
                            if (chainName === 'Other') return '';
                            
                            // Find full data for this chain and month
                            const fullData = revenueData.find(row => 
                                row.dt_month.toISOString().substring(0, 7) === month && 
                                row.display_name === chainName
                            );
                            
                            if (fullData) {
                                return [
                                    `L2 Fees: ${formatCurrency(parseNumericValue(fullData.l2_fees_as_superchain))}`,
                                    `L1 Costs: ${formatCurrency(parseNumericValue(fullData.l1_costs_as_superchain))}`,
                                    `Chain Governor Profit: ${formatCurrency(parseNumericValue(fullData.chain_governor_profit_estimated))}`
                                ];
                            }
                            return '';
                        }
                    }
                }
            },
            scales: {
                x: {
                    stacked: true,
                    ticks: {
                        color: '#cccccc'
                    },
                    grid: {
                        color: '#333333'
                    }
                },
                y: {
                    stacked: true,
                    ticks: {
                        color: '#cccccc',
                        callback: function(value) {
                            return formatCurrency(value);
                        }
                    },
                    grid: {
                        color: '#333333'
                    }
                }
            }
        }
    });
}

// Financial Statement Table Functions
function updateFinancialTable() {
    const tbody = financialTableElement.querySelector('tbody');
    const thead = financialTableElement.querySelector('thead tr');
    
    // Clear existing content
    thead.innerHTML = '<th>Metric</th>';
    tbody.innerHTML = '';

    if (revenueData.length === 0) return;

    // Get unique months
    const months = [...new Set(revenueData.map(row => 
        row.dt_month.toISOString().substring(0, 7)
    ))].sort();

    // Add month headers
    months.forEach(month => {
        const th = document.createElement('th');
        th.textContent = formatDate(month + '-01');
        thead.appendChild(th);
    });

    // Prepare data rows
    const metrics = [
        { key: 'l2_fees_as_superchain', label: 'L2 Fees' },
        { key: 'l1_costs_as_superchain', label: 'L1 Costs' },
        { key: 'net_onchain_profit_as_superchain', label: 'Net Onchain Profit' },
        { key: 'chain_governor_profit_estimated', label: 'Chain Governor Profit' },
        { key: 'revshare_estimated', label: 'Revshare to Optimism' }
    ];

    metrics.forEach(metric => {
        const row = document.createElement('tr');
        
        // Metric label
        const labelCell = document.createElement('td');
        labelCell.textContent = metric.label;
        row.appendChild(labelCell);

        // Monthly values
        months.forEach(month => {
            const monthData = revenueData.filter(row => 
                row.dt_month.toISOString().substring(0, 7) === month
            );
            
            const total = monthData.reduce((sum, row) => sum + parseNumericValue(row[metric.key]), 0);
            
            const cell = document.createElement('td');
            cell.textContent = formatCurrency(total);
            row.appendChild(cell);
        });

        tbody.appendChild(row);
    });
}

// Chain Logos Functions
function updateChainLogos() {
    chainLogosElement.innerHTML = '';

    if (revenueData.length === 0) return;

    // Get latest month data
    const latestMonth = new Date(Math.max(...revenueData.map(d => d.dt_month)));
    const latestData = revenueData.filter(d => 
        d.dt_month.getTime() === latestMonth.getTime()
    );

    latestData
        .sort((a, b) => (b.revshare_estimated || 0) - (a.revshare_estimated || 0))
        .forEach(chain => {
            const logoContainer = document.createElement('div');
            logoContainer.className = 'chain-logo-container';

            const logo = document.createElement('div');
            logo.className = 'chain-logo';
            logo.style.backgroundColor = CHAIN_COLORS[chain.display_name] || CHAIN_COLORS['Other'];
            logo.textContent = CHAIN_ICONS[chain.display_name] || chain.display_name.substring(0, 3).toUpperCase();

            const text = document.createElement('div');
            text.className = 'chain-logo-text';
            text.textContent = chain.display_name;

            // Add hover tooltip
            logo.addEventListener('mouseenter', (e) => {
                const tooltipContent = `
                    <strong>${chain.display_name}</strong><br>
                    Revshare: ${formatCurrency(parseNumericValue(chain.revshare_estimated))}<br>
                    L2 Fees: ${formatCurrency(parseNumericValue(chain.l2_fees_as_superchain))}<br>
                    L1 Costs: ${formatCurrency(parseNumericValue(chain.l1_costs_as_superchain))}<br>
                    Governor Profit: ${formatCurrency(parseNumericValue(chain.chain_governor_profit_estimated))}
                `;
                showTooltip(e, tooltipContent);
            });

            logo.addEventListener('mouseleave', hideTooltip);

            logoContainer.appendChild(logo);
            logoContainer.appendChild(text);
            chainLogosElement.appendChild(logoContainer);
        });
}

// Transactions Table Functions
function updateTransactionsTable() {
    const tbody = transactionsTableElement.querySelector('tbody');
    tbody.innerHTML = '';

    if (transactionData.length === 0) return;

    // Show latest 20 transactions
    transactionData.slice(0, 20).forEach(tx => {
        const row = document.createElement('tr');

        // Timestamp
        const timeCell = document.createElement('td');
        timeCell.textContent = formatDateTime(tx.tx_block_time);
        row.appendChild(timeCell);

        // Chain
        const chainCell = document.createElement('td');
        chainCell.textContent = tx.chain_name;
        row.appendChild(chainCell);

        // Transaction Hash
        const hashCell = document.createElement('td');
        const hashLink = document.createElement('a');
        hashLink.href = (BLOCK_EXPLORERS[tx.chain_name] || '#') + tx.tx_hash;
        hashLink.target = '_blank';
        hashLink.className = 'tx-hash-link';
        hashLink.textContent = tx.tx_hash.substring(0, 10) + '...' + tx.tx_hash.substring(-8);
        hashCell.appendChild(hashLink);
        row.appendChild(hashCell);

        // Value
        const valueCell = document.createElement('td');
        valueCell.textContent = formatCurrency(parseNumericValue(tx.value_decimal));
        row.appendChild(valueCell);

        tbody.appendChild(row);
    });
}

// Time Range Toggle Functions
function updateTimeRange(range) {
    currentTimeRange = range;
    
    // Update button states
    monthlyBtn.classList.toggle('active', range === 'monthly');
    quarterlyBtn.classList.toggle('active', range === 'quarterly');

    // Update all components
    updateHighLevelMetrics();
    createEarningsChart();
    updateFinancialTable();
    updateChainLogos();
}

// Event Listeners
monthlyBtn.addEventListener('click', () => updateTimeRange('monthly'));
quarterlyBtn.addEventListener('click', () => updateTimeRange('quarterly'));

// Mouse move handler for tooltip positioning
document.addEventListener('mousemove', (e) => {
    if (tooltip.classList.contains('visible')) {
        tooltip.style.left = e.pageX + 10 + 'px';
        tooltip.style.top = e.pageY + 10 + 'px';
    }
});

// Hide loading and show content
function hideLoading() {
    loadingElement.style.display = 'none';
    document.querySelectorAll('section').forEach(section => {
        section.classList.add('fade-in');
    });
}

// Initialize Dashboard
async function initializeDashboard() {
    try {
        console.log('Loading dashboard data...');
        
        await loadAllData();
        
        console.log('Updating dashboard components...');
        updateHighLevelMetrics();
        createEarningsChart();
        updateFinancialTable();
        updateChainLogos();
        updateTransactionsTable();
        
        hideLoading();
        
        console.log('Dashboard initialized successfully');
        
    } catch (error) {
        console.error('Failed to initialize dashboard:', error);
        
        // Show error message
        loadingElement.innerHTML = `
            <div style="text-align: center; color: #ff4757;">
                <h2>Failed to Load Dashboard</h2>
                <p>Error: ${error.message}</p>
                <p>Please check that the CSV files are available and try refreshing the page.</p>
            </div>
        `;
    }
}

// Start the dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', initializeDashboard);