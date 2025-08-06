// Dashboard Configuration
// This file contains customizable settings for the Superchain Revenue Dashboard

const DASHBOARD_CONFIG = {
    // Data file paths (relative to index.html)
    dataFiles: {
        revenue: 'revenue_data.csv',
        transactions: 'op_collective_revenue_transactions.csv',
        marketShare: 'Superchain_market_share.csv'
    },

    // Chain-specific styling and metadata
    chains: {
        'OP Mainnet': {
            color: '#ff0420',
            icon: 'OP',
            explorer: 'https://optimistic.etherscan.io/tx/'
        },
        'Base': {
            color: '#0052ff',
            icon: 'BASE',
            explorer: 'https://basescan.org/tx/'
        },
        'Zora': {
            color: '#ff6b35',
            icon: 'ZORA',
            explorer: 'https://explorer.zora.energy/tx/'
        },
        'Mode': {
            color: '#dffe00',
            icon: 'MODE',
            explorer: 'https://modescan.io/tx/'
        },
        'Unichain': {
            color: '#ff007a',
            icon: 'UNI',
            explorer: 'https://unichain.org/tx/'
        },
        'World': {
            color: '#000000',
            icon: 'WLD',
            explorer: 'https://worldchain.org/tx/'
        },
        'BOB': {
            color: '#f7931a',
            icon: 'BOB',
            explorer: 'https://explorer.gobob.xyz/tx/'
        },
        'Redstone': {
            color: '#ff4757',
            icon: 'RED',
            explorer: 'https://explorer.redstone.xyz/tx/'
        },
        'Metal': {
            color: '#2c3e50',
            icon: 'MTL',
            explorer: 'https://metalscan.io/tx/'
        },
        'Lisk': {
            color: '#1a82ff',
            icon: 'LSK',
            explorer: 'https://blockscout.lisk.com/tx/'
        },
        'Kroma': {
            color: '#00d395',
            icon: 'KRM',
            explorer: 'https://blockscout.kroma.network/tx/'
        },
        'Mint': {
            color: '#00ff88',
            icon: 'MINT',
            explorer: 'https://explorer.mint.fun/tx/'
        },
        'Ink': {
            color: '#8b5cf6',
            icon: 'INK',
            explorer: 'https://explorer.ink.finance/tx/'
        },
        'Shape': {
            color: '#06d6a0',
            icon: 'SHP',
            explorer: 'https://shapescan.xyz/tx/'
        },
        'Celo': {
            color: '#fcff52',
            icon: 'CELO',
            explorer: 'https://celoscan.io/tx/'
        },
        'Swell': {
            color: '#0ea5e9',
            icon: 'SWL',
            explorer: 'https://swellscan.io/tx/'
        },
        'Derive': {
            color: '#9333ea',
            icon: 'DRV',
            explorer: 'https://derivescan.io/tx/'
        },
        'SuperSeed': {
            color: '#10b981',
            icon: 'SEED',
            explorer: 'https://superseedscan.io/tx/'
        },
        'Polynomial': {
            color: '#f59e0b',
            icon: 'POLY',
            explorer: 'https://polynomialscan.io/tx/'
        },
        'Arena-Z NOD': {
            color: '#ef4444',
            icon: 'ARENA',
            explorer: 'https://arenascan.io/tx/'
        },
        'Orderly': {
            color: '#06b6d4',
            icon: 'ORD',
            explorer: 'https://orderlyscan.io/tx/'
        },
        'SwanChain': {
            color: '#ec4899',
            icon: 'SWAN',
            explorer: 'https://swanchainscan.io/tx/'
        },
        'Race': {
            color: '#84cc16',
            icon: 'RACE',
            explorer: 'https://racescan.io/tx/'
        },
        'Epic (Ethernity)': {
            color: '#a855f7',
            icon: 'EPIC',
            explorer: 'https://epicscan.io/tx/'
        },
        'Settlus': {
            color: '#14b8a6',
            icon: 'SET',
            explorer: 'https://settlusscan.io/tx/'
        },
        'Soneium': {
            color: '#6366f1',
            icon: 'SONY',
            explorer: 'https://soneiumscan.io/tx/'
        },
        'Hashkey': {
            color: '#f97316',
            icon: 'HASH',
            explorer: 'https://hashkeyscan.io/tx/'
        }
    },

    // Chart settings
    chart: {
        // Minimum ETH amount to show as individual chain (others grouped as "Other")
        minimumChainAmount: 0.1,
        // Maximum number of individual chains to show
        maxChains: 10,
        // Default color for chains not specified above
        defaultColor: '#6c757d'
    },

    // Table settings
    transactions: {
        // Number of transactions to show in the log
        maxTransactions: 20,
        // Hash display format (characters to show from start and end)
        hashDisplayLength: { start: 10, end: 8 }
    },

    // Formatting options
    formatting: {
        // Currency symbol and precision
        currency: {
            symbol: 'ETH',
            decimals: 2
        },
        // Percentage precision
        percentage: {
            decimals: 1
        },
        // Date formatting
        date: {
            monthFormat: { year: 'numeric', month: 'short' },
            dateTimeFormat: { 
                year: 'numeric', 
                month: 'short', 
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            }
        }
    },

    // External links
    links: {
        revshareRules: 'https://github.com/ethereum-optimism/op-analytics'
    },

    // Feature toggles
    features: {
        // Enable/disable quarterly view toggle
        quarterlyView: true,
        // Enable/disable tooltips
        tooltips: true,
        // Enable/disable animations
        animations: true,
        // Enable/disable responsive design
        responsive: true
    }
};

// Export for use in other files
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DASHBOARD_CONFIG;
}