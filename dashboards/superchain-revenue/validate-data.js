// Data Validation Script for Superchain Revenue Dashboard
// Run this in browser console to validate CSV data structure

async function validateDashboardData() {
    console.log('🔍 Validating dashboard data...');
    
    const results = {
        revenue: { valid: false, errors: [] },
        transactions: { valid: false, errors: [] },
        marketShare: { valid: false, errors: [] }
    };

    // Validation schemas
    const schemas = {
        revenue: {
            required: ['dt_month', 'chain_name', 'display_name', 'l2_fees_as_superchain', 
                      'l1_costs_as_superchain', 'net_onchain_profit_as_superchain', 
                      'revshare_estimated', 'chain_governor_profit_estimated'],
            types: {
                dt_month: 'date',
                l2_fees_as_superchain: 'number',
                l1_costs_as_superchain: 'number',
                net_onchain_profit_as_superchain: 'number',
                revshare_estimated: 'number',
                chain_governor_profit_estimated: 'number'
            }
        },
        transactions: {
            required: ['dt_month', 'chain_name', 'is_op_transfer', 'tx_block_time', 
                      'tx_hash', 'value_decimal'],
            types: {
                dt_month: 'date',
                tx_block_time: 'date',
                value_decimal: 'number',
                is_op_transfer: 'boolean'
            }
        },
        marketShare: {
            required: ['dt_month', 'Metric', 'value'],
            types: {
                dt_month: 'date',
                value: 'number'
            }
        }
    };

    // Helper functions
    function isValidDate(dateString) {
        const date = new Date(dateString);
        return !isNaN(date.getTime());
    }

    function validateRow(row, schema, rowIndex) {
        const errors = [];
        
        // Check required fields
        schema.required.forEach(field => {
            if (!(field in row) || row[field] === null || row[field] === undefined || row[field] === '') {
                errors.push(`Row ${rowIndex + 1}: Missing required field "${field}"`);
            }
        });

        // Check data types
        Object.entries(schema.types).forEach(([field, expectedType]) => {
            if (field in row && row[field] !== null && row[field] !== undefined && row[field] !== '') {
                const value = row[field];
                
                switch (expectedType) {
                    case 'date':
                        if (!isValidDate(value)) {
                            errors.push(`Row ${rowIndex + 1}: Invalid date format in "${field}": ${value}`);
                        }
                        break;
                    case 'number':
                        if (isNaN(Number(value))) {
                            errors.push(`Row ${rowIndex + 1}: Invalid number in "${field}": ${value}`);
                        }
                        break;
                    case 'boolean':
                        if (value !== true && value !== false && value !== 'true' && value !== 'false') {
                            errors.push(`Row ${rowIndex + 1}: Invalid boolean in "${field}": ${value}`);
                        }
                        break;
                }
            }
        });

        return errors;
    }

    // Test data loading
    try {
        // Revenue data
        try {
            const revenueResponse = await fetch('revenue_data.csv');
            const revenueText = await revenueResponse.text();
            
            const revenueData = await new Promise((resolve, reject) => {
                Papa.parse(revenueText, {
                    header: true,
                    skipEmptyLines: true,
                    dynamicTyping: true,
                    complete: (results) => resolve(results.data),
                    error: (error) => reject(error)
                });
            });

            console.log(`📊 Revenue data: ${revenueData.length} rows loaded`);
            
            revenueData.forEach((row, index) => {
                const errors = validateRow(row, schemas.revenue, index);
                results.revenue.errors.push(...errors);
            });

            results.revenue.valid = results.revenue.errors.length === 0;
            
        } catch (error) {
            results.revenue.errors.push(`Failed to load revenue data: ${error.message}`);
        }

        // Transaction data
        try {
            const txResponse = await fetch('op_collective_revenue_transactions.csv');
            const txText = await txResponse.text();
            
            const txData = await new Promise((resolve, reject) => {
                Papa.parse(txText, {
                    header: true,
                    skipEmptyLines: true,
                    dynamicTyping: true,
                    complete: (results) => resolve(results.data),
                    error: (error) => reject(error)
                });
            });

            console.log(`💸 Transaction data: ${txData.length} rows loaded`);
            
            txData.forEach((row, index) => {
                const errors = validateRow(row, schemas.transactions, index);
                results.transactions.errors.push(...errors);
            });

            results.transactions.valid = results.transactions.errors.length === 0;
            
        } catch (error) {
            results.transactions.errors.push(`Failed to load transaction data: ${error.message}`);
        }

        // Market share data
        try {
            const msResponse = await fetch('Superchain_market_share.csv');
            const msText = await msResponse.text();
            
            const msData = await new Promise((resolve, reject) => {
                Papa.parse(msText, {
                    header: true,
                    skipEmptyLines: true,
                    dynamicTyping: true,
                    complete: (results) => resolve(results.data),
                    error: (error) => reject(error)
                });
            });

            console.log(`📈 Market share data: ${msData.length} rows loaded`);
            
            msData.forEach((row, index) => {
                const errors = validateRow(row, schemas.marketShare, index);
                results.marketShare.errors.push(...errors);
            });

            results.marketShare.valid = results.marketShare.errors.length === 0;
            
        } catch (error) {
            results.marketShare.errors.push(`Failed to load market share data: ${error.message}`);
        }

    } catch (error) {
        console.error('❌ Validation failed:', error);
        return results;
    }

    // Print results
    console.log('\n📋 Validation Results:');
    console.log('======================');
    
    Object.entries(results).forEach(([dataset, result]) => {
        const status = result.valid ? '✅' : '❌';
        console.log(`${status} ${dataset}: ${result.valid ? 'Valid' : `${result.errors.length} errors`}`);
        
        if (!result.valid && result.errors.length > 0) {
            result.errors.forEach(error => console.log(`   - ${error}`));
        }
    });

    const allValid = Object.values(results).every(r => r.valid);
    console.log(`\n🎯 Overall Status: ${allValid ? '✅ All data is valid!' : '❌ Data validation failed'}`);
    
    return results;
}

// Auto-run validation if Papa (CSV parser) is available
if (typeof Papa !== 'undefined') {
    validateDashboardData();
} else {
    console.log('⚠️  Papa Parse not loaded. Include PapaParse library to run validation.');
}