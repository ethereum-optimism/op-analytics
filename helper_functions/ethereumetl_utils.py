# from ethereum_etl import get_ethereum_etl
import subprocess
import os

def get_ethereum_etl(start_block = 0, end_block = 1000, output_folder = "tx_outputs", uri = '', max_w = '8'):
        blocks_output = 'downloads/' + output_folder + '/blocks.csv'
        transactions_output = 'downloads/' + output_folder + '/transactions.csv'

        command = f"ethereumetl export_blocks_and_transactions --start-block {start_block} --end-block {end_block} --blocks-output {blocks_output} --transactions-output {transactions_output} --provider-uri {uri} --max-workers {max_w}"
        # print(command)
        # !{command}
        subprocess.run(command, shell=True)

def get_eth_etl_receipts(output_folder, uri):
        #https://github.com/blockchain-etl/ethereum-etl/blob/develop/docs/commands.md#export_receipts_and_logs
        transactions_output = 'downloads/' + output_folder + '/transactions.csv'
        receipts_output = 'downloads/' + output_folder + '/receipts.csv'
        transaction_hash_output = 'downloads/' + output_folder + '/transaction_hashes.txt'

        command_t = f"ethereumetl extract_csv_column --input {transactions_output} --column hash --output {transaction_hash_output}"
        # print(command_t)
        # !{command_t}
        subprocess.run(command_t, shell=True)
        

        command_r = f"ethereumetl export_receipts_and_logs --transaction-hashes {transaction_hash_output} --provider-uri {uri} --receipts-output {receipts_output}"
        # print(command_r)
        # !{command_r}
        subprocess.run(command_r, shell=True)

# https://github.com/blockchain-etl/ethereum-etl/blob/develop/docs/commands.md#stream
def stream_eth_etl(start_block = 0, uri = '', includes = 'block,transaction,log,token_transfer,trace,contract,token', \
                        pg_user = '', pg_pw = '', pg_host = '', pg_port = '', pg_db_name = ''):

        # Check if last_synced_block.txt exists
        if os.path.exists('last_synced_block.txt'):
                command = f"ethereumetl stream --provider-uri {uri} -e {includes} --output=postgresql+pg8000://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db_name}"
        else:
                command = f"ethereumetl stream --provider-uri {uri} --start-block {start_block} -e {includes} --output=postgresql+pg8000://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db_name}"
        # print(command)
        # !{command}
        subprocess.run(command, shell=True)