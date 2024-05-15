import os
import dotenv
import clickhouse_connect as cc

dotenv.load_dotenv()
# Get OPLabs DB Credentials
env_ch_host = os.getenv("OP_CLICKHOUSE_HOST")
env_ch_user = os.getenv("OP_CLICKHOUSE_USER")
env_ch_pw = os.getenv("OP_CLICKHOUSE_PW")
env_ch_port = os.getenv("OP_CLICKHOUSE_PORT")

def connect_to_clickhouse_db( #Default is OPLabs DB
                ch_host = env_ch_host
                , ch_port = env_ch_port
                , ch_user = env_ch_user
                , ch_pw = env_ch_pw
                ):
        client = cc.get_client(host=ch_host, port=ch_port, username=ch_user, password=ch_pw, secure=True)
        return client