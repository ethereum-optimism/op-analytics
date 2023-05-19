# https://909613235-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2FS3V2Pnr0gJCi6qOnph2I%2Fuploads%2Fw47UOsfUiuQq7Vn42dhj%2FSubgrounds%20Workshop%20%231%20Under%20the%20Hood.pdf?alt=media&token=0819617a-a0a6-4aad-9104-443e2cf5ea6d

from subgrounds.subgrounds import Subgrounds
from subgrounds.pagination import ShallowStrategy, LegacyStrategy
import pandas as pd
import requests as r
import defillama_utils as dfl

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}
sgs = pd.DataFrame(
        [
                 ['l2dao-velodrome','https://api.thegraph.com/subgraphs/name/messari/velodrome-optimism','']
                ,['synthetix-curve','https://api.thegraph.com/subgraphs/name/convex-community/volume-optimism','']
                ,['uniswap','https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis', '']
        ]
        ,columns = ['dfl_id','subgraph_url','query']
)
# curve_op = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/messari/velodrome-optimism")
# display(sgs)


def create_sg(tg_api, sg):
        csg = sg.load_subgraph(tg_api)
        return csg


def get_velodrome_pool_tvl(pid, min_ts = 0, max_ts = 99999999999999):
        sg = Subgrounds()
        velo = create_sg('https://api.thegraph.com/subgraphs/name/messari/velodrome-optimism', sg)
        q1 = velo.Query.liquidityPoolDailySnapshots(
        orderDirection='desc',
        first=max_ts*max_ts, #arbitrarily large number so we pull everything
                where=[
                velo.Query.liquidityPoolDailySnapshot.pool == pid,
                velo.Query.liquidityPoolDailySnapshot.timestamp > min_ts,
                velo.Query.liquidityPoolDailySnapshot.timestamp <= max_ts,
                ]
        )
        velo_tvl = sg.query_df([
                q1.id,
                q1.pool.id,
                q1.timestamp,
                q1.pool.inputTokens.id,
                q1.pool.inputTokens.symbol,
                
                q1.totalValueLockedUSD
                ]
                , pagination_strategy=ShallowStrategy)
        velo_wts = sg.query_df([
                q1.id,
                q1.pool.id,
                q1.timestamp,
                q1.inputTokenWeights,
                ]
                , pagination_strategy=ShallowStrategy)
        velo_reserves = sg.query_df([
                q1.id,
                q1.pool.id,
                q1.timestamp,
                q1.inputTokenBalances,
                ]
                , pagination_strategy=ShallowStrategy)
        
        df_array = [velo_tvl, velo_wts, velo_reserves]

        for df in df_array:
                df.columns = df.columns.str.replace('liquidityPoolDailySnapshots_', '')
                df['id_rank'] = df.groupby(['id']).cumcount()+1

        velo_tvl = velo_tvl.merge(velo_wts, on =['id','id_rank','pool_id','timestamp'])
        velo_tvl = velo_tvl.merge(velo_reserves, on =['id','id_rank','pool_id','timestamp'])

        velo_tvl['timestamp_dt'] = pd.to_datetime(velo_tvl['timestamp'],unit='s')
        velo_tvl['timestamp_day'] = pd.to_datetime(velo_tvl['timestamp'],unit='s').dt.floor('d')

        velo_tvl['inputTokenBalances'] = velo_tvl['inputTokenBalances'] / (10 ** 18)
        velo_tvl['inputToken_tvl'] = velo_tvl['totalValueLockedUSD'] * ( velo_tvl['inputTokenWeights'] / 100 )
        # velo_tvl['inputToken_price'] = velo_tvl['inputToken_tvl'] / velo_tvl['inputTokenBalances']

        #Standardize Columns
        # date	token	token_value	usd_value	protocol
        velo_tvl['protocol'] = 'Velodrome'
        velo_tvl = velo_tvl[['timestamp_day','pool_inputTokens_symbol','inputTokenBalances','inputToken_tvl','protocol']]
        velo_tvl = velo_tvl.rename(columns={
                'timestamp_day':'date',
                'pool_inputTokens_symbol':'token',
                'inputTokenBalances':'token_value',
                'inputToken_tvl':'usd_value'
        })

        return velo_tvl



def get_curve_pool_tvl(pid, min_ts = 0, max_ts = 99999999999999):
        sg = Subgrounds()
        curve = create_sg('https://api.thegraph.com/subgraphs/name/convex-community/volume-optimism', sg)
        q1 = curve.Query.dailyPoolSnapshots(
                orderBy= curve.DailyPoolSnapshot.timestamp,
                orderDirection='desc',
                first=max_ts*max_ts, #arbitrarily large number so we pull everything
                        where=[
                        curve.DailyPoolSnapshot.pool == pid,
                        curve.DailyPoolSnapshot.timestamp > min_ts,
                        curve.DailyPoolSnapshot.timestamp <= max_ts,
                        ]
        )
        curve_tvl = sg.query_df([
                q1.id,
                q1.pool.address,
                q1.pool.name,
                q1.pool.symbol,
                q1.timestamp,
                # q1.tvl,
                # q1.adminFeesUSD,
                # q1.lpFeesUSD,
                q1.pool.coinNames,
                # q1.normalizedReserves,
                # q1.reservesUSD,
                ]
                , pagination_strategy=ShallowStrategy)
        curve_reserves_normal = sg.query_df([
                q1.id,
                q1.pool.address,
                q1.timestamp,
                q1.normalizedReserves,
                # q1.pool.coinNames,
                
                # q1.reservesUSD
                ]
                , pagination_strategy=ShallowStrategy)
        curve_reserves_usd = sg.query_df([
                q1.id,
                q1.pool.address,
                q1.timestamp,
                q1.reservesUSD
                ]
                , pagination_strategy=ShallowStrategy)

        df_array = [curve_tvl, curve_reserves_normal, curve_reserves_usd]

        for df in df_array:
                df.columns = df.columns.str.replace('dailyPoolSnapshots_', '')
                df['id_rank'] = df.groupby(['id']).cumcount()+1

        curve_tvl = curve_tvl.merge(curve_reserves_normal, on =['id','id_rank','pool_address','timestamp'])
        curve_tvl = curve_tvl.merge(curve_reserves_usd, on =['id','id_rank','pool_address','timestamp'])

        curve_tvl['normalizedReserves'] = curve_tvl['normalizedReserves'] / ( 10 ** 18 ) #decimal adjust
        # curve_tvl['reservePrice'] = curve_tvl['reservesUSD'] / curve_tvl['normalizedReserves'] 
        curve_tvl['timestamp_dt'] = pd.to_datetime(curve_tvl['timestamp'],unit='s')

        #Standardize Columns
        # date	token	token_value	usd_value	protocol
        curve_tvl['protocol'] = 'Curve'
        curve_tvl = curve_tvl[['timestamp_dt','pool_coinNames','normalizedReserves','reservesUSD','protocol']]
        curve_tvl = curve_tvl.rename(columns={
                'timestamp_dt':'date',
                'pool_coinNames':'token',
                'normalizedReserves':'token_value',
                'reservesUSD':'usd_value'
        })

        return curve_tvl

def get_curve_pool_tvl_and_volume(chain, min_tvl = 10000, min_ts = 0, max_ts = 99999999999999):
        sg = Subgrounds()
        # Playground: https://thegraph.com/hosted-service/subgraph/convex-community/volume-optimism
        curve = create_sg('https://api.thegraph.com/subgraphs/name/convex-community/volume-' + str.lower(chain), sg)
        q1 = curve.Query.dailyPoolSnapshots(
        orderBy=curve.DailyPoolSnapshot.tvl,
        orderDirection='desc',
        first=max_ts*max_ts, #arbitrarily large number so we pull everything
                where=[
                curve.DailyPoolSnapshot.timestamp > min_ts,
                curve.DailyPoolSnapshot.timestamp <= max_ts,
                curve.DailyPoolSnapshot.tvl >= min_tvl,
                ]
        )
        curve_tvl = sg.query_df([
                q1.id,
                q1.timestamp,
                q1.pool,
                # q1.pool.name,
                # q1.pool.symbol,
                # q1.pool.coins,
                # q1.pool.coins,
                q1.pool.coinNames,
                q1.tvl,
                q1.lpFeesUSD,
                q1.adminFeesUSD,
                q1.totalDailyFeesUSD,
                q1.fee
                ]
                , pagination_strategy=ShallowStrategy)
        curve_tvl.columns = curve_tvl.columns.str.replace('dailyPoolSnapshots_', '')
        # print(curve_tvl.columns)
        curve_tvl['id_rank'] = curve_tvl.groupby(['id']).cumcount()+1
        

        grp = curve_tvl.groupby(['timestamp','pool_address','pool_name','pool_symbol','pool_lpToken','pool_isV2',\
                                 'pool_assetType','pool_poolType','tvl'\
                                 ,'lpFeesUSD', 'adminFeesUSD'\
                                        ,'totalDailyFeesUSD','fee'
                                        ]).\
                                agg({'pool_coinNames':lambda x: list(x.unique())}
                                     )
        grp.reset_index(inplace=True)

        # Assume Fees / fee rate = original volume
        grp['daily_trade_voume_usd'] = grp['totalDailyFeesUSD'] / grp['fee'] 

        #Map asset type
        mappings = {0: "USD", 1: "ETH", 2: "BTC", 3: "Other", 4: "Crypto"}
        grp['pool_assetType_mapped'] = grp['pool_assetType'].map(mappings)
        grp['dt'] = pd.to_datetime(grp['timestamp'], unit = 's')
        grp['chain'] = chain

        # convert the arrays to strings, sort the strings, and convert back to arrays
        grp['pool_coinNames'] = grp['pool_coinNames'].apply(lambda x: sorted(x))
        grp['pool_coinNames'] = grp['pool_coinNames'].apply(lambda x: [i if i != '0xeeee' else 'ETH' for i in x])
        grp['pool_coinNames'] = grp['pool_coinNames'].apply(lambda x: ','.join(map(str, x)))

        #cols to include
        cols = ['dt','chain','pool_address','pool_lpToken','pool_name','pool_symbol','pool_coinNames','pool_assetType_mapped','pool_poolType'\
                        ,'tvl','daily_trade_voume_usd','totalDailyFeesUSD','fee']
        grp = grp[cols]

        grp = grp.sort_values(by=['dt','daily_trade_voume_usd'],ascending=[False,False])


        return grp


def get_messari_format_pool_tvl(slug, pool_id, chain = 'optimism', min_ts = 0, max_ts = 99999999999999):
        sg = Subgrounds()
        msr_dfs = []
        # print(slug)
        sg_query = create_sg('https://api.thegraph.com/subgraphs/name/messari/' + slug + '-' + chain, sg).Query
        # Get Query
        pool_info = sg_query.liquidityPools(
        # orderBy=sg_query.liquidityPools.timestamp,
        # orderDirection='desc',
        first=10000,#max_ts*max_ts, #arbitrarily large number so we pull everything
                where=[
                # sg_query.liquidityPools.timestamp > min_ts,
                # sg_query.liquidityPools.timestamp <= max_ts,
                sg_query.liquidityPool.id == pool_id
                ]
        )
        snapshots = sg_query.liquidityPoolDailySnapshots(
        orderBy= sg_query.liquidityPoolDailySnapshot.timestamp,
        orderDirection='desc',
        first=10000,#max_ts*max_ts, #arbitrarily large number so we pull everything
                where=[
                sg_query.liquidityPoolDailySnapshot.timestamp > min_ts,
                sg_query.liquidityPoolDailySnapshot.timestamp <= max_ts,
                sg_query.liquidityPoolDailySnapshot.pool == pool_id
                ]
        )
        # Pull Fields
        pool_lst = sg.query_df([
                pool_info.id,
                pool_info.inputTokens.id,
                pool_info.inputTokens.symbol,
                pool_info.inputTokens.decimals,
                pool_info.inputTokens.lastPriceUSD
        ]
        , pagination_strategy=ShallowStrategy)
        #Snapshots
        snap_lst = sg.query_df([
                snapshots.id,
                snapshots.timestamp,
                #input tokens
                snapshots.inputTokenBalances,
                # q1.inputTokenBalances ,
                # #pull all pool info
                # q1.pool.name,
                # q1.pool.symbol,
                # q1.pool.inputTokens.id
        ])
        # print(msr_daily)
        # msr_daily = pd.concat(snap_lst)
        #fix up column names

        snap_lst.columns = snap_lst.columns.str.replace('liquidityPoolDailySnapshots_', '')
        snap_lst = snap_lst.rename(columns={'id':'pool_date_id'})
        
        pool_lst.columns = pool_lst.columns.str.replace('liquidityPools_', '')
        pool_lst = pool_lst.rename(columns={'id':'pool_id'})

        # GET TOKEN PRICES FROM LLAMA
        token_list = pool_lst['inputTokens_id'].drop_duplicates().to_list()

        prices = dfl.get_historical_defillama_prices(token_list, chain, min_ts)
        prices = prices.rename(columns={'token_address':'inputTokens_id'})

        pool_lst = pool_lst[['pool_id','inputTokens_id','inputTokens_lastPriceUSD','inputTokens_symbol','inputTokens_decimals']]


        snap_lst['token_order'] = snap_lst.groupby('pool_date_id')['pool_date_id'].cumcount() + 1
        pool_lst['token_order'] = pool_lst.groupby('pool_id')['pool_id'].cumcount() + 1

        snap_lst['date'] = pd.to_datetime(snap_lst['timestamp'], unit='s').dt.date

        data_df = snap_lst.merge(pool_lst,on='token_order',how='left')
        data_df = data_df.merge(prices,on=['inputTokens_id','date'],how='left')
        data_df['token_price'] = data_df['price'].combine_first(data_df['inputTokens_lastPriceUSD']) #prefer defillama's 'price'

        data_df['token_balance'] = data_df['inputTokenBalances'] / 10**data_df['inputTokens_decimals']
        data_df['usd_balance'] = data_df['token_balance'] * data_df['token_price']

        data_df = data_df.rename(columns={
                'symbol':'token',
                'token_balance':'token_value',
                'usd_balance':'usd_value'
        })

        data_df = data_df[['date','token','token_value','usd_value']]

        return data_df



def get_hop_pool_tvl(pid, min_ts = 0, max_ts = 99999999999999):
    prot_str = 'hop-protocol'
    hop = dfl.get_single_tvl(prot_str, ['Optimism'])
    hop = hop[(hop['token'] == pid) & (~hop['token_value'].isna())]
    hop = hop[['date','token','token_value','usd_value','protocol']]
    hop['protocol'] = 'Hop' #rename to match func
    hop.reset_index(inplace=True,drop=True)
    return hop

# Note, this is not in TVL tracking format - maybe we split this to a new file ~eventually
def get_messari_sg_pool_snapshots(slug, chains = ['optimism'], min_ts = 0, max_ts = 99999999999999):
        sg = Subgrounds()
        msr_dfs = []
        print(slug)
        for c in chains:
                print(c)
                try:
                        # Set Chain
                        curve = create_sg('https://api.thegraph.com/subgraphs/name/messari/' + slug + '-' + c, sg)
                        # Get Query
                        q1 = curve.Query.liquidityPoolDailySnapshots(
                        orderBy=curve.Query.liquidityPoolDailySnapshot.timestamp,
                        orderDirection='desc',
                        first=100000,#max_ts*max_ts, #arbitrarily large number so we pull everything
                                where=[
                                curve.Query.liquidityPoolDailySnapshot.timestamp > min_ts,
                                curve.Query.liquidityPoolDailySnapshot.timestamp <= max_ts,
                                curve.Query.liquidityPoolDailySnapshot.pool == '0xfb6fe7802ba9290ef8b00ca16af4bc26eb663a28'
                                ]
                        )
                        # Pull Fields
                        msr_list = sg.query_df([
                                q1.id,
                                q1.timestamp,
                                q1.totalValueLockedUSD,
                                q1.dailyVolumeUSD,
                                q1.rewardTokenEmissionsUSD,
                                #protocol
                                q1.protocol.id,
                                q1.protocol.name,
                                q1.protocol.slug,
                                q1.protocol.network,
                                #pool
                                q1.pool.id,
                                q1.pool.name,
                                q1.pool.symbol,
                                q1.pool.inputTokens.id
                        ]
                        , pagination_strategy=ShallowStrategy)
                        msr_df = pd.concat(msr_list)
                except:
                        msr_df = pd.DataFrame()
                        continue
                msr_dfs.append(msr_df)
        
        #combine all chains
        msr_daily = pd.concat(msr_dfs)

        #fix up column names

        msr_daily.columns = msr_daily.columns.str.replace('liquidityPoolDailySnapshots_', '')
        
        col_list = msr_daily.columns.to_list()
        print(col_list)
        col_list.remove('pool_inputTokens_id') # we want to group by everything else 
        
        msr_daily = msr_daily.fillna(0)

        msr_daily = msr_daily.groupby(col_list).agg({'pool_inputTokens_id':lambda x: list(x)})
        msr_daily.reset_index(inplace=True)

        msr_daily['timestamp'] = pd.to_datetime(msr_daily['timestamp'],unit='s')
        msr_daily['date'] = msr_daily['timestamp'].dt.floor('d')
        msr_daily['id_rank'] = msr_daily.groupby(['id']).cumcount()+1
        msr_daily = msr_daily[msr_daily['pool_id'] != 0] #weird....
        # display(msr_daily[msr_daily['id']=='0x445fe580ef8d70ff569ab36e80c647af338db351-19258'])
        return pd.DataFrame(msr_daily)