




def collection_wrangler(data, traits):
    df = data.copy()
    
    # dtype conversion and sort
    df['assetId'] = df['assetId'].astype(int)
    df['blockTimestamp'] = pd.to_datetime(df['blockTimestamp'], utc=True)
    df = df.sort_values(by='blockTimestamp')
    df = df[df['blockTimestamp'].isna() == False]
    df.set_index('blockTimestamp', inplace=True)
    
    df = scrub_marketaddresses(df)

    df['usdPrice'].fillna(0, inplace=True)
    df['usdPrice'] = pd.to_numeric(df['usdPrice'])
    df = df[df['usdPrice'] > df['usdPrice'].quantile(0.1)]

    subset = df.copy()
    df = subset.sort_values(by='blockTimestamp').tail(int(len(subset)*(80/100)))

    df['blockTimestamp'] = df.index

    df['project_average'] = df['usdPrice'].expanding().mean().shift()

    # fill missing data
    df = fill_missing_data(df)
    df = df[df['saleType'] == 'secondary']
    
    # 3 day rolling average
    days_to_avg = [3, 7, 30, 60]
    for day in days_to_avg:
        df[str(day) + 'day_ma'] = df['usdPrice'].rolling(f'{day}d', min_periods=1, closed='left').mean()

    faux_columns = df.filter(like='meta_').columns.tolist()
    faux_columns = [x for x in faux_columns if not x.endswith('_rarity')]
    for col in faux_columns:
        df[str(col) + '_3qty_rolling_avg'] = 0.0
        df[str(col) + '_30day_rolling_avg'] = 0.0
        df[str(col) + '_90day_rolling_avg'] = 0.0

    # rarity score for each trait and weighted average
    for trait in traits:
        df = df.copy()
        df[str(trait) + '_3qty_rolling_avg'] = df.groupby(trait)['usdPrice'].transform(lambda x: x.rolling(3, min_periods=1, closed='left').mean())
        df[str(trait) + '_3qty_rolling_avg'] = np.where(df[trait] == 0, 0, df[str(trait) + '_3qty_rolling_avg'])
        df[str(trait) + '_30day_rolling_avg'] = df.groupby(trait)['usdPrice'].transform(lambda x: x.rolling(window='30d', closed='left').mean())
        df[str(trait) + '_30day_rolling_avg'] = np.where(df[trait] == 0, 0, df[str(trait) + '_30day_rolling_avg'])
        df[str(trait) + '_90day_rolling_avg'] = df.groupby(trait)['usdPrice'].transform(lambda x: x.rolling(window='90d', closed='left').mean())
        df[str(trait) + '_90day_rolling_avg'] = np.where(df[trait] == 0, 0, df[str(trait) + '_90day_rolling_avg'])
    df.fillna(0, inplace=True)

    df.reset_index(drop=True, inplace=True)
    
    traits_3qty_rolling_avg = df.filter(like='3qty_rolling_avg').columns.tolist()
    df['rolling_avg_trait_mean'] = df[traits_3qty_rolling_avg].replace(0, np.nan).mean(axis=1).replace(np.nan, 0)
    df['rarest_trait_price'] = df[traits_3qty_rolling_avg].max(axis=1)
    
    df['last_sale_rarest_trait'] = df.groupby('rarest_trait')['usdPrice'].shift(1)
    df.fillna(0, inplace=True)
    
    df['rarest_trait_price_mean'] = df.groupby('rarest_trait')['rarest_trait_price'].transform(lambda x: x.expanding().mean())

    # last price and last sale date
    df = df.sort_values(['assetId','blockTimestamp'], ascending=[True, True])

    df['ETH_usdPrice'] = (pd.to_numeric(df['usdPrice']) / df['totalDecimalPrice']).round(2)
    historical = price_collection()
    price = historical.find({'blockchain': None, 'symbol': 'eth'}).sort('date', -1).limit(1).next()
    df['ETH_usdPrice'] = df['ETH_usdPrice'].replace(np.nan, price['open'])
    
    df = add_datepart(df, 'blockTimestamp', drop=False)

    # Data leak
    columns_to_drop = ['totalDecimalPrice']
    df = df.drop(columns_to_drop, axis=1)

    df.reset_index(drop=True, inplace=True)
    df = df.set_index('assetId')

    project = df['project'].iloc[0]
    ticker = df['nftTicker'].iloc[0].lower()

    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('<', '_')
    df.columns = df.columns.str.replace('>', '_')

    df.sort_index(inplace=True)
    return df