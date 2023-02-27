import pandas as pd


def add_rarity(data):
    traits = data.columns[1:]
    for trait in traits:
        data[trait].fillna(0.0, inplace=True)
        data[trait] = data[trait].astype('float')
        data[str(trait) + '_rarity'] = [( 1 / (data[trait].sum() / len(data))) if row == 1.0 else 0.0 for row in data[trait]]
    data.fillna(0, inplace=True)
    
    if 'trait_count' not in data.columns:
        data['trait_count'] = data[traits].sum(axis=1)
        
    trait_count = data['trait_count'].value_counts().to_dict()
    trait_count_df = pd.DataFrame.from_dict(trait_count, orient='index', columns=['trait_count_rarity']).rename_axis('trait_count').reset_index()
    data = pd.merge(data, trait_count_df, on='trait_count', how='left')
    data['trait_count_rarity'] = data['trait_count_rarity'].apply(lambda x: x / 100)
    
    trait_rarity = data.filter(like='_rarity').columns.tolist()
    data['rarest_trait'] = data[trait_rarity].idxmax(axis=1)
    data['rarest_trait'] = data['rarest_trait'].str.split('_').str[-2]
    
    data['rarity_score'] = data[trait_rarity].sum(axis=1)
    
    return data  