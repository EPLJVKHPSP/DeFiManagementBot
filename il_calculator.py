from pycoingecko import CoinGeckoAPI


def get_price(crypto_name):
    """ Getting assets price at CoinGeckoAPI"""
    cg_client = CoinGeckoAPI()
    try:
        data = cg_client.get_price(ids=crypto_name, vs_currencies='usd')
        return data[crypto_name]['usd']
    except Exception as e:
        print(f"Failed to get price: {e}")
        return None


def il_calculate(crypto2_qty_after=None, crypto2_qty_before=None,
                 crypto1_qty_before=None, crypto1_qty_after=None,
                 fee=None, crypto1_name=None, crypto2_name=None):
    """ Calcualtion"""
    # Get prices for cryptos
    price1_after = get_price(crypto1_name)
    price2_after = get_price(crypto2_name)
    # Return formula's calculations results
    return ((crypto2_qty_after - crypto2_qty_before - (
            crypto1_qty_before - crypto1_qty_after) * price1_after / price2_after)) * price2_after + fee
