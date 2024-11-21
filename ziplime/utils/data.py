def get_fundamental_data(bar_data, assets, fields, bar_count, frequency):
    if frequency != '1q':
        raise Exception("Currently only frequency of 1 quarter is supported for fundamental data")
    frequency = '1d'
    df = bar_data.history(
        assets, fields, bar_count, frequency
    )
    return df[df != 0]
