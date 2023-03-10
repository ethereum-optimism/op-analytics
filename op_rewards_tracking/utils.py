# -*- coding: utf-8 -*-
def format_number(num, decimal=1):
    """
    Convert a number to a string in the format of K, M, B, T.
    """
    suffixes = {1000000000000: 'T', 1000000000: 'B', 1000000: 'M', 1000: 'K'}
    for suf in suffixes.keys():
        if num >= suf:
            val = num / suf
            return f"{val:.{decimal}f}{suffixes[suf]}"
    return f"{num:.{decimal}f}"
