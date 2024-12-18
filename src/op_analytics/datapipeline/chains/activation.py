from datetime import date


from op_analytics.coreutils.time import date_fromstr


CHAIN_ACTIVATION_DATES = {
    "automata": date_fromstr("2024-07-17"),
    "base": date_fromstr("2023-06-15"),
    "bob": date_fromstr("2024-04-11"),
    "cyber": date_fromstr("2024-04-18"),
    "fraxtal": date_fromstr("2024-02-01"),
    "ham": date_fromstr("2024-05-24"),
    "ink": date_fromstr("2024-12-06"),
    "kroma": date_fromstr("2023-09-05"),
    "lisk": date_fromstr("2024-05-03"),
    "lyra": date_fromstr("2023-11-15"),
    "metal": date_fromstr("2024-03-27"),
    "mint": date_fromstr("2024-05-13"),
    "mode": date_fromstr("2023-11-16"),
    "op": date_fromstr("2021-11-12"),
    "orderly": date_fromstr("2023-10-06"),
    "polynomial": date_fromstr("2024-06-10"),
    "race": date_fromstr("2024-07-08"),
    "redstone": date_fromstr("2024-04-03"),
    "shape": date_fromstr("2024-07-23"),
    "swan": date_fromstr("2024-06-18"),
    "unichain": date_fromstr("2024-11-04"),
    "worldchain": date_fromstr("2024-06-25"),
    "xterio": date_fromstr("2024-05-24"),
    "zora": date_fromstr("2023-06-13"),
    # TESTNETS
    "ink_sepolia": date_fromstr("2024-10-15"),
    "op_sepolia": date_fromstr("2024-01-01"),
    "unichain_sepolia": date_fromstr("2024-09-19"),
}


def is_chain_active(chain: str, dateval: date) -> bool:
    activation = CHAIN_ACTIVATION_DATES[chain]

    return dateval >= activation
