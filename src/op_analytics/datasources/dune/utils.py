from datetime import timedelta


from op_analytics.coreutils.time import date_fromstr, now_date


def determine_lookback(
    min_dt: str | None = None,
    max_dt: str | None = None,
    default_n_days: int = 7,
):
    now_dateval = now_date()
    if min_dt is None:
        min_date = now_dateval - timedelta(days=default_n_days)
    else:
        min_date = date_fromstr(min_dt)

    if max_dt is None:
        max_date = now_dateval
    else:
        max_date = date_fromstr(max_dt)

    lookback_start_days = (now_dateval - min_date).days
    lookback_end_days = (now_dateval - max_date).days

    return lookback_start_days, lookback_end_days
