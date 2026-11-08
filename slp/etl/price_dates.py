from slp.etl.price_relations import load_relation


def run_price_dates_etl() -> dict:
    return load_relation("price_dates")
