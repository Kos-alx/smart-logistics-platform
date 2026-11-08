from slp.etl.price_relations import load_relation


def run_price_body_types_etl() -> dict:
    return load_relation("price_body_types")
