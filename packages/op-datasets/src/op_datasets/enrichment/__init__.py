from op_datasets.enrichment.destructure import desctructure_event_args

REGISTERED_FUNCTIONS = [desctructure_event_args]

REGISTRY = {_.__name__: _ for _ in REGISTERED_FUNCTIONS}

__all__ = list(REGISTRY.keys()) + ["REGISTRY"]
