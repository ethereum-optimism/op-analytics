REGISTERED_INTERMEDIATE_MODELS = {}


def register(func):
    REGISTERED_INTERMEDIATE_MODELS[func.__name__] = func
