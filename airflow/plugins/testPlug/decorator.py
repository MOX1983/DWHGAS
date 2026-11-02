


def spark_task(application: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(application=application, *args, **kwargs)

        return wrapper

    return decorator