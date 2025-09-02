def register_udfs(con) -> None:
    # Simple example UDF; add any project-wide functions here.
    def is_positive(x: float) -> bool:
        return x is not None and x > 0.0
    con.create_function("is_positive", is_positive, replace=True)
