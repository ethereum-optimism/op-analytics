from datetime import date


def evaluate_buffer(process_dt: date):
    """Evaluate and remove bad data from the buffer.

    Find dt values that are incomplete at process_dt. Alter table to delete data
    from the imcomplete dt values. This preents us from writing bad data to GCS.
    """
    pass
