def validate(ds):
    """
    Given a potential datasource description, return its canonical form.
    """

    return ds

def unpack(ds):
    """
    Instantiate and unpack a datasource.
    """

    selector, args = ds[0], ds[1:]

    for s in selector(*args):
        yield s
