import baseten.bdn


def test_namespace_package_has_no_init() -> None:
    # baseten is a PEP 420 namespace package; a regular package would carry
    # __file__ and break coexistence with other baseten-* distributions.
    import baseten

    assert getattr(baseten, "__file__", None) is None


def test_bdn_imports() -> None:
    assert baseten.bdn.__doc__
