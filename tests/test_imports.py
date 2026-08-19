"""Test the public import surface and the model/generic boundary."""

import pathlib

import pytest

SRC = pathlib.Path(__file__).parent.parent / "src" / "iampypsa"


def test_facade_is_the_five_curated_names():
    """The front door stays small — anything else is internals, imported explicitly."""
    import iampypsa

    assert set(iampypsa.__all__) == {
        "open_coupler", "Coupler", "IamLoader", "load_quantity_specs", "__version__",
    }
    for name in iampypsa.__all__:
        assert hasattr(iampypsa, name), name


def test_import_subpackages():
    """The sub-packages import correctly."""
    from iampypsa import downscale, formats, models, quantities, reference, transforms  # noqa: F401
    from iampypsa import coupler, loader, units  # noqa: F401


def test_no_iam_named_outside_models():
    """Nothing on the generic path may name an IAM — the boundary erodes silently otherwise."""
    offenders = [
        path.relative_to(SRC).as_posix()
        for path in SRC.rglob("*.py")
        if "models/" not in path.relative_to(SRC).as_posix()
        and "remind" in path.read_text().lower()
    ]
    assert offenders == []


def test_open_coupler_rejects_an_unknown_model():
    from iampypsa import open_coupler

    with pytest.raises(ValueError, match="Unknown model"):
        open_coupler("whatever.gdx", model="not-an-iam")
