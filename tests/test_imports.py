"""Test the public import surface and the model/generic boundary."""

import pathlib

import pytest

SRC = pathlib.Path(__file__).parent.parent / "src" / "iampypsa"


def test_facade_is_the_five_curated_names():
    """The front door stays small — anything else is internals, imported explicitly."""
    import iampypsa

    assert set(iampypsa.__all__) == {
        "build_coupler", "Coupler", "IamLoader", "load_quantity_specs", "__version__",
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


def test_build_coupler_rejects_an_unknown_model():
    from iampypsa import build_coupler

    with pytest.raises(ValueError, match="Unknown model"):
        build_coupler("whatever.gdx", model="not-an-iam")


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        ("remind2pypsa_amt_filtered.gdx", "RemindGdxCoupler"),
        ("remind_generic_amt_filtered.mif", "RemindIamcCoupler"),
    ],
)
def test_build_coupler_pairs_the_backend_with_its_coupler(fixture, expected):
    """The pairing consumers used to do by hand — one place, driven by the suffix."""
    from iampypsa import build_coupler

    coupler = build_coupler(pathlib.Path(__file__).parent / "data" / fixture)
    assert type(coupler).__name__ == expected
    assert coupler.quantities["co2_price"]["to_unit"] == "USD/tCO2"
