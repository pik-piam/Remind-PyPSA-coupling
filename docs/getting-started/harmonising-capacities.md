# Harmonising capacities

A PyPSA model typically starts from its own existing power-plant database (e.g.
[powerplantmatching](https://github.com/PyPSA/powerplantmatching) for PyPSA-Eur). The IAM,
independently, reports capacity targets per (region, technology, year) via
`iampypsa.transforms.capacities.build_capacity_targets`. These need to be harmonised.

## The general approach

Rather than discarding the model's existing plant-level detail (e.g. location and
decommissioning year) and running the PyPSA model greenfield, the recommended approach is as
follows:

1. Filter the power-plant database for the target year, dropping plants that aren't built yet or
   are already decommissioned by then. What's left is the fleet actually online in that year.
2. Group that filtered fleet's plants by (region, technology) — using the same
   [technology mapping](technology-mapping.md) that decides cost sourcing, so the grouping is
   consistent across the whole pipeline.
3. Compare the IAM's target against that filtered fleet's aggregate capacity, per (region,
   technology) group:
    - **IAM target ≥ PyPSA capacity** — no scaling is applied. The existing fleet is left exactly
      as filtered; any additional capacity needed to reach the target is left entirely to the
      solver, which is free to build it wherever is economical — this needs to be enforced
      through a global constraint, see
      [Enforcing the target during optimisation](#enforcing-the-target-during-optimisation).
    - **IAM target < PyPSA capacity** — a scaling factor (`target / existing`, `< 1`) is applied
      to every matched plant in the group, proportionally shrinking their capacity so the group's
      aggregate now equals the target. This is effectively **early retirement**: individual plants
      keep their location and other attributes, just at reduced capacity.

This keeps the model's spatial/technical detail while still being consistent with the IAM's
capacity pathway.

## Enforcing the target during optimisation

If the IAM's target is at or above the existing (filtered) capacity, scaling alone does nothing —
so an **additional global constraint** on total capacity per (region, technology) group is
required during optimisation to actually make the solver reach it. Two such constraints are
possible: a **lower bound**, which is required to guarantee the IAM's target is actually met, and
an optional **upper bound**, which additionally prevents PyPSA from building *more* capacity than
the IAM specifies — effectively an adequacy check on the IAM's own capacity assumptions.

## Worked example: PyPSA-Eur

`pypsa-eur-iam/scripts/remind/adjust_powerplants_REMIND.py` is the canonical implementation of
steps 1–3 above. `pypsa-eur-iam/scripts/remind/installed_capacity_constraints_REMIND.py`
implements the global constraint from the previous section.

## Where this fits in the package split

Like [sector coupling](sector-coupling.md), harmonisation is a **model-specific implementation
detail**, not something `iampypsa` does for you. The package's job stops at supplying the
capacity targets (`build_capacity_targets`); the scaling/reconciliation step against a specific
model's plant database currently lives in the model repo.

## Next

- [Sector Coupling](sector-coupling.md)
