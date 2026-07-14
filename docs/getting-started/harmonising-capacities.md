# Harmonising capacities

A PyPSA model typically starts from its own existing power-plant database (e.g.
[powerplantmatching](https://github.com/PyPSA/powerplantmatching) for PyPSA-Eur). The IAM,
independently, reports capacity targets per (region, technology, year) via
`iampypsa.transforms.capacities.build_capacity_targets`. These need to be harmonised.

## The general approach

Rather than discarding the model's existing plant-level detail (e.g. location and
decommissioning year) and running the PyPSA model greenfield, the recommended approach is as
follows:

1. Filter the power-plant database for the model year, dropping power plants that aren't built
   yet or are already decommissioned by then.
2. Group the filtered power-plant database by (region, technology) — using the
   [technology mapping](technology-mapping.md).
3. Compare the IAM's target against that filtered power-plant database's aggregate capacity, per
   (region, technology) group. Two cases need to be distinguished:
    - **IAM target ≥ PyPSA capacity**: the power-plant database is left exactly as filtered; any
      additional capacity needed to reach the target is left to the solver, which is free to
      decide where to build it — this needs to be enforced through an additional global
      constraint across all nodes within the region, see
      [Enforcing the target during optimisation](#enforcing-the-target-during-optimisation).
    - **IAM target < PyPSA capacity** — a scaling factor (`target / existing`, `< 1`) is applied
      to every matched power plant in the group, proportionally shrinking their capacity so the
      group's aggregate now equals the target. This is effectively **early retirement**:
      individual plants keep their location and other attributes, just at reduced capacity.

This keeps the model's spatial/technical detail while still being consistent with the IAM's
capacity pathway.

## Enforcing the target during optimisation

If IAM target ≥ PyPSA capacity, an **additional global constraint** on total capacity per
(region, technology) group is required during optimisation to ensure that all capacity set by
the IAM is actually built in PyPSA. Two constraints are possible:

- A mandatory **lower bound**, which ensures that across all modelled PyPSA nodes within the IAM
  region the required capacity is actually installed (exceeding the capacity already set through
  the filtered power-plant database).
- An optional **upper bound**, which additionally prevents PyPSA from building *more* capacity
  than the IAM specifies — effectively an adequacy check on the IAM's own capacity assumptions.

## Worked example: PyPSA-Eur

`pypsa-eur-iam/scripts/remind/adjust_powerplants_REMIND.py` implements steps 1-3 above.
`pypsa-eur-iam/scripts/remind/installed_capacity_constraints_REMIND.py` implements the global
constraints.

## Next

- [Sector coupling](sector-coupling.md)
