#  REMIND-PyPSA Coupling Tools

Welcome to the `rpycpl` docu! `rpycpl` is a package to support the soft-coupling of the [REMIND](https://github.com/remindmodel/remind)
IAM and [PyPSA](https://github.com/PyPSA) power system model family, such as [PyPSA-eur](https://pypsa-eur.readthedocs.io/en/latest/) or [PyPSA-China](https://pik-piam.github.io/PyPSA-China-PIK/latest/). The package is written and maintained by the Potsdam Institute for Climate Impact
Research's [Energy Transition Lab](https://www.pik-potsdam.de/en/institute/labs/energy-transition/energy-transition-lab).

`rpycpl` is the **shared layer** of the IAM-PSM coupling. It holds the logic that is identical for all PyPSA models that couple to REMIND. These include
- reading REMIND output
- unit conversion
- translation of costs, capacities
- downscaling of demands from REMIND region to countries

The interface to the PSM models' snakemake workflow is exposed via a thin `CouplingAdapter`. Each snakemake workflow subclasses this into a custom **adapter** with any required tweaks. 

> **REMIND-side logic lives in the package. PyPSA-side logic lives in the model.**

> The adapter is the seam where the two meet.

## Start here

- **[Architecture & usage](architecture.md)** — how the components fit together, what
  belongs in the package versus the local adapter, a worked PyPSA-Eur example, and how to
  add a new model such as PyPSA-Earth.
- **Reference** (in the nav) — the auto-generated API reference for every module.

## What the package provides

| Operations | Module(s) |
|---|---|
| Read REMIND output (GDX / IAMC) | `rpycpl.io` — `RemindLoader`, `remind_symbols` |
| Logical-name → symbol map + units | `data/remind_symbols.yaml`, `rpycpl.units` |
| Tidy-frame transforms (CO₂, loads, capacities, costs, mapping) | `rpycpl.transforms` |
| Region → country downscaling (SSP proxies) | `rpycpl.downscale`, `rpycpl.io.ssp` |
| The coupling adapter interface | `rpycpl.adapters.CouplingAdapter` |

## IAM REMIND data

REMIND data can be read from `.gdx` (via `gamspy`) or the IAMC `.mif`/`.csv` exchange
format — the backend is auto-detected from the source.

