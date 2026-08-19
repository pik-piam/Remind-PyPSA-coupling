# Sector coupling

Sector coupling is represented using a simplified structure of buses, links, and loads — all
loads are in electricity, with no other energy carriers involved. See the REMIND-PyPSA-Eur paper
(linked on the [home page](../index.md#further-reading)) for the methodology behind this choice.

!!! note "Under development"
    This representation of sector coupling is still under development — for example,
    [`PyPSA-China`](https://github.com/pik-piam/PyPSA-China-PIK) handles heat differently.

As an example, REMIND currently provides sectoral electricity demand for the following sectors (see
`models/remind/quantities_gdx.yaml` / `quantities_mif.yaml`):

- Hydrogen electrolysis (`electrolysis`)
- EV passenger (`EV_pass`)
- EV freight (`EV_freight`)
- Heat pumps (`heatpump`)
- Resistive heating (`resistive`)
- Space cooling (`space_cooling`, currently only in `.mif`)
- Residual demand (`AC`)

For each of these sectors, a load time series needs to be added to the PyPSA network — otherwise
the demand should be folded into the residual demand instead. Attaching the `Load` to its own
`Bus` also enables demand-side management, if an additional `Store` is added alongside it.

This simplified structure means PyPSA doesn't decide, for example, how many heat pumps or
resistive heaters to install — that investment decision stays with the IAM.

## Example in PyPSA-Eur

PyPSA-Eur is the current reference implementation:
`pypsa-eur-iam/scripts/remind/add_electricity_sector_REMIND.py` attaches one `Bus`/`Link`/`Load`
(and, where enabled, a `Store`) per sector. See `pypsa-eur-iam/README.md`'s "What's different
from upstream PyPSA-Eur" section for the full rundown.
