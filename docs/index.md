#  REMIND-PyPSA Coupling Tools 

A package to support the coupling of REMIND and PyPSA, written by the Potsdam Institute for Climate Impact Studies' [Energy Transition Lab](https://www.pik-potsdam.de/en/institute/labs/energy-transition/energy-transition-lab).

The package provides a suite of transformation and disaggration tools for REMIND data as well as abstracted frameworks to support these.

## Abstracted logic
The package provides an abstracted framework for ETL operations based steps. The steps can be read from YAML into the [transformation class](objects#objects)

## Remind data support
Can be imported either as csv or gdx. Both formats can be processed (gdx not yet integrated in the examples)

## Built in transformations and disaggregation
The package comes with tools to [transform](transformations) and disaggregate remind data.

## Tutorials

[Examples](https://github.com/pik-piam/Remind-PyPSA-coupling/tree/main/examples) and [tutorials](tutorials) to help you understand the package