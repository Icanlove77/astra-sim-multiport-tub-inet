# ASTRA-sim
[ASTRA-sim](https://astra-sim.github.io/) is a distributed AI system simulator. It models the end-to-end software and hardware stack of modern AI systems - encompassing workload scheduling, collective communication algorithms, and hardware architectures (compute/memory/network). 

# Multi-port AstraSim

We did some adaptations to AstraSim to support multi-port collective algorithms: First, we've enabled host-based forwarding in ns-3. Second, we've implemented Trivance in AstraSim with ns-3.