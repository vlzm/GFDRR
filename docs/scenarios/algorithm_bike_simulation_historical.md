# Algorithm 1a: Bike-Sharing Simulation — Historical Replay

## What is this about?

Before we can predict the future, we need to prove we can reproduce the past. This algorithm takes a dataset of real bike trips — every ride that actually happened, with its origin, destination, and timestamps — and replays them through a physical simulation of the station network. Bikes leave stations, spend time in transit, arrive, and dock. If a station is full, the bike gets redirected to the nearest available one.

Why bother simulating history if we already know what happened? Because the raw trip data doesn't tell us what the stations looked like at every moment. It doesn't tell us how close a station was to running empty, or how many docks were free at 8:47 AM. The simulation reconstructs the full state of the system, second by second, from just the trip records and an initial snapshot.

This also serves as a validation tool. If the simulator produces inventory traces that don't match reality, something is wrong — a bug in the physics, a bad initial state, or missing rebalancing events. Fix it here, before adding any ML.

---

## Notation

| Symbol | Meaning |
|--------|---------|
| $\mathcal{S} = \{1, \dots, N\}$ | Set of stations |
| $t \in \{0, \dots, T\}$ | Time step (e.g. 1 hour) |
| $\mathcal{H}$ | Historical trip dataset: $\{(o_k, d_k, t^{start}_k, t^{end}_k)\}$ |
| $\mathbf{x}_i(t)$ | Inventory at station $i$, time $t$ |
| $C_i$ | Dock capacity of station $i$ |
| $f_j$ | Free docks at station $j$ |
| $O_i(t)$ | Departures from station $i$: $|\{k : o_k = i, \; t^{start}_k \in [t, t+1)\}|$ |
| $T_{ij}(t)$ | Trip count from $i$ to $j$: $|\{k : o_k = i, \; d_k = j, \; t^{start}_k \in [t, t+1)\}|$ |
| $P(j \mid i)$ | Destination probability: $T_{ij}(t) / O_i(t)$ |
| $d_i(t)$ | Realized departures from station $i$ |
| $\ell_i(t)$ | Lost demand at station $i$: $O_i(t) - d_i(t)$ |
| $n_{ij}$ | Sampled trip counts from $i$ to $j$ |
| $\mathcal{B}(t)$ | Transit buffer: in-flight bikes $\{(id, j, t_{arr})\}$ |
| $\mathcal{A}(t)$ | Bikes arriving at time $t$: $\{b \in \mathcal{B} \mid t_{arr} \leq t\}$ |
| $t_{arr}$ | Arrival time of a trip |
| $\tau_k$ | Duration of trip $k$: $t^{end}_k - t^{start}_k$ |
| $a_j$ | Incoming bike count at station $j$ |
| $\mathcal{L}$ | Trip log (output) |

**Invariant:** $\sum_i \mathbf{x}_i(t) + |\mathcal{B}(t)| = \text{const} \;\; \forall t$

---

## Procedure

**Input:** $\mathbf{x}(0)$, $\mathbf{C}$, $\mathcal{H}$

**Output:** $\mathcal{L}$, $\mathbf{x}(1), \dots, \mathbf{x}(T)$

**Init:** $\mathcal{B}(0) \leftarrow \varnothing$

---

**for** $t = 0, \dots, T-1$ **do**

> **// Phase A: Resolve arrivals from transit buffer**
>
> $\mathcal{A}(t) \leftarrow \{b \in \mathcal{B}(t) \mid t_{arr} \leq t\};\qquad \mathcal{B}(t) \leftarrow \mathcal{B}(t) \setminus \mathcal{A}(t)$
>
> **for each** station $j$ with arriving bikes **do**
> > $a_j \leftarrow |\{b \in \mathcal{A}(t) : b.dest = j\}|$
> >
> > $f_j \leftarrow C_j - \mathbf{x}_j(t)$
> >
> > accept $\min(a_j, f_j)$: $\;\mathbf{x}_j \mathrel{+}= \min(a_j, f_j)$, mark $\texttt{completed}$ in $\mathcal{L}$
> >
> > **for each** remaining bike **do**
> > > $k \leftarrow \arg\min_{k': C_{k'} - \mathbf{x}_{k'} > 0} \text{dist}(j, k')$
> > >
> > > $\mathbf{x}_k \mathrel{+}= 1$, mark $\texttt{redirected}$ in $\mathcal{L}$

> **// Phase B: Generate trips from historical data**
>
> **B1.** Aggregate from $\mathcal{H}$:
>
> $\qquad O_i(t) \leftarrow |\{k \in \mathcal{H} : o_k = i,\; t^{start}_k \in [t, t+1)\}|$
>
> $\qquad T_{ij}(t) \leftarrow |\{k \in \mathcal{H} : o_k = i,\; d_k = j,\; t^{start}_k \in [t, t+1)\}|$
>
> **B2.** Destination probabilities:
>
> $\qquad P(j \mid i) \leftarrow T_{ij}(t) \;/\; O_i(t)$
>
> **B3.** Constrain departures:
>
> $\qquad d_i(t) \leftarrow \min\bigl(O_i(t),\; \mathbf{x}_i(t)\bigr)$
>
> $\qquad \ell_i(t) \leftarrow O_i(t) - d_i(t)$
>
> $\qquad \mathcal{L} \mathrel{+}= \{(t, i, \varnothing, \texttt{lost})\} \times \ell_i$
>
> **B4.** Sample trips:
>
> $\qquad n_{ij} \sim \text{Multinomial}(d_i, P(\cdot \mid i)) \qquad \forall i$
>
> $\qquad$ **for each** trip $(i \to j)$ in $\mathbf{n}$ **do**
>
> > $\mathbf{x}_i \mathrel{-}= 1$
> >
> > $t_{arr} \leftarrow t + \tau_k \qquad$ *(duration from historical trip record)*
> >
> > **if** $t_{arr} \leq t+1$: resolve arrival immediately (as Phase A)
> >
> > **else:** $\mathcal{B}(t) \mathrel{+}= \{(id, j, t_{arr})\}$, mark $\texttt{in\_transit}$ in $\mathcal{L}$

> **// Phase C: Record state**
>
> $\mathbf{x}(t+1) \leftarrow \mathbf{x}$ as modified by Phases A and B
>
> verify: $\sum_i \mathbf{x}_i(t+1) + |\mathcal{B}(t)| = \text{const}$

**end for**

---

## Note on Lost Demand

Historical $O_i(t)$ is **realized** demand — trips that actually occurred. If a station was empty, no trips were recorded, and $O_i(t) = 0$. This means:

- Step B3 will rarely trigger — the data is already constrained by reality
- $\ell_i(t) \approx 0$ in most steps — not because the system is perfect, but because unmet demand is invisible in historical data
- Non-zero $\ell_i(t)$ during replay indicates that the simulation state diverged from reality, most likely due to a rebalancing event that occurred in real life but is absent from the model

---

## Status Lifecycle

```
departure           arrival              dock check
   │                   │                    │
   ▼                   ▼                    ▼
in_transit ──(τ expires)──► pending ──► completed
                                    └──► redirected (if full)

lost_demand (never departed)
```

---

## Metrics (from $\mathcal{L}$)

$$\text{service rate}(t) = \frac{|\texttt{completed}| + |\texttt{redirected}|}{|\texttt{completed}| + |\texttt{redirected}| + |\texttt{lost}|}$$

$$\text{redirect rate}(t) = \frac{|\texttt{redirected}|}{|\texttt{completed}| + |\texttt{redirected}|}$$

$$\text{avg in-transit}(t) = |\mathcal{B}(t)|$$
