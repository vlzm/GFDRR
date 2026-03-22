```
graph-logistics-platform/
│
├── README.md
├── pyproject.toml
│
├── docs/
│   ├── graph_data_model.md
│   └── diagrams/
│
├── src/
│   └── flowplatform/
│       │
│       ├── core/                        # L1 + L2: модель данных целиком
│       │   │
│       │   ├── enums.py                 # FacilityType, OperationType, FacilityRole,
│       │   │                            #   ModalType, PeriodType, AttributeKind
│       │   ├── schemas/                 # табличные контракты (dataclass/pydantic)
│       │   │   ├── entity.py            # Facility, CommodityCategory, ResourceCategory,
│       │   │   │                        #   Resource (L3), Commodity (L3)
│       │   │   ├── temporal.py          # PlanningHorizon, Segment, Period
│       │   │   ├── behavior.py          # FacilityRole, FacilityOperation,
│       │   │   │                        #   FacilityAvailability, EdgeRule
│       │   │   ├── edge.py              # Edge, EdgeCommodity, EdgeCapacity,
│       │   │   │                        #   EdgeCommodityCapacity, EdgeVehicle,
│       │   │   │                        #   EdgeLeadTimeResolved
│       │   │   ├── demand_supply.py     # Demand, Supply, InventoryInitial, InventoryInTransit
│       │   │   ├── transformation.py    # Transformation, TransformationInput/Output
│       │   │   ├── resource.py          # ResourceFleet, ResourceCommodityCompat,
│       │   │   │                        #   ResourceModalCompat, ResourceAvailability
│       │   │   ├── parameters.py        # OperationCost, TransportCost, ResourceCost
│       │   │   ├── pricing.py           # CommoditySellPriceTier, ProcurementCostTier
│       │   │   ├── hierarchy.py         # HierarchyType/Level/Node/Membership
│       │   │   │                        #   (facility + commodity)
│       │   │   ├── scenario.py          # Scenario, ScenarioEdgeRules, ManualEdges,
│       │   │   │                        #   ParameterOverrides
│       │   │   └── output.py            # Solution*, Simulation*Log, Metadata
│       │   │
│       │   ├── roles.py                 # DEFAULT_ROLES, role derivation (§2.4)
│       │   ├── edges.py                 # edge identity logic, commodity filtering
│       │   ├── attributes/              # Attribute System (§9–10)
│       │   │   ├── spec.py              # AttributeSpec
│       │   │   ├── grain_groups.py      # GrainGroup, auto_group_attributes
│       │   │   ├── merge_plan.py        # MergePlan, plan_merges
│       │   │   └── builder.py           # AttributeBuilder — spine assembly
│       │   │
│       │   └── model.py                 # RawModelData, ResolvedModelData
│       │                                #   (контракты: что на входе, что на выходе)
│       │
│       ├── loading/                     # Global loader: сырые данные → RawModelData
│       │   ├── base.py                  # абстрактный DataSource protocol
│       │   ├── csv_loader.py            # из CSV/папки
│       │   ├── database_loader.py       # из БД
│       │   └── validators.py            # schema validation при загрузке
│       │                                #   (типы колонок, nullability)
│       │
│       ├── build/                       # Build Pipeline: RawModelData → ResolvedModelData
│       │   ├── pipeline.py              # orchestrator (§13.1)
│       │   ├── validation.py            # бизнес-валидация (§13.2):
│       │   │                            #   unit consistency, referential integrity,
│       │   │                            #   resource completeness, graph connectivity
│       │   ├── time_resolution.py       # resolve_to_periods (§13.3)
│       │   ├── lead_time.py             # resolve_lead_times (§13.4)
│       │   ├── edge_builder.py          # build_edges: rules + manual (§5.6)
│       │   ├── transformation.py        # resolve_transformations (§13.5)
│       │   ├── fleet_capacity.py        # compute_fleet_capacity (§13.6)
│       │   ├── aggregation.py           # hierarchical aggregation (§13.8)
│       │   └── spine.py                 # spine assembly (§13.7)
│       │
│       ├── consumers/
│       │   ├── rebalancer/
│       │   │   ├── dataloader.py        # ResolvedModelData → матрицы/vectors для LP
│       │   │   ├── formulation.py       # LP/MILP model building
│       │   │   └── solver_config.py
│       │   │
│       │   ├── simulator/
│       │   │   ├── dataloader.py        # ResolvedModelData → начальный SimulationState
│       │   │   ├── state.py             # SimulationState (runtime)
│       │   │   ├── engine.py            # simulation_step, main loop
│       │   │   ├── dispatch.py          # Dispatch dataclass
│       │   │   └── step_solvers/
│       │   │       ├── protocol.py      # StepSolver protocol
│       │   │       ├── greedy.py
│       │   │       ├── vrp.py
│       │   │       └── rebalance.py
│       │   │
│       │   └── analytics/
│       │       ├── dataloader.py        # подтягивает historical + solution/simulation
│       │       └── plan_vs_fact.py
│       │
│       ├── historical/                  # Historical Data Layer (§12)
│       │   ├── schemas.py               # Shipment (L3), HistoricalFlow/Inventory (L2)
│       │   └── aggregation.py           # shipments → historical_flow (materialized views)
│       │
│       └── domains/                     # L3 конфигурации (§14)
│           ├── bike_sharing.py
│
├── tests/
│   ├── unit/
│   │   ├── core/
│   │   │   ├── test_roles.py
│   │   │   ├── test_attributes.py
│   │   │   └── test_grain_groups.py
│   │   ├── build/
│   │   │   ├── test_validation.py
│   │   │   ├── test_time_resolution.py
│   │   │   ├── test_lead_time.py
│   │   │   └── test_edge_builder.py
│   │   └── loading/
│   │       └── test_csv_loader.py
│   │
│   ├── integration/
│   │   ├── test_full_pipeline.py        # raw CSV → ResolvedModelData
│   │   ├── test_rebalancer_e2e.py
│   │   └── test_simulator_e2e.py
│   │
│   └── fixtures/
│       └── bike_minimal/
│
└── notebooks/
    └── 01_bike_sharing_demo.ipynb
```