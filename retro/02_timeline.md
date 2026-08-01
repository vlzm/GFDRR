# Полная хронология разработки

278 коммитов, 2026-01-29 — 2026-07-30, 80 активных дней.

Формат строки: `#номер  hh:mm  hash  +добавлено/-удалено (файлов)  сообщение`


## 2026-01-29 (чт) — 2 коммитов, +1937/-0

- `#001 21:02 41170be` +207/-0 (4f) [.gitignore,README.md,gbp,pyproject.toml] — **Initialize project structure with .gitignore, pyproject.toml, README.md, and package directory. Added dependencies for core functionality, web framework, database access, observability, and development tools.**
- `#002 21:43 3c9bf69` +1730/-0 (3f) [.cursorrules,SPEC.md,SPEC_reference.md] — **Add technical specifications and cursor rules for the Graph Optimization Platform**
    > - Introduced `SPEC.md` detailing the architectural strategy, technology stack, and project structure.
    > - Added `.cursorrules` to outline coding guidelines, project context, and critical knowledge base for development.
    > - These documents establish foundational principles for maintaining code quality and adherence to the modular monolith architecture.

## 2026-02-01 (вс) — 1 коммитов, +274/-0

- `#003 18:11 e78f98a` +274/-0 (1f) [notebooks] — **Add exploratory data analysis (EDA) notebook for initial data insights**
    > - Created a new Jupyter notebook `00_EDA.ipynb` to facilitate exploratory data analysis.
    > - The notebook includes data importation using Pandas and a structured presentation of trip data, including trip IDs, start and end times, distances, durations, and community area information.
    > - This addition aims to support data-driven decision-making and enhance understanding of the dataset for further analysis and optimization tasks.

## 2026-02-02 (пн) — 2 коммитов, +1021/-69

- `#004 20:23 c2c4336` +955/-0 (4f) [gbp,notebooks,pyproject.toml] — **Add mock data generation utilities and validation schemas for graph representation**
    > - Introduced `graph_model.py` and `graph_model_mock.py` to define DataFrame-based models and mock data loaders for the graph structure.
    > - Implemented Pandera schemas for validating DataFrames, ensuring data integrity across module boundaries.
    > - Added a new dependency on `pandera` in `pyproject.toml` to support DataFrame validation.
    > - Created a Jupyter notebook `01_test_mock_loader.ipynb` for testing the mock data loader functionality and visualizing generated graph data.
    > - These additions enhance the testing framework and facilitate the development of graph-related features in the platform.
- `#005 20:28 b5ccd0d` +66/-69 (2f) [gbp,notebooks] — **Update mock data loader configurations and test notebook for enhanced graph representation**
    > - Modified `edge_density` values in `GraphMockDataLoader` to 1 for all configurations, ensuring a denser graph structure for testing.
    > - Updated execution counts in `01_test_mock_loader.ipynb` to reflect the latest runs and adjusted output values for improved accuracy in the mock data representation.
    > - These changes enhance the fidelity of the mock data used in testing and facilitate better validation of graph-related functionalities.

## 2026-02-03 (вт) — 1 коммитов, +331/-0

- `#006 20:33 74b090f` +331/-0 (3f) [gbp] — **Add data loading and VRP solving components for rebalancing functionality**
    > - Introduced `DataLoaderRebalancer` class for handling data loading, transformation, and saving processes.
    > - Added `Rebalancer` class to manage the rebalancing logic, including data loading, imbalance detection, VRP data model creation, and solving the VRP using the `VRPSolver`.
    > - Implemented `VRPSolver` class utilizing OR-Tools for optimizing vehicle routing problems.
    > - These additions establish a foundational structure for the rebalancing module, enhancing the platform's capability to address logistical challenges on graph-structured data.

> **пауза 4 дней**

## 2026-02-07 (сб) — 7 коммитов, +1907/-238

- `#007 18:46 312b457` +12/-13 (1f) [gbp] — **some mess**
- `#008 18:50 25c0bd3` +1077/-154 (1f) [notebooks] — **Refactor exploratory data analysis (EDA) notebook for improved data presentation**
    > - Updated the `00_EDA.ipynb` notebook to enhance the structure and clarity of the exploratory data analysis.
    > - Changed column names to more descriptive terms (e.g., `ride_id`, `rideable_type`, `started_at`, etc.) for better understanding of the dataset.
    > - Adjusted execution counts to reflect the latest runs and included a warning for mixed data types during CSV import.
    > - These modifications aim to facilitate better insights and usability of the EDA notebook for data-driven decision-making.
- `#009 19:13 2166c0e` +26/-10 (1f) [notebooks] — **Refactor EDA notebook to enhance mock data generation and execution tracking**
    > - Updated `00_EDA.ipynb` to improve the structure and clarity of exploratory data analysis.
    > - Changed execution counts to `null` for cells to reset the notebook state.
    > - Added mock DataFrame generation for bike station data, including columns for `station_id`, `capacity`, `lat`, `lon`, and `num_bikes_available`.
    > - These modifications aim to facilitate better data insights and usability for future analysis and testing.
- `#010 20:13 8f89904` +709/-14 (2f) [notebooks,pyproject.toml] — **Refactor EDA notebook: Update bike availability analysis and enhance inventory management logic**
    > - Changed execution counts for specific code cells to maintain proper order.
    > - Updated bike availability analysis to sort by `commodity_quantity` instead of `num_bikes_available`.
    > - Enhanced the mock DataFrame creation with additional columns for better inventory management.
    > - Implemented a function to update inventory based on the results of the pickup and delivery problem.
    > - Added detailed output for the pickup and delivery solution, including routes and inventory changes.
    > - Adjusted the number of stations in the mock data for more comprehensive testing.
    > - Updated the display name in the notebook metadata for clarity.
    > Update pyproject.toml: Ensure correct scipy version is specified
    > - Added `scipy==1.17.0` to dependencies for compatibility.
- `#011 20:17 95f93dc` +10/-0 (1f) [notebooks] — **Add placeholder comment for future implementation of depots and trucks**
- `#012 23:45 a99ea0e` +71/-45 (1f) [notebooks] — **Refactor EDA notebook: Update comments and improve data preparation functions for stations, depots, and resources**
- `#013 23:46 c233fe2` +2/-2 (1f) [notebooks] — **Refactor EDA notebook: Update print statements to reflect 'commodities' instead of 'bikes'**

## 2026-02-08 (вс) — 1 коммитов, +184/-95

- `#014 00:05 1694c3f` +184/-95 (1f) [notebooks] — **Refactor pickup-delivery pairs and update EDA notebook**
    > - Increased the number of pickup-delivery pairs from 3 to 4 and adjusted quantities accordingly.
    > - Updated the output format to reflect changes in commodities and distances.
    > - Enhanced the greedy matching algorithm for pickups and deliveries, improving efficiency.
    > - Added new columns for utilization, target count, and balance in the inventory summary.
    > - Adjusted inventory quantities and calculations to reflect the new delivery strategy.

> **пауза 6 дней**

## 2026-02-14 (сб) — 2 коммитов, +269/-189

- `#015 12:57 3790c65` +150/-79 (1f) [notebooks] — **Enhance EDA notebook with new data preparation utilities**
    > - Introduced `DataLoader` class for generating fake stations, depots, and resources data.
    > - Added static methods for preparing stations, depots, and resources data with detailed docstrings and TODOs for future implementation.
    > - Updated execution counts to `null` to reset the notebook state.
    > - Improved structure and clarity of the exploratory data analysis, facilitating better insights for future analysis and testing.
- `#016 17:41 f7ea607` +119/-110 (1f) [notebooks] — **Fix AttributeError in DataLoader and update EDA notebook**
    > - Resolved an `AttributeError` related to the `DataLoader` class by adding a `config` attribute in the constructor.
    > - Updated the `prepare_stations_data` method to utilize the `config` parameter for dynamic data generation.
    > - Enhanced error handling and output clarity in the exploratory data analysis notebook, ensuring smoother execution and better insights for future analysis.

> **пауза 9 дней**

## 2026-02-23 (пн) — 1 коммитов, +857/-1301

- `#017 21:32 c85c59a` +857/-1301 (16f) [gbp,notebooks] — **First iteration of refactoring**

## 2026-02-24 (вт) — 4 коммитов, +588/-2222

- `#018 15:38 a643141` +307/-188 (8f) [gbp,notebooks] — **Refactor data loading and processing in the rebalancer module**
    > - Moved data loading functionality from `data_loader.py` to `dataloader.py`, encapsulating it within the `DataLoaderRebalancer` class.
    > - Updated the `Rebalancer` class to utilize the new `DataLoaderRebalancerProtocol`, enhancing modularity and adherence to the hexagonal architecture.
    > - Refactored methods for creating distance matrices and pickup-delivery pairs, ensuring they are now part of the `DataLoaderRebalancer` class.
    > - Introduced a new `protocols.py` file to define interfaces for data loading, promoting explicit dependencies and modularity.
    > - Added a new Jupyter notebook for testing the pipeline, facilitating easier validation of the rebalancer's functionality.
- `#019 15:58 79aed18` +249/-1783 (4f) [gbp,notebooks] — **cleaned and prepared notes for graph loader**
- `#020 21:39 bef268b` +20/-243 (2f) [gbp,notebooks] — **Refactor data loading in Rebalancer and update execution counts in the EDA notebook**
- `#021 22:02 e9d8e82` +12/-8 (2f) [gbp,notebooks] — **Refactor data handling in Rebalancer and update notebook comments for clarity**

## 2026-02-25 (ср) — 1 коммитов, +627/-213

- `#022 21:08 7086ab2` +627/-213 (10f) [gbp,notebooks] — **Refactor DataLoader and Rebalancer modules to enhance data handling and modularity**
    > - Introduced a new `DataLoaderGraph` class for converting raw source data into a temporal `GraphData` representation, encapsulating the logic for building nodes, edges, resources, and commodities.
    > - Updated `DataLoaderRebalancer` to utilize the new `DataLoaderGraph`, streamlining the data loading process and improving adherence to hexagonal architecture principles.
    > - Refactored demand calculation logic into a dedicated `DemandCalculator` class, enhancing clarity and separation of concerns.
    > - Modified the `Rebalancer` class to simplify the pipeline execution flow, ensuring it directly interacts with the updated data loader classes.
    > - Enhanced the Jupyter notebook to reflect changes in the data loading process and to validate the new structure.

> **пауза 4 дней**

## 2026-03-01 (вс) — 2 коммитов, +5015/-723

- `#023 13:55 f7a9f2d` +399/-17 (10f) [gbp,notebooks] — **Enhance data validation and structure by implementing Pandera schemas and decorators across modules**
- `#024 21:34 a78369d` +4616/-706 (18f) [.gitignore,gbp,notebooks] — **Add initial graph API test notebook and update pipeline notebook kernel specification**
    > - Created a new Jupyter notebook `01_test_graph_api.ipynb` to test graph API functionalities, including node and edge attributes, flows, and demands.
    > - Updated the kernel specification in `02_test_pipeline.ipynb` to use "GFDRR (3.11.9)".

## 2026-03-04 (ср) — 1 коммитов, +314/-187

- `#025 21:15 2f65eb1` +314/-187 (5f) [gbp,notebooks] — **feat(graph): enhance edge management and distance service integration**
    > - Added edges attribute to GraphData class, allowing for dynamic edge storage.
    > - Implemented set_distance_service method in GraphQueryMixin for flexible distance service configuration.
    > - Refactored edge building methods to utilize the new distance service setup.
    > - Updated example notebook to reflect changes in edge building and distance service usage.
    > - Enhanced save/load functionality for edges in parquet format.
    > - Improved documentation and type hints for better clarity and usability.

## 2026-03-06 (пт) — 1 коммитов, +1210/-857

- `#026 21:06 2e24b85` +1210/-857 (5f) [gbp,notebooks] — **feat(graph): introduce temporal attributes and validation in AttributeTable**
    > - Added TimeGranularity and GRANULARITY_TO_FREQ to support temporal attributes.
    > - Enhanced AttributeTable to include date_column, time_granularity, and date_format fields for temporal data handling.
    > - Implemented validation logic to ensure consistency of temporal fields in AttributeTable.
    > - Updated example notebook to demonstrate usage of temporal attributes in graph data.
    > - Enhanced save/load functionality to accommodate new temporal fields in parquet format.
    > - Improved documentation for AttributeTable to clarify the handling of static and temporal attributes.

## 2026-03-08 (вс) — 4 коммитов, +4271/-1980

- `#027 14:27 e8a1115` +1430/-180 (12f) [gbp,notebooks,tests] — **feat(loaders): implement DataLoaderGraph and associated contracts for graph data loading**
    > - Introduced `DataLoaderGraph` class to convert raw source data into a universal `GraphData`, supporting static and temporal inventory management.
    > - Added `DataLoaderMock` for generating mock data for testing purposes.
    > - Created Pydantic models in `contracts.py` for configuration and validation of data sources.
    > - Implemented Pandera schemas for validating source DataFrames, ensuring data integrity.
    > - Established protocols for data source and graph loader interfaces to promote modularity and adherence to hexagonal architecture principles.
    > - Added comprehensive tests for the graph data loader, covering loading, snapshot structure, and validation.
    > - Updated notebooks to demonstrate the usage of the new data loading functionality.
- `#028 15:03 054669a` +2257/-1800 (17f) [SPEC_reference.md,gbp,notebooks,tests] — **feat(rebalancer): restructure rebalancer module with contracts and data loading enhancements**
    > - Removed the `dataloader_graph.py` and `decorators.py` files to streamline the codebase.
    > - Introduced `contracts.py` to define Pydantic models for rebalancer configurations and schemas, ensuring strict validation.
    > - Updated `DataLoaderRebalancer` to utilize the new `RebalancerConfig` for configuration management, enhancing clarity and type safety.
    > - Refactored demand calculation logic into the `DemandCalculator` class, improving separation of concerns.
    > - Enhanced the `Rebalancer` class to directly interact with the updated data loader, promoting modularity.
    > - Implemented comprehensive validation using Pandera schemas across the rebalancer module to ensure data integrity.
    > - Updated tests and documentation to reflect the new structure and functionality.
- `#029 20:44 cd25948` +42/-0 (3f) [notebooks] — **feat(notebooks): add initial download_gbfs and load_trips_data notebooks**
- `#030 20:44 f894780` +542/-0 (1f) [notebooks] — **feat(notebooks): add comprehensive data loading and processing in download_gbfs notebook**

## 2026-03-09 (пн) — 2 коммитов, +2711/-1324

- `#031 20:04 8425f51` +1408/-951 (5f) [gbp,notebooks,tests] — **feat(loaders): enhance DataLoaderGraph with additional attributes and telemetry support**
    > - Updated `DataLoaderGraph` to include new methods for building station property attributes, cost attributes, and trip flows.
    > - Introduced telemetry data handling in the data loader, allowing for comprehensive snapshots that include telemetry metrics.
    > - Enhanced the `DataLoaderMock` to generate additional mock data for telemetry, trips, and costs, improving testing capabilities.
    > - Updated the `DataSourceProtocol` to include new data attributes for telemetry and trips, ensuring compatibility with the enhanced data loader.
    > - Added tests to validate the presence of new attributes and ensure data integrity across the loading process.
    > - Improved documentation and comments to reflect the new functionalities and their usage in the data loading pipeline.
- `#032 20:43 ff5f746` +1303/-373 (7f) [gbp,notebooks,tests] — **feat(graph): enhance GraphData with temporal attributes and validation**
    > - Added `inventory_ts`, `telemetry_ts`, and `timestamps` attributes to the `GraphData` class for improved temporal data handling.
    > - Introduced the `available_dates` property to retrieve available timestamps for temporal snapshots.
    > - Implemented `get_snapshot` method to return a single-timestamp snapshot from the full temporal series.
    > - Added private methods `_snapshot_inventory` and `_snapshot_telemetry` for handling inventory and telemetry data snapshots.
    > - Enhanced `GraphValidator` with `_validate_inventory_ts_schema` to ensure proper validation of the inventory time-series schema.
    > - Updated `DataLoaderGraph` to initialize new temporal attributes from the data source.
    > - Added tests to validate the new temporal attributes and ensure correct functionality of the snapshot methods.
    > - Improved documentation to reflect the new features and their usage in the graph data structure.

> **пауза 13 дней**

## 2026-03-22 (вс) — 12 коммитов, +13856/-9016

- `#033 14:59 cea645d` +6850/-0 (80f) [.gitignore,docs,gbp,pyproject.toml,tests] — **feat(docs): add architecture and data model documentation**
    > - Introduced comprehensive architecture diagrams to illustrate the core entities, facility roles, network graph examples, and multi-commodity flow concepts.
    > - Added detailed documentation for the graph-based data model, covering key entities such as Commodity, Resource, and Facility, along with their relationships and attributes.
    > - Created a structured repository documentation to outline the project structure and components.
    > - Enhanced the understanding of the build pipeline and data consumers through visual representations.
    > - Included an entity-relationship diagram to provide a full table schema overview.
    > - Improved overall documentation clarity and accessibility for future development and onboarding.
- `#034 15:09 cb688f1` +180/-206 (9f) [docs] — **feat(docs): update architecture and data model documentation for bike-sharing**
    > - Revised architecture diagrams to reflect the bike-sharing example, replacing the previous gas logistics context.
    > - Enhanced the graph data model documentation to include bike-sharing commodities and facilities, ensuring clarity on instance-level tracking and resource categories.
    > - Updated diagrams to illustrate the new operational roles and transformations specific to bike-sharing, including maintenance and repair processes.
    > - Improved overall documentation coherence and accessibility for stakeholders involved in bike-sharing logistics and optimization.
- `#035 15:16 1a98e3b` +61/-31 (1f) [docs] — **feat(docs): enhance graph data model documentation with implementation details**
    > - Updated the graph data model documentation to include the current implementation status of `RawModelData` and `ResolvedModelData`, detailing their alignment with Python packages.
    > - Added sections on the build pipeline, including the steps involved in `build_model()` and the role of various enums and roles in the model.
    > - Clarified the naming conventions for attributes and tables, ensuring consistency in terminology.
    > - Included validation caveats regarding the use of `validate_raw_model()` and its dependency on pre-materialized tables.
    > - Improved overall documentation clarity to facilitate better understanding for stakeholders and developers.
- `#036 15:26 28ab711` +1157/-0 (1f) [notebooks] — **feat(notebooks): introduce graph model playground for interactive validation**
    > - Added a new Jupyter notebook for interactive exploration of the tabular graph logistics model, focusing on the transition from `RawModelData` to `ResolvedModelData`.
    > - Included sections for schema validation, business validation, and the full build pipeline, enhancing the understanding of model construction.
    > - Implemented a toy bike-sharing `RawModelData` example to facilitate hands-on experimentation with model attributes and validation processes.
    > - Improved documentation within the notebook to guide users through the setup and execution of the model validation steps.
- `#037 18:55 bde0716` +874/-7289 (27f) [SPEC.md,docs,gbp,notebooks,tests] — **refactor(graph): restructure graph module and update documentation**
    > - Removed the `graph` module's core files, including `builders.py`, `core.py`, `validators.py`, and `queries.py`, to streamline the architecture and enhance modularity.
    > - Introduced a new `core/` directory to house the L2 logistics model, including `RawModelData`, schemas, and enums, improving organization and clarity.
    > - Updated documentation in `SPEC.md` and `architecture_diagrams.md` to reflect the new structure and clarify the transition from `RawModelData` to `ResolvedModelData`.
    > - Enhanced the `DataLoaderGraph` documentation to detail the new data transformation processes and validation steps, ensuring better understanding for users.
    > - Improved overall documentation coherence to facilitate easier navigation and comprehension of the graph data model.
- `#038 19:29 bb3060e` +131/-466 (10f) [docs,gbp,notebooks,tests] — **refactor(loaders): streamline DataLoaderGraph and update documentation**
    > - Removed legacy attributes and methods related to rebalancer snapshots from `DataLoaderGraph`, enhancing clarity and focus on core functionalities.
    > - Updated `GraphLoaderProtocol` to reflect the new data access patterns, including properties for `raw`, `resolved`, and `source` data.
    > - Revised documentation in `dataloader_graph.py` to clarify the data loading process and the relationship between `RawModelData` and `ResolvedModelData`.
    > - Enhanced architecture diagrams to accurately represent the updated data flow and loader capabilities.
    > - Improved test cases to align with the new structure, ensuring comprehensive coverage of the `DataLoaderGraph` functionalities.
- `#039 20:20 d9ceff4` +1672/-320 (9f) [docs,gbp,tests] — **refactor(core): enhance RawModelData structure and introduce quick-start factory**
    > - Added a new `REFACTORING_SPEC.md` document outlining a 4-step refactoring plan to improve code clarity and navigation within the `gbp` architecture.
    > - Introduced logical grouping of fields in `RawModelData` and `ResolvedModelData` to enhance readability while maintaining backward compatibility.
    > - Created a `make_raw_model` factory function to simplify the creation of `RawModelData` from minimal inputs, lowering the entry barrier for experimentation.
    > - Updated `DataLoaderGraph` to utilize the new `BikeShareSourceProtocol`, ensuring a more domain-agnostic approach.
    > - Enhanced unit tests for the `make_raw_model` function to validate its functionality and ensure comprehensive coverage of the new features.
    > - Revised documentation across various modules to reflect the changes and improve overall clarity for developers and stakeholders.
- `#040 20:36 902b58e` +590/-0 (1f) [docs] — **feat(docs): introduce comprehensive data journey documentation for bike-sharing**
    > - Added a new document `DATA_JOURNEY.md` detailing the complete data journey for a bike-sharing example, illustrating the flow from raw data generation to graph model transformation.
    > - Included specific sections on real-world scenarios, data loading processes, and the transformation of domain-specific data into universal tables.
    > - Documented the structure and attributes of generated data, including DataFrames for stations, depots, and inventory snapshots, enhancing clarity for developers and stakeholders.
    > - Provided detailed explanations of the data loading and processing steps, including the roles and operations of facilities within the network.
    > - Improved overall documentation coherence to facilitate better understanding of the data journey and its implications for the optimization platform.
- `#041 21:34 473a97f` +687/-0 (1f) [docs] — **feat(docs): introduce dynamic attribute system design documentation**
    > - Added a new document `ATTRIBUTE_SYSTEM_DESIGN.md` detailing the design and implementation of a dynamic attribute system for the optimization platform.
    > - Outlined the context, current limitations, and goals for the AttributeBuilder API, emphasizing the need for flexibility in registering custom attributes.
    > - Described the architectural separation between structural and parametric tables, clarifying the distinction and their roles within the system.
    > - Introduced the `RegisteredAttribute` and `AttributeRegistry` classes, providing a central API for managing parametric attributes and their validation.
    > - Documented the registration process for attributes, including validation steps and the handling of dynamic grain definitions.
    > - Enhanced overall documentation coherence to facilitate better understanding of the attribute system's design and its implications for the platform.
- `#042 21:52 a8ccefc` +960/-97 (9f) [gbp,tests] — **feat(core): implement dynamic attribute registry and enhance model data structure**
    > - Introduced the `AttributeRegistry` class to manage parametric attributes, providing a central API for registration, validation, and retrieval.
    > - Enhanced `RawModelData` and `ResolvedModelData` to include an `attributes` field, allowing for dynamic attribute management.
    > - Updated methods to serialize and deserialize the attribute registry, ensuring compatibility with existing data structures.
    > - Improved documentation to clarify the new attribute system and its integration within the model data classes.
    > - Added unit tests for the `AttributeRegistry` to validate registration processes and ensure robust functionality.
    > - Revised existing tests to accommodate changes in the model data structure and attribute handling.
- `#043 22:16 1284fa3` +510/-215 (10f) [docs,gbp,tests] — **refactor(core): clean up fixed parametric fields in model data**
    > - Removed fixed parametric fields from `RawModelData` and `ResolvedModelData`, consolidating all parametric data management through the `AttributeRegistry`.
    > - Updated related properties and methods to reflect the new structure, ensuring that all parametric attributes are accessed via the registry.
    > - Revised the `build_model()` pipeline to eliminate hardcoded references to removed fields, enhancing flexibility and maintainability.
    > - Adjusted `DataLoaderGraph` to register parametric data exclusively through the `AttributeRegistry`, streamlining data loading processes.
    > - Updated tests to validate the new attribute access patterns and ensure comprehensive coverage of the changes.
    > - Enhanced documentation to clarify the updated model structure and the transition to a unified attribute management system.
- `#044 22:23 f88ac04` +184/-392 (1f) [docs] — **docs(DATA_JOURNEY): update data journey documentation for bike-sharing**
    > - Revised the structure of the `DATA_JOURNEY.md` document to clarify the architectural separation between structural tables and parametric attributes.
    > - Enhanced explanations of how structural tables describe graph topology and how parametric attributes are registered and utilized within the system.
    > - Updated section headings to reflect the new organization, including renumbering sections for better clarity.
    > - Improved overall documentation coherence to facilitate understanding of the data journey from raw data generation to graph model transformation.

## 2026-03-24 (вт) — 4 коммитов, +1849/-1338

- `#045 20:07 abff99f` +251/-1258 (9f) [.cursorrules,PROJECT_STATE.md,SPEC.md,do] — **refactor(docs): update project documentation and remove obsolete files**
    > - Replaced the existing `.cursorrules` file with a new foundational document outlining the Graph-Based Logistics Platform, including its core concepts, architecture, and current phase.
    > - Introduced `PROJECT_STATE.md` to provide a comprehensive overview of the project's vision, roadmap, and current progress, detailing the stabilization of the core library.
    > - Deleted outdated `SPEC.md`, `CLEANUP_PARAMETRIC_FIELDS.md`, and `repo_struct.md` files to streamline documentation and eliminate redundancy.
    > - Added `docs/design/attribute_system.md` and `docs/design/graph_data_model.md` to enhance understanding of the dynamic attribute system and the overall data model structure.
    > - Improved overall documentation coherence and clarity to facilitate better navigation and comprehension for developers and stakeholders.
- `#046 20:15 dc57ec4` +59/-0 (1f) [PROJECT.md] — **feat(docs): add foundational project documentation for the Graph-Based Logistics Platform**
    > - Introduced `PROJECT.md`, outlining the vision, roadmap, and principles of the platform, emphasizing the core concept of the Environment for simulating logistics networks.
    > - Detailed the two levels of tasks: operational (step-by-step simulation) and strategic (one-time optimization), clarifying their distinct roles and data handling.
    > - Established a clear roadmap for future phases, ensuring alignment with the project's goals and current progress.
    > - Enhanced documentation coherence to facilitate better understanding for developers and stakeholders.
- `#047 21:16 0cc2575` +1443/-12 (9f) [CLAUDE.md,PROJECT_STATE.md,gbp,notebooks] — **feat(docs): add CLAUDE.md for project guidance and update notebooks**
    > - Introduced `CLAUDE.md`, providing comprehensive guidance for working with the Graph-Based Logistics Platform, including project overview, commands, architecture, data model invariants, code style, and AI collaboration rules.
    > - Added `05_pipeline_walkthrough.ipynb` to illustrate the step-by-step process of the `build_model()` function, enhancing understanding of data transformations within the pipeline.
    > - Created `01_attribute_registry_edge_cases.ipynb` to verify new validations in the `AttributeRegistry`, ensuring robust error handling and clarity in attribute registration.
    > - Updated `PROJECT_STATE.md` to reflect the completion of key tasks and progress in the current phase, including stabilization of the `AttributeRegistry`.
- `#048 21:31 cb99097` +96/-68 (2f) [gbp,notebooks] — **feat(core): add factory method to construct ResolvedModelData from raw data**
    > - Introduced `from_raw()` class method in `ResolvedModelData` to centralize the construction process from raw tables and build artifacts.
    > - The new method allows for a streamlined mapping of raw fields to resolved fields, enhancing consistency across the platform.
    > - Updated the `05_pipeline_walkthrough.ipynb` notebook to utilize the new factory method, improving clarity in the assembly of `ResolvedModelData`.
    > - Enhanced documentation to reflect the changes and provide guidance on using the new method effectively.

## 2026-03-26 (чт) — 1 коммитов, +3019/-61

- `#049 22:02 f9ac6a1` +3019/-61 (3f) [PROJECT_STATE.md,docs,notebooks] — **Refactor code structure for improved readability and maintainability, added design doc for env, updated project state**

## 2026-03-27 (пт) — 4 коммитов, +1464/-1013

- `#050 20:05 5c679b5` +1017/-1007 (19f) [.claude,.github,.gitignore,docs,pyprojec] — **Refactor documentation and code structure for GBP**
    > - Updated `graph_data_model.md` to clarify parametric attributes and their registration.
    > - Completed the architecture clarity refactoring in `refactoring.md`, detailing the four-step process to improve code navigation and understanding.
    > - Added an index page for GBP documentation, summarizing the platform and its pipeline.
    > - Created detailed module documentation for `gbp.build`, `gbp.core`, `gbp.io`, `gbp.loaders`, `gbp.loading`, and `gbp.rebalancer`, outlining their functionalities and key components.
    > - Introduced a new factory function in `gbp.core.factory` for quick-start model creation.
    > - Enhanced the `pyproject.toml` to include `pydata-sphinx-theme` for improved documentation styling.
- `#051 19:31 7b9cb45` +445/-4 (4f) [docs,pyproject.toml] — **docs: redesign Sphinx documentation with professional styling**
    > - Add Furo theme color palette (dark sidebar, teal-blue brand) with full dark mode
    > - Add custom CSS: Inter/JetBrains Mono fonts, card grid, pipeline visualization
    > - Redesign index.md as landing page with Core Concepts, Package Reference cards,
    > and Architecture & Design section (previously orphaned docs now discoverable)
    > - Enable sphinxcontrib-mermaid for diagram rendering in architecture_diagrams.md
    > - Fix pyproject.toml docs deps: replace unused pydata-sphinx-theme with furo
    > - Add GitHub source links and footer icon
    > https://claude.ai/code/session_013LHChDkoeqgVSLSh77wT5X
- `#052 20:32 af15575` +0/-0 (0f) [] — **Merge pull request #1 from vlzm/claude/html-preview-collaboration-alkEk**
    > Enhance documentation styling and add Mermaid diagram support
- `#053 20:40 e6423a5` +2/-2 (1f) [docs] — **fix(docs): improve compatibility of build script with Windows paths**

## 2026-03-28 (сб) — 2 коммитов, +3574/-40

- `#054 12:47 df90d4d` +2669/-34 (25f) [PROJECT_STATE.md,gbp,notebooks,tests] — **Add integration and unit tests for simulation engine and phases**
    > - Introduced a new Jupyter notebook for verifying the complete simulation pipeline, including building models, initializing states, and executing demand, arrivals, and dispatch phases.
    > - Created integration tests for the Environment simulation engine, ensuring the full pipeline functions correctly with various phases.
    > - Developed unit tests for DemandPhase and ArrivalsPhase, validating their execution and interaction with inventory and resources.
    > - Implemented tests for DispatchPhase, focusing on dispatch validation, resource assignment, and rejection scenarios.
    > - Added tests for the SimulationLog, ensuring correct logging of inventory, flow events, and rejected dispatches.
    > - Established shared fixtures for simulator tests to streamline the setup of resolved models with initial inventory.
    > - Enhanced test coverage for the Phase protocol, PhaseResult, and Schedule functionalities.
- `#055 14:51 63a8915` +905/-6 (10f) [.claude,CLAUDE.md,PROJECT_STATE.md,docs] — **Add detailed diagrams and storytelling documentation for environment simulation**
    > - Created "13. Environment Engine — Simulation Loop" diagram to illustrate the simulation process and state transitions.
    > - Developed "14. Environment Phases — Per-Period Execution Detail" diagram to detail the execution of different phases within the simulation.
    > - Added PNG representation of the environment simulation loop for visual reference.
    > - Introduced storytelling guides for the graph data model, attribute system, and environment design, providing comprehensive explanations of the underlying concepts and structures.

## 2026-03-31 (вт) — 2 коммитов, +3188/-393

- `#056 20:35 b588472` +1450/-9 (9f) [.claude,docs,gbp,notebooks] — **Add notebooks for mock data simulation and model validation**
    > - Created `workbook.ipynb` to demonstrate the loading and validation of mock data using `DataLoaderMock` and `DataLoaderGraph`.
    > - Implemented `03_mock_simulation.ipynb` to run a full simulation pipeline with mock data, including demand injection and inventory analysis.
    > - Included detailed markdown explanations and code cells for each step of the simulation process.
- `#057 21:48 438a5b4` +1738/-384 (12f) [PROJECT_STATE.md,docs,gbp,notebooks,test] — **Add tests for ObservedFlow and ObservedInventory schemas**
    > - Implement tests to validate the ObservedFlow schema, including checks for valid data, optional fields, default values, and rejection of invalid inputs.
    > - Implement tests for the ObservedInventory schema, ensuring valid data and rejection of negative quantities and extra fields.
    > - Add a new test class for observations in test_graph_loader.py to verify the population and integrity of observed flow and inventory data.

## 2026-04-01 (ср) — 3 коммитов, +482/-554

- `#058 19:37 163b3d2` +448/-426 (16f) [PROJECT_STATE.md,gbp,pyproject.toml,test] — **refactored the code**
- `#059 20:40 16df4da` +11/-8 (2f) [CLAUDE.md,PROJECT_STATE.md] — **Consolidate loading utilities into loaders directory and update documentation**
- `#060 21:46 678ba3c` +23/-120 (13f) [docs,gbp,tests] — **Refactor data source protocols: rename DataSourceProtocol to BikeShareSourceProtocol and remove deprecated functions**

> **пауза 6 дней**

## 2026-04-07 (вт) — 1 коммитов, +4705/-340

- `#061 22:56 1962f26` +4705/-340 (15f) [CLAUDE.md,gbp,notebooks,pyproject.toml,t] — **Refactor mock simulation notebook and tests for improved clarity and accuracy**
    > - Updated the simulation notebook to clarify the purpose and structure of the mock simulation, ensuring it reproduces historical inventory accurately.
    > - Modified the configuration for mock data generation to use more descriptive keys.
    > - Enhanced tests to validate commodity categories and ensure proper handling of inventory and demand across different phases.
    > - Added assertions to verify that observed inventory and flow data include all commodity categories.
    > - Adjusted the rebalancer tests to account for station capacities and inventory distribution by commodity type.
    > - Updated dependencies in `pyproject.toml` to include visualization libraries for enhanced data representation.

## 2026-04-10 (пт) — 1 коммитов, +143/-75

- `#062 22:16 d2df040` +143/-75 (18f) [PROJECT_STATE.md,gbp,notebooks,tests] — **Refactor schemas and loaders to remove unit fields**
    > - Updated PROJECT_STATE.md to reflect the removal of unit fields from various schemas.
    > - Removed `*_unit` fields from Demand, Supply, Inventory, Edge, and other related schemas.
    > - Adjusted factory and loader functions to align with the new schema definitions.
    > - Updated tests to remove references to quantity_unit and distance_unit.
    > - Cleaned up unnecessary validation checks related to unit consistency.

> **пауза 5 дней**

## 2026-04-15 (ср) — 3 коммитов, +5250/-5276

- `#063 20:13 e30c218` +526/-228 (11f) [.claude,PROJECT_STATE.md,docs,gbp,notebo] — **feat: add DistanceMatrix schema and refactor edge handling**
    > - Introduced DistanceMatrix class to represent pairwise distances and travel durations between facilities.
    > - Updated GraphLoaderConfig to clarify edge building process.
    > - Refactored DataLoaderGraph to compute distance matrix instead of edges, enhancing clarity and separation of concerns.
    > - Adjusted related notebooks to reflect changes in data loading and processing.
    > - Added a new skill for Ousterhout architecture review to analyze code against design principles.
- `#064 21:53 71d580c` +4611/-5048 (17f) [docs,gbp,notebooks,tests] — **Refactor model loading and derivation process**
    > - Updated the `04_observations.ipynb` notebook to change the data loading process and remove unnecessary outputs.
    > - Introduced a new notebook `05_build_derivations.ipynb` to verify the derivation of tables during the model building process.
    > - Modified `conftest.py` to include a fixture for resolved graph models, ensuring tests use the built model data.
    > - Updated tests in `test_graph_loader.py` to reflect changes in the loading and building process, ensuring that derived data is correctly validated.
    > - Added new tests in `test_engine_preconditions.py` to check simulator preconditions regarding flow inputs.
- `#065 22:32 75200c9` +113/-0 (1f) [docs] — **feat: add Architecture Map for project overview and guidelines**

## 2026-04-16 (чт) — 1 коммитов, +925/-881

- `#066 21:47 72ed0eb` +925/-881 (6f) [gbp,notebooks] — **Add minimal loader notebook for bike-sharing contract verification**
    > - Introduced a new Jupyter notebook `06_minimal_loader.ipynb` to verify the minimal bike-sharing contract.
    > - Implemented tests for `DataLoaderMockMinimal` and `DataLoaderGraph` to ensure correct data loading and model building.
    > - Included steps to check populated data fields, build model derivations, and simulate the environment with demand and arrivals phases.
    > - Added summaries for raw data and resolved model data to facilitate analysis.

## 2026-04-17 (пт) — 2 коммитов, +889/-168

- `#067 20:16 002ecaa` +522/-145 (4f) [docs,notebooks] — **Refactor data pipeline and environment initialization in documentation and notebook**
    > - Updated the data pipeline steps in `01_graph_data_model.md` to reflect new functions and processes for building the model from raw data.
    > - Enhanced the explanation of the environment initialization process in `03_environment.md`, detailing how inventory and resources are set up.
    > - Improved the interactive dashboard in `workbook.ipynb` by refining the simulation and data generation functions, ensuring better clarity and functionality.
- `#068 22:43 91cddf2` +367/-23 (1f) [notebooks] — **Refactor workbook.ipynb: Update execution counts, remove stderr outputs, and enhance data generation logic for stations and trips**

## 2026-04-18 (сб) — 1 коммитов, +575/-0

- `#069 22:13 a4d8e14` +575/-0 (2f) ["docs] — **Add visual diagrams for understanding levels in GBP**
    > This commit introduces a new markdown file containing diagrams that illustrate the various levels of understanding within the GBP system. Each diagram corresponds to a specific level of comprehension, detailing the interactions and components visible at that level. The file serves as a visual aid to complement the existing documentation on code understanding levels, enhancing clarity on user interactions, module contracts, and system architecture.

## 2026-04-19 (вс) — 1 коммитов, +143/-37

- `#070 20:13 f35d268` +143/-37 (1f) ["docs] — **Add data contract reference guide with detailed descriptions for RawModelData, ResolvedModelData, and SimulationLog**

## 2026-04-20 (пн) — 2 коммитов, +2604/-1

- `#071 00:57 9a89e81` +1/-1 (1f) [.gitignore] — **Fix .gitignore: Ensure 'build/' directory is correctly ignored**
- `#072 19:28 73e2225` +2603/-0 (23f) [gbp,tests] — **Add unit tests and fixtures for build pipeline components**
    > - Created `__init__.py` for unit tests in the `build` module.
    > - Added `fixtures.py` with minimal bike-sharing `RawModelData` fixtures for testing.
    > - Implemented unit tests for derivation helpers in `test_defaults.py`.
    > - Developed tests for edge building logic in `test_edge_builder.py`.
    > - Created tests for fleet capacity computation in `test_fleet_capacity.py`.
    > - Added tests for lead time resolution in `test_lead_time.py`.
    > - Implemented integration tests for the build model pipeline in `test_pipeline.py`.
    > - Developed tests for spine assembly in `test_spine.py`.
    > - Added tests for time resolution in `test_time_resolution.py`.
    > - Created tests for transformation resolution in `test_transformation.py`.
    > - Implemented validation tests for raw model in `test_validation.py`.

> **пауза 6 дней**

## 2026-04-26 (вс) — 2 коммитов, +2123/-726

- `#073 12:03 6e10bf3` +1016/-669 (5f) [gbp,notebooks] — **up to date**
- `#074 13:28 bd92527` +1107/-57 (2f) [gbp,notebooks] — **Refactor code structure for improved readability and maintainability**

## 2026-04-27 (пн) — 2 коммитов, +848/-1899

- `#075 22:09 377c7c7` +226/-1829 (11f) [CLAUDE.md,PROJECT_STATE.md,gbp,notebooks] — **Refactor inventory handling in DataLoaderMock and DataLoaderGraph**
    > - Introduced a new Jupyter notebook to verify the refactor of inventory data handling.
    > - Removed the `df_inventory_ts` attribute from the mock surface, replacing it with a long-format `inventory_initial` DataFrame.
    > - Updated tests to reflect changes in inventory structure, ensuring that the new `inventory_initial` DataFrame is correctly populated and validated.
    > - Deleted obsolete test file `test_rebalancer.py` as it is no longer relevant to the current implementation.
- `#076 23:28 af1a216` +622/-70 (7f) [PROJECT_STATE.md,gbp,notebooks,tests] — **feat: Split OrganicFlowPhase into OrganicDeparturePhase and OrganicArrivalPhase**
    > - Added OrganicDeparturePhase to handle outflow from source facilities.
    > - Added OrganicArrivalPhase to manage inflow to target facilities and clip inventory.
    > - Updated PROJECT_STATE.md to reflect new phases and verification notebooks.
    > - Modified __init__.py to include new phases in the public API.
    > - Enhanced built_in_phases.py with new phase implementations and logic.
    > - Created a new verification notebook to ensure parity between combined and split phase simulations.
    > - Updated tests to cover new phase functionality and ensure correct behavior.

## 2026-04-30 (чт) — 2 коммитов, +3691/-791

- `#077 21:34 6511eff` +1952/-9 (12f) [gbp,notebooks,tests] — **Add new phases and logging for latent demand, lost demand, and dock blocking**
    > - Introduced `HistoricalLatentDemandPhase`, `HistoricalODStructurePhase`, `DeparturePhysicsPhase`, `HistoricalTripSamplingPhase`, and `DockCapacityPhase` with corresponding tests.
    > - Enhanced logging capabilities by adding new log tables for latent demand, lost demand, and dock blocking.
    > - Updated integration and unit tests to cover new phases and ensure correct logging behavior.
    > - Implemented tests for random number generator stability and state management for intermediates.
- `#078 22:46 fefea9a` +1739/-782 (23f) [.claude,gbp,notebooks,tests] — **Refactor simulator event handling and improve test coverage**
    > - Consolidated event handling in PhaseResult to use a unified events dictionary.
    > - Updated notebooks to reflect changes in event handling and display.
    > - Enhanced unit tests for DemandPhase, DispatchPhase, and inventory management to utilize new event structure.
    > - Added new tests for dispatch lifecycle to ensure proper state transitions and event emissions.
    > - Normalized consumer tables in ResolvedModelData to default to empty DataFrames for consistency.
    > - Improved assertions in tests to check for event presence and correctness.

## 2026-05-01 (пт) — 4 коммитов, +4353/-2019

- `#079 09:43 9020dae` +0/-116 (1f) [.cursorrules] — **Delete .cursorrules**
- `#080 19:47 9ae37b7` +4313/-20 (22f) [.claude,.omc,gbp,notebooks,tests] — **Add unit tests for RebalancerTask and LatentDemandInflatorPhase; enhance DataLoaderMock for truck configurations**
    > - Implemented unit tests for RebalancerTask covering various scenarios including imbalance handling and resource assignment.
    > - Added tests for LatentDemandInflatorPhase to validate demand scaling and integration with other phases.
    > - Enhanced DataLoaderMock to support truck configurations, ensuring correct behavior with zero and multiple trucks.
    > - Updated existing tests to reflect changes in DataLoaderMock and added new tests for truck-related functionality.
- `#081 19:47 13c64a4` +0/-0 (0f) [] — **Merge branch 'main' of https://github.com/vlzm/GFDRR**
- `#082 19:55 3b22184` +40/-1883 (16f) [.omc,CLAUDE.md,PROJECT_STATE.md,gbp,note] — **Refactor code structure and remove redundant sections for improved readability and maintainability**

## 2026-05-02 (сб) — 1 коммитов, +1622/-176

- `#083 22:24 96599b6` +1622/-176 (10f) [.omc,notebooks] — **Add initial Jupyter notebook for model verification and treatment run simulation**
    > - Implemented imports and setup for the simulation environment
    > - Configured mock data loader with specified parameters
    > - Built a resolved mock model and defined treatment phases including rebalancer task
    > - Executed treatment run and logged results for resource management
    > - Added dataframes for simulation resource logs and treatment results

## 2026-05-03 (вс) — 1 коммитов, +686/-40

- `#084 22:35 8932111` +686/-40 (16f) [.omc,docs,gbp,notebooks,tests] — **feat: add consumption and production operations to role derivation**
    > - Introduced new OperationType values: CONSUMPTION and PRODUCTION.
    > - Updated role derivation logic to include SINK and SOURCE roles based on the new operations.
    > - Enhanced documentation to clarify the role derivation process and its symmetry.
    > - Added tests to verify the correct addition of SINK and SOURCE roles when using CONSUMPTION and PRODUCTION.
    > - Updated existing enums and roles to accommodate the new operations without affecting existing functionality.
    > - Created new notebooks for verifying the implementation of the new operations in various scenarios.

## 2026-05-05 (вт) — 2 коммитов, +3134/-237

- `#085 23:08 d35aa89` +2744/-178 (34f) [.omc,docs,gbp,notebooks,tests] — **Add tests and extend ObservedFlow schema with duration_hours**
    > - Introduced duration_hours to ObservedFlow schema, allowing for tracking of duration in hours.
    > - Added validation for duration_hours to ensure it can be zero but not negative.
    > - Implemented round-trip tests for ObservedFlow with parquet to ensure dtype consistency for duration_hours.
    > - Created unit tests for HistoricalTripSamplingPhase to validate handling of duration_hours in various scenarios.
    > - Developed tests for InvariantCheckPhase to ensure invariants hold with the new duration_hours data.
    > - Added tests for OverflowRedirectPhase to verify behavior with storage capacities and overflow detection.
    > - Enhanced integration tests for the historical replay pipeline to include new logging for redirected flows and invariant violations.
- `#086 23:31 3297768` +390/-59 (6f) [.omc,gbp,tests] — **Update project memory and state files; enhance OverflowRedirectPhase logic and tests**

## 2026-05-06 (ср) — 1 коммитов, +559/-343

- `#087 22:50 698eb88` +559/-343 (8f) [.omc,gbp,notebooks,tests] — **feat: add EndOfPeriodDeficitPhase to handle end-of-period negative inventory**
    > - Implemented EndOfPeriodDeficitPhase to record end-of-period negative inventory as lost demand.
    > - The phase clips inventory to zero and reduces in-transit shipments to cover deficits.
    > - Added detailed docstring explaining the phase's behavior and placement in the pipeline.
    > - Updated historical replay notebook to include tests for end-of-period deficit handling.
    > - Created unit tests for EndOfPeriodDeficitPhase covering various scenarios including no negative inventory, clipping, tolerance handling, and integration with the historical replay pipeline.

## 2026-05-07 (чт) — 1 коммитов, +921/-60

- `#088 17:03 90fc598` +921/-60 (4f) [gbp,notebooks,tests] — **Add unit tests for IntervalOverlapPlanner in rebalancer_planner**
    > - Implement tests to validate the functionality of the IntervalOverlapPlanner.
    > - Include tests for empty inputs, known-good examples, truck capacity clipping, and distance matrix handling.
    > - Utilize pandas DataFrames to construct source and destination frames for testing.

## 2026-05-09 (сб) — 2 коммитов, +36/-4798

- `#089 23:19 dc1305d` +36/-4069 (40f) [.omc,PROJECT.md,docs] — **Remove outdated documentation files related to bike-sharing simulation and graph data model; update attribute system and environment design documentation for clarity and structure; add session state files for tracking simulation sessions.**
- `#090 23:45 2340a24` +0/-729 (3f) [docs] — **refactor: remove outdated design documents for attribute system and observations**

## 2026-05-11 (пн) — 3 коммитов, +6574/-26084

- `#091 17:17 71be3f3` +3198/-25260 (109f) ["docs,PROJECT.md,PROJECT_STATE.md,docs,n] — **Add historical replay pipeline notebook for algorithm verification**
    > - Implemented a comprehensive Jupyter notebook to verify the Historical Replay pipeline as per the deep-interview specifications.
    > - The notebook includes sections for building a resolved bike-sharing model, computing conservation baselines, and running the historical replay phases.
    > - Added detailed checks for inventory and flow parity, per-commodity conservation, and overflow safety-net status.
    > - Included end-of-period deficit analysis with treatment scenarios for lost demand.
- `#092 18:06 8e09703` +3310/-772 (66f) [.claude,CLAUDE.md,gbp,pyproject.toml] — **Enhance documentation across loaders and parquet modules**
    > - Added detailed docstrings to various functions in `gbp/io/parquet.py`, including parameter and return descriptions for better clarity.
    > - Improved docstrings in `gbp/loaders/contracts.py`, `gbp/loaders/csv_loader.py`, and `gbp/loaders/dataloader_graph.py` to include parameter and return information.
    > - Updated `gbp/loaders/dataloader_mock.py` and `gbp/loaders/dataloader_mock_minimal.py` with clearer descriptions of parameters and functionality.
    > - Refined docstrings in `gbp/loaders/protocols.py` to specify method functionalities and return types.
    > - Enhanced validation function documentation in `gbp/loaders/validators.py` to clarify parameters and return values.
    > - Changed documentation style in `pyproject.toml` from Google to NumPy convention for consistency.
- `#093 23:23 7ef4c25` +66/-52 (4f) [.claude,CLAUDE.md] — **Add code style, epistemic rules, and post-task process documentation**

## 2026-05-12 (вт) — 3 коммитов, +2782/-5437

- `#094 16:00 b0a6137` +110/-0 (1f) [notebooks] — **Add canonical scenario notebook for historical replay simulation**
- `#095 17:31 334a783` +638/-4787 (41f) [.claude,CLAUDE.md,PROJECT.md,PROJECT_STA] — **Refactor loaders: Remove GenericSourceProtocol and validators.py**
    > - Deleted the GenericSourceProtocol class from protocols.py as it was aspirational and not implemented.
    > - Removed validators.py file which contained column validation helpers for DataFrames, as it is no longer needed.
    > - Updated canonical_scenario.ipynb to streamline imports and reduce code size by consolidating multiple lines into single lines.
    > - Deleted workbook_mock.ipynb as it is no longer relevant to the current implementation.
- `#096 18:24 38fe27b` +2034/-650 (11f) [docs,gbp,notebooks,pyproject.toml,tests] — **Refactor rebalancer.py: Move distance map functions to distance module**
    > - Moved `_build_edge_distance_map` and `_create_distance_matrix` functions to `gbp.consumers.simulator.distance` module.
    > - Updated imports in `rebalancer.py` to use the new functions.
    > - Removed redundant distance calculation functions from `rebalancer.py`.
    > Update canonical scenario notebook: Fix import for OverflowRedirectPhase
    > - Changed import statement for `OverflowRedirectPhase` to reflect its new location in the codebase.
    > Enhance Ruff configuration: Ignore specific docstring warnings for tests
    > - Added per-file ignores for docstring warnings in test files to improve linting flexibility.
    > Add unit tests for build pipeline
    > - Created `test_build_pipeline.py` to validate the output of `build_model` against expected resolved tables.
    > - Included tests for demand, supply, periods, facilities, edges, inventory, resources, and commodity categories.
    > Implement integration tests for canonical scenario
    > - Added `test_canonical_scenario.py` to run end-to-end tests on the canonical scenario.
    > - Verified invariants such as no invariant violations, no rejected dispatches, and conservation of inventory.
    > Introduce unit tests for dispatch lifecycle
    > - Created `test_dispatch_lifecycle.py` to cover various scenarios in the dispatch lifecycle.
    > - Included tests for successful dispatches, rejection reasons, resource auto-assignment, and sequential inventory consumption.

## 2026-05-13 (ср) — 1 коммитов, +501/-66

- `#097 15:30 c51369b` +501/-66 (13f) [.omc,AGENTS.md,CLAUDE.md,notebooks] — **Update project memory, add session files, and enhance documentation**
    > - Updated project memory with new last scanned timestamps and modified file access details.
    > - Added multiple session JSON files to track session details, including session IDs, end times, and modes used.
    > - Created AGENTS.md to outline the Citi Bike Simulation Platform and its guidelines.
    > - Updated CLAUDE.md with refined universal rules for code and communication.
    > - Enhanced canonical_scenario.ipynb with execution outputs and improved logging.
    > - Introduced a new workbook.ipynb for additional data loading and processing tasks.

## 2026-05-15 (пт) — 3 коммитов, +419/-3903

- `#098 16:30 88796c3` +0/-3243 (29f) [.omc] — **Remove obsolete state files and deep interview specification for the rebalancer task. This includes the deletion of the deep interview spec, autopilot state, deep interview state, mission state, and various session state files. These changes reflect the completion of the historical bike replay pipeline and the transition to the next phase of development.**
- `#099 16:31 6888921` +0/-579 (2f) [docs] — **Remove obsolete cleanup plan document for vertical Citi Bike minimum**
- `#100 20:55 c43eee3` +419/-81 (1f) [notebooks] — **Update execution counts and add output cells for timestamps and station costs in the workbook**

## 2026-05-16 (сб) — 1 коммитов, +137/-155

- `#101 21:17 2c476dc` +137/-155 (1f) [notebooks] — **Update execution counts, modify display options, and clean up notebook structure**

> **пауза 13 дней**

## 2026-05-29 (пт) — 1 коммитов, +1269/-388

- `#102 21:20 214491a` +1269/-388 (9f) [.omc,notebooks] — **Add project memory and session files; implement bike rebalancing notebook**
    > - Created project memory JSON file to store project details including tech stack, build commands, and directory structure.
    > - Added multiple session JSON files to track session details such as session ID, end time, and reasons for session termination.
    > - Introduced a new Jupyter notebook for bike rebalancing using OR-Tools, including detailed implementation of the routing model and output results.

## 2026-05-31 (вс) — 2 коммитов, +15860/-15714

- `#103 16:24 8856176` +15635/-15680 (75f) ["docs,.claude,.gitignore,.omc,AGENTS.md,] — ** to date**
- `#104 22:24 a0eb6e3` +225/-34 (1f) [notebooks] — **Refactor depot and truck data handling; update variable names for clarity and consistency**

## 2026-06-01 (пн) — 4 коммитов, +380/-404

- `#105 21:24 0e2d58e` +238/-313 (1f) [notebooks] — **Refactor data loading functions and enhance initial inventory setup**
    > - Updated the `load_trips_raw_df` function to include additional columns for dropping NaN values.
    > - Refactored `get_initial_inventory_df` to clarify the handling of electric and classic bikes.
    > - Improved the structure and readability of various data processing functions.
    > - Added detailed comments and documentation for clarity.
    > - Adjusted configuration paths and parameters for better usability.
- `#106 21:32 337dd9a` +137/-87 (1f) [notebooks] — **Refactor notebook structure; update execution counts and reorganize function definitions**
- `#107 21:46 5046c30` +4/-4 (1f) [notebooks] — **Update execution counts and reorder period grid initialization in notebook**
- `#108 21:50 d9566bc` +1/-0 (1f) [notebooks] — **Add comment for future configuration of period grid recalculation**

## 2026-06-02 (вт) — 3 коммитов, +1910/-1741

- `#109 17:29 d96d4fe` +1667/-5 (1f) [notebooks] — **Implement feature X to enhance user experience and fix bug Y in module Z**
- `#110 19:59 6ed4b58` +187/-1713 (7f) [.omc,notebooks] — **Refactor code structure for improved readability and maintainability**
- `#111 21:38 669a41a` +56/-23 (1f) [notebooks] — **Update comments and documentation in the notebook for clarity and organization**

## 2026-06-03 (ср) — 6 коммитов, +3458/-14893

- `#112 15:41 f7ace41` +997/-245 (8f) [notebooks] — **Refactor code structure for improved readability and maintainability**
- `#113 16:50 4b3175d` +933/-0 (5f) [notebooks] — **Implement core simulation engine and phases for historical replay functionality**
- `#114 19:09 3c59c58` +443/-5163 (11f) ["docs,.github,docs,notebooks,tests] — **Remove integration and unit tests for the dispatch lifecycle and canonical scenario; add design document for the simulation engine environment.**
- `#115 19:13 f6e0756` +0/-9153 (46f) [gbp] — **Remove unused schemas and role derivation logic from the GBP core module**
    > - Deleted the `roles.py` file which contained logic for deriving facility roles based on operations and types.
    > - Removed the entire `schemas` directory, including all related Pydantic models for behavior, demand/supply, edge, entity, observations, resource, scenario, temporal, and transformation.
    > - Eliminated the `contracts.py` and `protocols.py` files from the loaders package, which defined configurations and protocols for data loading.
- `#116 19:16 703a39e` +0/-73 (4f) [gbp] — **Remove unused consumer and loader modules from the project**
- `#117 20:40 7687407` +1085/-259 (7f) [notebooks] — **Refactor simulator state management and add pipeline smoke test**
    > - Enhanced the `state.py` module with detailed flow-event schema, builders, and observation derivations.
    > - Introduced functions for managing flow journals, including `empty_flows_journal` and `finalize_flows`.
    > - Updated `SimulationState` class to include derived properties for departures, arrivals, demand, supply, and OD matrix.
    > - Removed the `SimulationLog` class and its associated methods for event recording.
    > - Added a new Jupyter notebook `test_pipeline.ipynb` to perform a smoke test of the simulation pipeline, ensuring simulated flows match historical data.
    > - Cleaned up the `workbook.ipynb` by updating comments and improving clarity on data structures and methods.

## 2026-06-05 (пт) — 1 коммитов, +1456/-2521

- `#118 21:44 12a81f7` +1456/-2521 (18f) [CLAUDE.md,PROJECT.md,PROJECT_STATE.md,gb] — **Refactor simulation phases and update test pipeline**
    > - Removed the `phases.py` file, consolidating phase logic for simulation.
    > - Updated import paths in `test_pipeline.ipynb` to reflect new module structure.
    > - Simplified markdown descriptions and code comments for clarity.
    > - Adjusted the simulation setup to ensure historical demand is reproduced accurately.
    > - Enhanced assertions to validate the equality of simulated and historical departures.

## 2026-06-07 (вс) — 4 коммитов, +1370/-2525

- `#119 15:24 dae6646` +712/-282 (12f) [.claude,gbp,notebooks] — **Refactor and enhance simulation pipeline**
    > - Removed outdated code style and epistemic rules documentation.
    > - Updated post-task process guidelines for project state validation.
    > - Refined Ousterhout review skill documentation for project-specific context.
    > - Consolidated update documentation skill to streamline the review process.
    > - Enhanced the engine and phases in the simulator to implement capacity-aware docking and overflow redirection.
    > - Introduced new functions for managing dock capacities and overflow handling.
    > - Updated dataloader to support raw Citi Bike data loading with improved structure and documentation.
    > - Modified test pipeline notebook to reflect changes in the simulation process and ensure accurate historical demand reproduction.
- `#120 15:54 d8b406b` +178/-638 (8f) [gbp,notebooks] — **Refactor simulation phases in notebooks**
    > - Consolidated the phases in `flat_env.ipynb` to streamline the simulation process, merging arrival and overflow handling into a single phase for improved clarity and efficiency.
    > - Updated `test_pipeline.ipynb` to reflect changes in phase definitions, replacing previous phase classes with a new `DockArrivals` class that handles both previous and same-period arrivals.
    > - Revised `workbook.ipynb` to implement the new phase structure, ensuring that the simulation accurately reflects historical data while preparing for future enhancements in demand handling.
    > - Removed redundant code and comments to enhance readability and maintainability across all notebooks.
- `#121 20:53 541ead3` +480/-1574 (3f) [notebooks] — **Refactor code structure for improved readability and maintainability**
- `#122 20:53 e54127c` +0/-31 (1f) [AGENTS.md] — **Remove AGENTS.md as part of codebase cleanup**

## 2026-06-08 (пн) — 5 коммитов, +820/-749

- `#123 14:02 780a0a1` +700/-659 (6f) [gbp] — **Split simulator state.py by secret into journal/state/mechanics**
    > state.py had grown into the whole simulator foundation under a name that
    > promised only the network state. Split it along three secrets, none of which
    > move into the phases:
    > - journal.py: the flow-event format and how to read it (schema, event
    > builders, finalize, and the marginal observations). Write and read stay
    > together so the column layout lives in one module.
    > - state.py: the network's facts at the current period -- SimulationState, the
    > run-config primitives (Schedule, PeriodRow, SimulatorConfigError) and the
    > inventory arithmetic that maintains it.
    > - mechanics.py: the rules a phase applies -- capacity-aware docking, overflow
    > redirect, demand realization and OD expansion.
    > Dependency direction is linear: journal <- state <- mechanics <- phases <-
    > engine. Function bodies are moved verbatim; the package public surface and the
    > notebook imports are unchanged. Verified end-to-end: the base-replay invariant
    > (simulated == historical departures/arrivals/inventory) still holds.
    > https://claude.ai/code/session_01Rf3PaXG8fGTswYshxFpQ3z
- `#124 16:03 589041d` +0/-0 (0f) [] — **Merge pull request #2 from vlzm/claude/magical-volta-npkC6**
    > Refactor: split state.py into journal, mechanics, and state modules
- `#125 15:13 ba9cf45` +22/-11 (7f) [gbp] — **Lift the flow journal into a gbp/model layer**
    > The flow journal is the shared vocabulary both the loaders (historical flows)
    > and the simulator (simulated flows) speak, yet it lived inside
    > consumers/simulator/. That made the loaders import from inside the simulator
    > package while the simulator imported ResolvedModelData back from the loaders --
    > a circular dependency between the two packages, and a location that misrepresented
    > the journal's role.
    > Move gbp/consumers/simulator/journal.py to gbp/model/journal.py, a neutral model
    > layer that depends on neither side. Now loaders -> model and simulator ->
    > {model, loaders}; the loaders no longer reach into the simulator and the cycle
    > is gone. Pure relocation plus import updates; the journal's public API and the
    > base-replay invariant are unchanged.
    > https://claude.ai/code/session_01Rf3PaXG8fGTswYshxFpQ3z
- `#126 17:19 55ecae6` +0/-0 (0f) [] — **Merge pull request #3 from vlzm/claude/magical-volta-npkC6**
    > Lift the flow journal into a gbp/model layer
- `#127 21:27 e775a00` +98/-79 (4f) [gbp,notebooks] — **Refactor overflow handling in mechanics and phases; enhance state adjustments**

## 2026-06-11 (чт) — 1 коммитов, +1363/-8

- `#128 21:36 1797792` +1363/-8 (8f) [.claude,gbp,notebooks] — **Add canonical phase diagram and refactor imports in simulator modules**
    > - Created a new markdown file for the canonical four phases diagram in the phase-diagram directory.
    > - Updated imports in engine.py, phases.py, state.py, and dataloader_graph.py to streamline access to journal functions.
    > - Added debugging functionality in the step method of the Environment class to facilitate troubleshooting.
    > - Modified test_pipeline.ipynb to reflect changes in execution counts and added debugging outputs for better traceability.

## 2026-06-12 (пт) — 1 коммитов, +2050/-2000

- `#129 20:58 c307ec6` +2050/-2000 (9f) [docs,gbp,notebooks] — **up to date**

## 2026-06-14 (вс) — 1 коммитов, +564/-71

- `#130 22:41 56ae5c0` +564/-71 (9f) [docs,gbp,notebooks,pyproject.toml] — **Implement loss logging for trips in the flow journal**
    > - Added a new design document for loss logging, detailing the problem, principles, and implementation steps.
    > - Introduced `lost_events` function to log stockout and dock-full losses in the journal.
    > - Updated `FormDeparturesPhase` and `DockArrivals` to emit loss events appropriately.
    > - Implemented run-level invariant checks (I1-I4) to ensure demand split and spine closure.
    > - Enhanced the inventory projection to account for lost events.
    > - Added `requests` as a new dependency in `pyproject.toml`.
    > - Updated the test pipeline notebook to validate run invariants and print loss summaries.

## 2026-06-16 (вт) — 2 коммитов, +605/-210

- `#131 22:44 3e311dd` +605/-192 (12f) [.claude,CLAUDE.md,Notations.md,gbp,pypro] — **Refactor simulation phases and state management for clarity and consistency**
    > - Updated docstrings across various classes and methods to improve clarity and consistency in terminology (e.g., "dock" to "park", "stock" to "inventory").
    > - Enhanced the `DockArrivals` phase to clarify the docking and redirecting process, including better handling of overflow bikes.
    > - Modified the `FormDeparturesPhase` to improve clarity on how demand is gated by inventory and how lost demand is recorded.
    > - Refined the `FormPotentialTripsPhase` to better describe the transformation of departure counts into actual trips.
    > - Adjusted the `SimulationState` class to reflect changes in terminology and improve method documentation.
    > - Updated the flow journal to clarify event definitions and improve the documentation of loss events.
    > - Improved the data loading functions to provide clearer descriptions of their purpose and functionality.
    > - Added type hints and improved type checking in various methods to enhance code quality and maintainability.
    > - Updated the `pyproject.toml` to configure mypy to ignore missing imports for pandas.
- `#132 22:45 b238b6d` +0/-18 (1f) [Notations.md] — **Remove outdated drift tracking section from Notations.md**

## 2026-06-17 (ср) — 3 коммитов, +2640/-477

- `#133 18:34 58cbdb2` +355/-141 (10f) [Notations.md,gbp,notebooks] — **Add EnvironmentConfig class and refactor simulation engine**
    > - Introduced EnvironmentConfig class to encapsulate simulation settings such as phases, seed, scenario ID, validation flag, demand scale factor, and number of periods.
    > - Updated the simulation engine to utilize the new EnvironmentConfig class, allowing for more flexible configuration of simulation runs.
    > - Modified phase execution methods to accept EnvironmentConfig, enabling dynamic adjustments based on configuration settings.
    > - Enhanced validation checks and demand handling to incorporate the demand scale factor.
    > - Refactored related documentation and comments for clarity and consistency.
    > Co-authored-by: Copilot <copilot@github.com>
- `#134 18:35 50535db` +467/-12 (1f) [notebooks] — **Update execution counts and adjust facility capacities in test_pipeline notebook**
- `#135 21:02 3b07e8e` +1818/-324 (6f) [gbp,notebooks] — **Refactor code structure for improved readability and maintainability**
    > Co-authored-by: Copilot <copilot@github.com>

## 2026-06-18 (чт) — 1 коммитов, +4036/-32

- `#136 21:36 01543b7` +4036/-32 (1f) [notebooks] — **Implement code changes to enhance functionality and improve performance**

## 2026-06-19 (пт) — 2 коммитов, +447/-7830

- `#137 21:42 96f590e` +394/-7830 (5f) [docs,notebooks] — **Refactor code structure for improved readability and maintainability**
- `#138 21:49 261b40d` +53/-0 (2f) [docs] — **Add guidance on using head, paper, and code for problem-solving and clarify the card's application per scenario**

> **пауза 4 дней**

## 2026-06-23 (вт) — 1 коммитов, +314/-1

- `#139 22:37 5501ba7` +314/-1 (2f) [docs,notebooks] — **Update GFDRR display name in test_pipeline notebook**

## 2026-06-24 (ср) — 4 коммитов, +1972/-453

- `#140 10:10 c9f0ef7` +730/-208 (6f) [Notations.md,gbp,notebooks] — **Refactor test_pipeline notebook: update execution counts, fix SettingWithCopyWarning, and modify data paths**
    > - Updated execution counts for code cells to maintain proper order.
    > - Added handling for SettingWithCopyWarning in dataloader_raw.py.
    > - Changed trips_path to use mac_path for consistency across environments.
    > - Introduced a new cell to display historical flows DataFrame with updated structure and content.
    > - Adjusted output formatting for improved readability in the notebook.
- `#141 15:49 008337f` +562/-2 (6f) [notebooks,tests] — **Update notebook and add test files for improved functionality and structure**
- `#142 19:39 77637ec` +272/-160 (1f) [notebooks] — **Refactor test_pipeline.ipynb: Update execution counts, fix attribute error, and adjust data paths**
    > - Updated execution count for specific code cells to reflect the correct order of execution.
    > - Fixed an AttributeError related to 'facilities_capacities_per_commodity_cat_df' in the ResolvedModelData class.
    > - Changed the data path from mac_path to ubuntu_path for consistency in data loading.
    > - Added a copy method to historical_flows_df to avoid SettingWithCopyWarning.
    > - Adjusted output columns in the final DataFrame to include facility_id and capacity.
    > - Updated the notebook metadata to reflect the correct Python version.
- `#143 19:47 88294c2` +408/-83 (1f) [notebooks] — **Fix historical flows DataFrame processing and update execution counts**
    > - Changed variable name from `historical_flows_df` to `historical_flows_df_raw` for clarity.
    > - Updated execution counts for several cells to reflect the new order of operations.
    > - Removed error handling for `facilities_capacities_per_commodity_cat_df` as it was not present in the `ResolvedModelData` class.
    > - Enhanced merging logic for historical flows to include planned and realized target capacities.
    > - Updated DataFrame column names to reflect new capacity calculations.
    > - Adjusted output display to show the correct number of columns after modifications.

## 2026-06-25 (чт) — 5 коммитов, +3246/-1311

- `#144 16:48 60e4b68` +2629/-25 (3f) [docs,notebooks] — **Fix execution counts and update facility costs in test_pipeline.ipynb; address SettingWithCopyWarning and adjust paths for data loading**
- `#145 17:12 51daf6b` +52/-2 (1f) [docs] — **Add section on unified automaton for user trips and rebalancing in transaction model**
- `#146 19:10 7225c8e` +248/-305 (1f) [docs] — **Implement code changes to enhance functionality and improve performance**
- `#147 21:50 8efe2e8` +77/-934 (2f) [gbp,notebooks] — **Refactor code structure for improved readability and maintainability**
- `#148 23:23 52fb263` +240/-45 (16f) [.claude,Notations.md,docs,gbp,tests] — **Refactor flow journal module: rename journal to flows, update references**
    > - Renamed the journal module to flows across the codebase for clarity.
    > - Updated all references in the simulator, validation, and loader modules to point to the new flows module.
    > - Adjusted method and parameter names in SimulationState to reflect the new terminology.
    > - Added a new flows.py module that encapsulates flow event schema, builders, and read-models.
    > - Introduced unit tests for flow-event builders to ensure correct event structure and validation.
    > - Updated invariants and scenarios tests to align with the new flows module.

## 2026-06-26 (пт) — 4 коммитов, +966/-1028

- `#149 12:24 308b3ee` +0/-0 (0f) [] — **Merge pull request #4 from vlzm/city_bike_mvp_accounting**
    > Refactor code structure for improved readability and maintainability
- `#150 12:28 69be42c` +0/-0 (0f) [] — **Merge pull request #5 from vlzm/city_bike_mvp**
    > City bike mvp
- `#151 19:15 aab6393` +966/-1028 (2f) [gbp,notebooks] — **Refactor code structure for improved readability and maintainability**
- `#152 20:17 9f3560f` +0/-0 (0f) [] — **Merge pull request #6 from vlzm/city_bike_mvp**
    > Refactor code structure for improved readability and maintainability

## 2026-06-29 (пн) — 2 коммитов, +4640/-972

- `#153 22:06 4d5f6a9` +3892/-402 (21f) [.claude,CLAUDE.md,Notations.md,docs,gbp,] — **Refactor code for improved readability and consistency**
    > - Updated formatting in `tests/invariants.py` to enhance clarity.
    > - Refactored DataFrame creation in `tests/scenarios.py` for better readability.
    > - Improved formatting in `tests/test_flows.py` for consistency and clarity.
    > - Simplified DataFrame sorting in `tests/test_scenarios.py`.
    > - Added new tests for inventory moments and redirect neighbor table functionality.
- `#154 22:58 9bdf096` +748/-570 (11f) [Notations.md,docs,gbp,notebooks,tests] — **Refactor phase_rank calculation in scenarios**
    > - Updated the _history function to compute phase_rank using the timing rule, ensuring consistency with the finalized journal.
    > - Modified the test_step_id_is_a_pure_function_of_the_journal test to use the new phase_rank calculation method, reinforcing the dependency of step_id on period_id, phase_rank, and redirect_round.

## 2026-06-30 (вт) — 3 коммитов, +2829/-1026

- `#155 14:37 61f8aa0` +638/-114 (11f) [.claude,CLAUDE.md,Notations.md,docs,gbp,] — **feat: implement step_id stamping at step opening**
    > - Introduced a run-global step counter in SimulationState to assign step_id when phases open inventory steps.
    > - Updated DockArrivals and FormDeparturesPhase to open steps and stamp step_id on events.
    > - Modified finalize_flows to trust stamped step_id for the simulator and derive it for the historical loader.
    > - Changed references from redirect_round to phase_round in relevant places to reflect the new design.
    > - Added tests to ensure distinct step_ids for separate opens and validate that stamped step_id matches the old tuple-derived values.
- `#156 18:59 aa71089` +1867/-750 (2f) [gbp,notebooks] — **Refactor code structure for improved readability and maintainability**
    > Co-authored-by: Copilot <copilot@github.com>
- `#157 21:38 cf87a49` +324/-162 (3f) [gbp,notebooks] — **Add functions for initial inventory and capacity calculations in dataloader_graph**
    > - Implemented `get_replay_initial_inventory_df` to determine the minimum initial inventory required to avoid stockouts during replay.
    > - Added `get_replay_capacities_df` to calculate the smallest dock capacities needed to prevent redirects during the simulation.
    > - Updated `ResolvedModelData` to utilize the new inventory and capacity functions, ensuring a more accurate simulation environment.
    > - Adjusted the test and check pipeline notebooks to reflect changes in inventory scaling and capacity calculations.
    > Co-authored-by: Copilot <copilot@github.com>

## 2026-07-03 (пт) — 6 коммитов, +4637/-4412

- `#158 13:49 7fb9716` +91/-2865 (5f) [gbp,notebooks,tests] — **Rename inventory_after to quantity_eop in _period_end_inventory_from_moments function and update related test to reflect this change**
- `#159 14:42 3b727ed` +235/-48 (5f) [Notations.md,gbp,notebooks] — **feat: add sizing run functionality to measure initial inventory and capacities**
- `#160 16:14 8d04f1e` +926/-447 (12f) [Notations.md,docs,gbp,notebooks,tests] — **Refactor flow journal invariants and enhance scenario tests**
    > - Updated the flow journal invariants to improve the validation of event sequences and ensure proper handling of move_id and event_id relationships.
    > - Introduced new scenarios: `overflow_delayed` and `redirect_chain` to test complex redirect behaviors in the simulation.
    > - Enhanced existing tests to validate the correct behavior of redirected flows, including handling of delayed docking and multiple redirects.
    > - Adjusted the test framework to ensure that the journal remains well-formed under new conditions introduced by the updated scenarios.
- `#161 17:05 e2aea56` +2162/-907 (20f) [Notations.md,docs,gbp,notebooks,pyprojec] — **feat: update dependencies and enhance tests**
    > - Added type hints for requests in dev dependencies.
    > - Updated scenario tests to reflect changes in run invariants from I1-I4 to I1-I5.
    > - Refactored the run function to utilize a canonical phases list, simplifying the phase management.
    > - Introduced new tests for docking mechanics, ensuring correct behavior under various conditions.
    > - Added tests for sizing functionality, verifying that the initial inventory and dock capacities meet demand without mutation of scenario data.
- `#162 20:41 41a1293` +809/-107 (8f) [.claude,AGENTS.md,CLAUDE.md,gbp,notebook] — **feat: introduce Citi Bike Simulation Platform documentation and codebase updates**
    > - Added AGENTS.md to outline the Citi Bike Simulation Platform, including commands for installation, linting, and type checking.
    > - Expanded CLAUDE.md with new answer style guidelines for chat interactions.
    > - Updated settings.local.json to include new skills for artifact design.
    > - Introduced new functionality in the test_pipeline notebook, adjusting paths and execution counts for improved accuracy.
    > - Added detailed documentation for the DockArrivals phase in both cursor_gpt_answer.md and cursor_opus_answer.md, explaining its mechanics and interactions.
    > - Created Untitled file for plan_overflow_redirect functionality.
    > - Enhanced overall codebase clarity and maintainability through structured documentation.
- `#163 21:43 249ec6f` +414/-38 (12f) [.claude,Notations.md,docs,gbp,notebooks,] — **feat: enhance trip duration and distance calculations in simulation**
    > - Updated Notations.md to clarify the definitions of travel time and distance in the context of the simulation.
    > - Introduced a new documentation file for trip distance and duration, detailing the computation methods and their implications.
    > - Refactored the mechanics of leg duration calculations to include fallback estimates based on great-circle distance when no historical data is available.
    > - Added a new function to calculate mean riding speed from historical trips, improving the accuracy of travel time estimates for redirects.
    > - Enhanced the simulation's handling of overflow redirects to ensure correct travel time estimation based on both historical data and distance calculations.
    > - Updated tests to validate the new functionality and ensure correct behavior under various scenarios.

## 2026-07-04 (сб) — 5 коммитов, +4276/-821

- `#164 18:12 51d0769` +152/-3 (7f) [.claude,Notations.md,gbp,notebooks,tests] — **feat: add riding time and cost calculations to flow events**
    > - Updated Notations.md to include definitions for `elapsed_periods`, `rate`, and `cost`, enhancing clarity on flow event metrics.
    > - Implemented `flows_with_costs` function to enrich flow event data with cumulative riding time and accrued costs based on rates.
    > - Modified `get_flows_wide` to integrate the new cost calculations, ensuring accurate representation of flow events.
    > - Enhanced test scenarios to validate the correct computation of riding time and costs across various flow events, including historical and simulated data.
    > - Updated notebooks to demonstrate the new functionality and its implications for trip costing.
- `#165 19:18 ba0b69b` +1788/-12 (15f) [CLAUDE.md,Notations.md,app,pyproject.tom] — **feat: update CLI commands and enhance documentation for UI integration**
    > - Modified CLAUDE.md to include new installation command for UI dependencies and updated linting and formatting commands to include the app directory.
    > - Added new commands for running the Streamlit UI and executing scenarios from the terminal.
    > - Expanded Notations.md with detailed descriptions of run artifacts, clarifying the structure and purpose of files generated during simulations.
    > - Updated pyproject.toml to replace the deprecated `viz` section with a new `ui` section for Streamlit and pydeck dependencies.
- `#166 19:42 73a5008` +73/-11 (8f) [.claude,Notations.md,app,notebooks] — **feat: enhance period handling and UI integration with wall-clock times**
    > - Added `t0` to `meta.json` to represent the wall-clock start of period 0, improving time representation in the UI.
    > - Updated `Notations.md` to include the new `t0` definition and its role in period calculations.
    > - Implemented `period_start_time` function to convert period IDs to wall-clock times, enhancing clarity in visualizations.
    > - Modified various UI components to display period start times instead of plain period IDs when available.
    > - Enhanced facility detail and trip map views to utilize the new time representation, improving user experience.
- `#167 21:00 edf0054` +1054/-242 (17f) [Notations.md,app,docs,gbp,notebooks,scri] — **feat: integrate OSRM routing for distance and travel time calculations**
    > - Introduced a new `Routes` class to handle distance and travel time queries between facilities, supporting both haversine and OSRM modes.
    > - Updated `Notations.md` to include detailed descriptions of the new routing capabilities and their implications for trip calculations.
    > - Refactored various components to utilize the new routing system, including the mechanics of leg duration calculations and flow event processing.
    > - Enhanced the CLI and UI to support routing mode selection, allowing users to choose between straight-line and road network distance measurements.
    > - Added documentation for setting up and using OSRM with the project, ensuring users can leverage road network data for more accurate travel time estimates.
    > - Updated tests and notebooks to validate the new routing functionality and demonstrate its impact on simulation outcomes.
- `#168 23:42 645285f` +1209/-553 (25f) [Notations.md,app,docs,gbp,notebooks,test] — **feat: enhance flow metrics and artifact generation**
    > - Added new metrics to `Notations.md`, including `duration_periods`, `measures`, `redirects`, and `losses`, improving clarity on flow event data.
    > - Refactored `build_panel` to utilize new `flows_to_*` functions for calculating departed, arrived, redirected, and lost demand metrics, reducing redundancy.
    > - Updated `build_arcs` to include endpoint coordinates for each arc, enhancing trip mapping capabilities.
    > - Modified the runner to streamline the scenario execution process, consolidating sizing and running logic into a single function.
    > - Enhanced UI components to reflect new metrics and improve user experience in visualizations and data displays.
    > - Updated tests and documentation to validate new functionalities and ensure accurate representation of flow metrics.

## 2026-07-05 (вс) — 4 коммитов, +2996/-222

- `#169 14:00 514d029` +1643/-44 (12f) [AGENTS.md,CLAUDE.md,Notations.md,gbp,pyp] — **feat: implement overnight rebalancing functionality in simulation**
    > - Introduced a new rebalancing module to manage bike movements between stations by truck during off-peak hours, enhancing inventory management.
    > - Updated `Notations.md` to include detailed descriptions of rebalancing concepts, including `rebalance`, `undocking`, and `target inventory`.
    > - Refactored the simulation phases to incorporate rebalancing logic, ensuring seamless integration with existing flow event processing.
    > - Enhanced the `SimulationState` to track the bike-level rebalance plan, facilitating execution across periods.
    > - Added comprehensive tests to validate rebalancing calculations, planning, and execution, ensuring robustness in various scenarios.
    > - Updated documentation and examples to demonstrate the new rebalancing capabilities and their impact on overall simulation outcomes.
- `#170 20:26 f504668` +450/-108 (10f) [Notations.md,app,gbp,notebooks,tests] — **feat: add truck fleet and rebalancing capabilities to simulation**
    > - Implemented functionality for overnight rebalancing, allowing trucks to move bikes between stations during off-peak hours.
    > - Updated `Notations.md` to include new parameters related to rebalancing, such as `home depot` and `truck fleet`.
    > - Enhanced the `runner` and `artifacts` modules to support truck fleet configuration and rebalancing settings.
    > - Refactored the simulation phases to accommodate the new rebalancing logic, ensuring trucks start and end at their designated depots.
    > - Added UI components for configuring truck homes and capacities, improving user interaction for rebalancing scenarios.
    > - Updated tests to validate the new rebalancing features and ensure correct behavior across various scenarios.
- `#171 21:13 8087fdf` +424/-70 (10f) [Notations.md,app,tests] — **feat: enhance simulation with truck trip visualization and download capabilities**
    > - Added a new page for visualizing truck trips, displaying bike movements during overnight rebalancing on a map.
    > - Implemented a download page for exporting raw output tables of saved runs as CSV files, improving data accessibility.
    > - Updated `Notations.md` to include descriptions of new features related to truck trips and data downloads.
    > - Enhanced the UI to support new pages and improved user interaction for analyzing truck movements and downloading data.
    > - Refactored existing components to accommodate the new functionalities and ensure seamless integration with the simulation workflow.
    > - Updated tests to validate the new truck trip visualizations and download features, ensuring robustness across scenarios.
- `#172 23:10 b6b8765` +479/-0 (1f) [docs] — **feat: add deployment plan documentation for Azure hosting**
    > - Introduced a comprehensive deployment plan for hosting the simulation core and Streamlit application on Azure, targeting up to 10 users.
    > - Documented the current architecture, including the use of Azure Container Apps and Azure Files for data management.
    > - Outlined a step-by-step approach for preparing the code for deployment, including environment variable management and Docker setup.
    > - Provided detailed descriptions of the target architecture and data storage solutions, ensuring clarity on the deployment process.
    > - Established criteria for readiness at each stage, facilitating a structured approach to deployment.

## 2026-07-06 (пн) — 15 коммитов, +11936/-4263

- `#173 17:38 3bc6048` +3155/-12 (8f) [.dockerignore,.gitignore,README.md,app,g] — **feat: add .dockerignore and update data directory handling**
    > - Introduced a .dockerignore file to exclude unnecessary files from Docker builds, optimizing image size.
    > - Updated .gitignore to clarify the handling of the .uv directory.
    > - Enhanced the README.md with installation instructions and data layout details for better user guidance.
    > - Refactored data directory handling in the artifacts and runner modules to support environment variable configuration for data paths.
    > - Adjusted test fixtures to align with the new data directory structure, ensuring consistency in test environments.
- `#174 17:45 5068758` +60/-0 (3f) [Dockerfile,README.md,docker-compose.yml] — **feat: add Docker support with docker-compose and Dockerfile**
    > - Introduced a docker-compose.yml file to facilitate the deployment of the application and OSRM routing server.
    > - Created a Dockerfile to define the application environment, including dependencies and configuration for running the Streamlit app.
    > - Updated README.md with instructions for running the application using Docker, enhancing user accessibility and setup guidance.
- `#175 18:29 6d6daee` +491/-51 (17f) ["docs,Notations.md,docs,gbp,tests] — **feat: enhance documentation and clarify code understanding levels**
    > - Added a new document outlining the levels of code understanding, addressing the cognitive debt associated with AI-assisted coding.
    > - Introduced a structured approach to understanding code, detailing five levels from user interface knowledge to in-depth module internals.
    > - Created a comprehensive plan for canonical documentation, ensuring clarity and consistency across project artifacts.
    > - Updated existing documentation to reflect new insights and decisions regarding code comprehension and documentation practices.
- `#176 18:52 710f32c` +1090/-108 (7f) [docs,tests] — **feat: add comprehensive scenario documentation and testing framework**
    > - Introduced a new document cataloging canonical scenarios for the simulator, detailing the flow events and their corresponding tables.
    > - Created a structured approach to building scenarios, including user-trip and rebalancing scenarios, with clear explanations of their construction and expected outcomes.
    > - Added a testing framework that validates the scenarios against the real engine, ensuring the accuracy of the documented tables and flow events.
    > - Enhanced existing test files to incorporate new scenario setups and validation checks, improving overall test coverage and reliability.
    > - Updated relevant documentation to reflect the new scenario structures and testing methodologies, ensuring clarity and consistency across project artifacts.
- `#177 19:07 0d4d586` +473/-0 (7f) [docs] — **feat: update documentation and restructure archive files**
    > - Added a new `simulator.md` document detailing the internal workings of the simulation, including phases, state management, and event processing.
    > - Merged content from `trip_distance_and_duration.md` into the new `simulator.md` for better organization and clarity.
    > - Created several new archive documents, including `move_id_event_id_plan.md`, `transaction_model.ru.md`, and `wide_panel.ru.md`, to outline key concepts and plans for future implementation.
    > - Established a clear status for each document, indicating whether they are approved, conceptual, or in planning stages.
    > - Updated the Russian version of the `transaction_model` to align with the new structure and provide comprehensive insights into the simulation's transactional nature.
    > - Enhanced the overall documentation framework to improve accessibility and understanding of the simulation's architecture and functionality.
- `#178 19:13 9ab0bc4` +378/-1 (3f) [docs] — **feat: add rebalancing documentation and new file for detailed processes**
    > - Introduced `rebalancing.md`, outlining the mechanics of bike movement between stations, including planning and execution phases.
    > - Updated `documentation_plan_ru.md` to reference the new rebalancing document and clarify the status of related materials.
    > - Enhanced the `simulator.md` to link to `rebalancing.md`, improving navigation and understanding of the simulation's architecture.
    > - Provided comprehensive explanations of rebalancing concepts, including demand management and planning strategies, to aid in future development and understanding.
- `#179 19:21 c720c88` +750/-180 (4f) ["docs,docs] — **feat: add Russian and English versions of code comprehension levels documentation**
    > - Created `comprehension_levels.md` and `comprehension_levels_ru.md` to outline five levels of code understanding, addressing cognitive debt in AI-assisted coding.
    > - Ensured both documents are synchronized and provide a structured approach to understanding code from user interface knowledge to in-depth module internals.
    > - Updated `documentation_plan_ru.md` to reference the new comprehension documents and clarify the status of related materials.
    > - Removed the outdated Cyrillic version of the comprehension levels document to streamline documentation.
- `#180 19:29 faec52a` +254/-0 (4f) [README.md,docs] — **feat: enhance documentation with new English and Russian README files**
    > - Added `README.md` and `README_ru.md` as entry points for project documentation, outlining project details, setup instructions, and repository structure.
    > - Updated `documentation_plan_ru.md` to reference the new README files, ensuring clarity on documentation navigation.
    > - Improved accessibility for both English and Russian-speaking users by providing comprehensive guides on how to run the simulator and understand the project structure.
- `#181 21:01 d708154` +662/-282 (2f) [docs] — **feat: restructure documentation by removing outdated Russian plan and adding detailed simulator guide**
    > - Deleted `documentation_plan_ru.md` to streamline documentation and eliminate historical context.
    > - Introduced `simulator_clear.md`, providing a comprehensive step-by-step explanation of the simulation process, including code structure, phases, and state management.
    > - Enhanced clarity and accessibility of simulator documentation for users, ensuring a better understanding of the simulation's architecture and functionality.
- `#182 21:26 de1cbea` +420/-0 (2f) [.claude,docs] — **feat: add documentation-style skill for project clarity and structure**
    > - Introduced a new `SKILL.md` file detailing guidelines for writing, rewriting, and reviewing project documentation in Markdown format.
    > - Established clear steps for documentation creation, language usage, vocabulary consistency, and structural organization to enhance clarity and accessibility.
    > - Aimed to improve the overall quality of project documentation, ensuring that new readers can easily understand the content and its context.
- `#183 21:31 3610e1d` +946/-1779 (5f) [docs] — **feat: consolidate and enhance rebalancing documentation**
    > - Deleted `rebalancing_clear.md` and integrated its content into `rebalancing.md` for a more streamlined and comprehensive guide on bike movement logistics.
    > - Updated `rebalancing.md` to include detailed explanations of the rebalancing process, phases, and data handling, improving clarity and accessibility for users.
    > - Enhanced the overall structure of the documentation to ensure a cohesive understanding of the rebalancing mechanics within the simulation framework.
- `#184 22:02 6e0764c` +1031/-18 (8f) [.claude,docs] — **feat: expand documentation with new app and dataloader guides**
    > - Added `app.md` to provide a detailed overview of the application structure, including the flow from execution to the web interface.
    > - Introduced `dataloader.md` to explain the data loading process, detailing how raw CSV data is transformed into `ResolvedModelData`.
    > - Enhanced the `flow_journal.md` to clarify the role of the flow journal in the simulation, including event definitions and processing.
    > - Updated `README.md` and `README_ru.md` to reference the new documents, improving navigation and understanding of the project structure.
    > - Aimed to improve overall documentation clarity and accessibility for users, facilitating better comprehension of the system's architecture and functionality.
- `#185 22:07 40675e6` +45/-38 (2f) [docs] — **fix: clarify documentation on saved runs and flow journal processing**
    > - Updated `app.md` to specify that saved-run pages read only saved files and clarified the role of `DATA_DIR` in data management.
    > - Revised descriptions in `flow_journal.md` to enhance understanding of event processing, including the finalization of journal entries and the handling of order columns.
    > - Improved consistency in terminology and explanations throughout the documentation to aid user comprehension of the system's functionality.
- `#186 22:20 5a39202` +398/-0 (1f) [docs] — **feat: add deepening candidates documentation for architecture review**
    > - Introduced `deepening_candidates.md`, outlining refactoring candidates identified during the architecture review on 2026-07-06.
    > - Document details the transition from shallow to deep modules, emphasizing testability and easier navigation.
    > - Includes a status legend for tracking candidate progress and a vocabulary section to clarify terms used in the review.
    > - Provides a comprehensive overview of candidates, their layers, and proposed solutions to enhance module behavior and maintainability.
- `#187 23:22 b0423b3` +1783/-1794 (30f) [Notations.md,app,docs,gbp,notebooks,test] — **feat: enhance documentation and refactor data loading functions**
    > - Updated `Notations.md` to clarify the role of `SimulationState.apply_step_events` and the order of phases in the simulation process.
    > - Revised `artifacts.py` to streamline the loading of various tables, replacing direct calls with typed accessors for better maintainability.
    > - Improved `ui_shared.py` by adding new functions for loading specific tables and rebalancing settings, enhancing code clarity and usability.
    > - Refactored view files to utilize the new loading functions, ensuring consistency across the application.
    > - Enhanced documentation in `app.md` to reflect changes in data loading processes and clarify the relationship between the model layer and the UI.

## 2026-07-07 (вт) — 7 коммитов, +3283/-564

- `#188 10:26 f025fd2` +900/-119 (19f) [Notations.md,app,docs,gbp,pyproject.toml] — **Refactor data handling and validation with pandera and pydantic**
    > - Updated home.py to access RunMeta attributes directly instead of using string keys.
    > - Introduced a new data contracts plan document outlining the implementation of data contracts using pandera and pydantic.
    > - Enhanced EnvironmentConfig to validate configuration parameters during initialization.
    > - Updated Phase class to use Literal types for routing mode and dock arrival settings.
    > - Implemented pandera schemas for various data tables in dataloader_graph.py and dataloader_raw.py, ensuring data integrity at load time.
    > - Added checks for journal schema violations in validation.py and during historical flow retrieval.
    > - Created a new journal_schema.py to define the flow journal schema and associated validation logic.
    > - Updated tests to reflect changes in data access patterns and added new tests for rebalancing settings.
- `#189 11:41 15c38d3` +258/-47 (9f) [Notations.md,app,docs,gbp,tests] — **feat: enhance run metadata with inputs and code version tracking; update documentation and tests**
- `#190 13:02 88cd6ef` +902/-36 (11f) [CLAUDE.md,Notations.md,app,docs,pyprojec] — **feat: Implement run-artifact API with FastAPI**
    > - Added `app/api.py` to serve run artifacts over HTTP, allowing for starting runs and retrieving run statuses.
    > - Introduced `app/api_client.py` for HTTP client functions to interact with the API.
    > - Updated `app/ui_shared.py` to support fetching runs and tables from the API when `API_URL` is set.
    > - Modified `app/views/run_scenario.py` to handle runs via the API, including polling for status updates.
    > - Enhanced `app/views/downloads.py` and `app/views/home.py` to integrate with the new API for listing runs and downloading tables.
    > - Created `docs/api.md` to document the API design and endpoints.
    > - Added tests for the API in `tests/test_app_api.py` to ensure functionality and correctness.
    > - Updated `pyproject.toml` to include FastAPI and Uvicorn dependencies.
- `#191 13:38 0e9b0af` +246/-3 (3f) [docs] — **feat: Add architecture documentation with system diagrams and module depth table**
- `#192 15:06 9c44d54` +371/-151 (12f) [Notations.md,README.md,docs,gbp] — **docs: fix every mismatch and coverage gap from the 2026-07-07 audit**
    > Facts (audit 1.1-1.5): remove the two fabricated runtime checks from
    > rebalancing.md and simulator.md, re-quote _event_deltas and describe its
    > two delegators in flow_journal.md, correct the loader row of Who Calls
    > What, scope the marginals key claim, fix app.md's loader-caller and
    > cache-key claims to the two-backend reality, and align dataloader.md
    > with the code (get_depots, build order, journal schema check, the
    > closing check_engine_tables call). Fix the stale I1-I4 comment in
    > flows.py.
    > API story (audit 2.1, 1.6, 2.9): rewrite api.md from a design plan to a
    > description of the running service, document app/api_client.py, add the
    > missing endpoint details (404 rules, run_name pattern, worker-time
    > dataset build), add api.md and the three app files to both READMEs, fix
    > the install commands to include the api extra, and add the two-backends
    > note to app.md.
    > Coverage gaps (audit 2.2-2.8): full runner flag list in app.md, the
    > sizing_scale_factor mechanism and the construction guards in
    > simulator.md, the run-time journal schema check framing, the pandera
    > schema layer in dataloader.md, the missing journal functions in the
    > flow_journal.md code map, and the rebalancing config guards and full
    > plan column list.
    > Archive deepening_candidates.md (audit 1.7): an executed plan that
    > quotes deleted code.
    > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
- `#193 15:11 1a039bf` +465/-102 (29f) [.claude,CLAUDE.md,Notations.md,README.md] — **docs: restructure docs/ into explanation, guides, and method folders**
    > Move the code explainers (architecture, dataloader, simulator,
    > rebalancing, flow_journal, app, api, scenarios) to docs/explanation/,
    > the how-to guide (osrm_setup) to docs/guides/, and the way-of-working
    > documents (comprehension_levels, working-method, with their Russian
    > companions) to docs/method/. The two READMEs stay at the top as the
    > entry point.
    > Update every path that referenced the moved files: the DOC_PATH
    > constant and docstrings of tests/test_docs_scenarios.py, the
    > docs/explanation/api.md references in app/api.py, app/api_client.py,
    > app/ui_shared.py, app/views/run_scenario.py, tests/test_app_api.py and
    > CLAUDE.md, the osrm_setup path in gbp/routing.py, both root README
    > mentions, Notations.md, the canonical notebook comment, the
    > documentation-style skill, and every relative link inside docs/
    > (Notations.md links from the subfolders become ../../Notations.md).
    > Archive the completed docs audit (all 31 items done, pytest green:
    > 221 passed).
    > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
- `#194 16:25 435cf8e` +141/-106 (2f) [docs] — **docs: enhance architecture and dataloader documentation for clarity and accuracy**
    > - Updated architecture.md to refine descriptions of system diagrams and module interactions, ensuring clarity in the representation of data flow and responsibilities.
    > - Revised dataloader.md to improve explanations of the data loading process, including the handling of raw trip CSVs and the construction of initial inventory and capacity tables.
    > - Enhanced terminology and consistency throughout both documents to better align with the code and improve user understanding of the system's functionality.

## 2026-07-10 (пт) — 7 коммитов, +5708/-167

- `#195 11:34 3885268` +348/-1 (2f) [CLAUDE.md,docs] — **feat: Add implementation plan for ML demand forecasting phase**
- `#196 11:37 8dc8245` +127/-0 (1f) [docs] — **feat: Add ML cloud phase decision document outlining Azure service choices and project scope**
- `#197 11:43 5424b9e` +25/-4 (2f) [CLAUDE.md,Notations.md] — **docs: update CLAUDE.md and Notations.md for clarity on demand forecasting phase and canonical scenario**
- `#198 13:13 2c588f8` +1017/-9 (10f) [Notations.md,app,gbp,notebooks,tests] — **feat: Implement forecast demand functionality**
    > - Added a new module for building and saving forecast demand tables, including seasonal naive forecasting.
    > - Introduced functions for generating forecast periods, mapping OD matrices by hour of week, and applying forecast demand to resolved model data.
    > - Enhanced the run scenario view to support selecting between historical demand and forecast demand.
    > - Created a Jupyter notebook to demonstrate the forecast pipeline, including loading trips, generating forecasts, and running simulations.
    > - Developed comprehensive tests for the forecast functionality, ensuring accuracy in demand calculations and artifact handling.
- `#199 13:59 06ba3e7` +1044/-21 (17f) [.dvc,.dvcignore,.gitignore,CLAUDE.md,Not] — **feat: Implement data downloading and schema harmonization for Citi Bike trips**
    > - Added `gbp/ml/data.py` to handle downloading monthly Citi Bike trip files, extracting CSVs, and harmonizing schemas from old and new formats.
    > - Introduced `gbp/ml/training.py` to build the training table from raw trip data, ensuring zero rows are kept for stations with no departures.
    > - Updated `pyproject.toml` to include DVC as a dependency for machine learning workflows.
    > - Enhanced tests in `tests/test_dataloader_raw.py` to verify cleaning logic for out-of-area and reversed trip times.
    > - Created new tests in `tests/test_ml_data.py` for data downloading, schema mapping, and CSV extraction logic.
    > - Added tests in `tests/test_ml_training.py` to validate the training table builder and ensure idempotency of parquet partitions.
- `#200 15:16 cdc8e29` +1264/-38 (13f) [Notations.md,data,docs,gbp,pyproject.tom] — **feat: Implement stockout share computation and integrate with training table**
    > - Add station status module to compute stockout shares based on archived station data.
    > - Update training module to include stockout share in the training table schema and during partition building.
    > - Enhance data loading functions to download and process station status dumps.
    > - Introduce weather data handling in training table construction.
    > - Create comprehensive tests for feature building, stockout share calculations, and integration with training data.
    > - Ensure training table and forecast input maintain identical feature values for the same station-day.
- `#201 20:46 962bfdd` +1883/-94 (16f) [CLAUDE.md,Notations.md,gbp,pyproject.tom] — **Implement SARIMAX and Seasonal Naive models for demand forecasting**
    > - Added `SarimaxTotalModel` class for SARIMAX-based forecasting on city-level total series, including methods for fitting, predicting, saving, and loading the model.
    > - Introduced `SeasonalNaiveModel` class to implement a seasonal naive forecasting approach, utilizing precomputed hour-of-week means.
    > - Enhanced training data loading with `load_training_table` function to optimize memory usage by converting float columns to `float32` and categorizing ID columns.
    > - Updated `pyproject.toml` to include new dependencies for LightGBM, Statsmodels, MLflow, and PyTorch.
    > - Expanded test suite in `test_forecast.py` to cover new model implementations and ensure correct functionality.
    > - Created `test_ml_models.py` to comprehensively test model interfaces, fitting, prediction, and metrics across different model families.

## 2026-07-11 (сб) — 17 коммитов, +5505/-1310

- `#202 12:30 d484395` +685/-8 (8f) [CLAUDE.md,Notations.md,app,docs,gbp,test] — **feat: Implement two-level evaluation for demand models and add evaluation script**
- `#203 13:15 b3fba89` +167/-158 (6f) [Notations.md,app,docs,gbp,tests] — **feat: Enhance two-level evaluation by implementing replay-state forecast runs and updating evaluation scripts**
- `#204 17:06 94cc6a9` +1290/-28 (12f) [CLAUDE.md,Notations.md,data,docs,gbp,tes] — **Implement retraining pipeline and model registry**
    > - Added a new pipeline module (`gbp/ml/pipeline.py`) to manage the retraining process, including steps for downloading data, building training tables, training models, backtesting, and promoting models based on performance.
    > - Introduced a model registry module (`gbp/ml/registry.py`) to handle model versioning, champion promotion, and retrieval of models from the registry.
    > - Enhanced the model loading functionality in `gbp/ml/models/__init__.py` with a new `load_model` function to retrieve trained models from the registry.
    > - Created comprehensive tests for the model registry and pipeline functionalities in `tests/test_ml_registry.py`, ensuring end-to-end validation of the retraining process.
- `#205 17:47 7683e89` +931/-2 (8f) [CLAUDE.md,Notations.md,app,docs,gbp,pypr] — **feat: add model monitoring page with metrics and drift reports**
    > - Introduced a new page for model monitoring in the Streamlit app.
    > - Implemented functionality to load and display model metrics history, including MAE, Poisson deviance, and bias.
    > - Added visualizations for metrics over time, highlighting degraded months based on rolling MAE.
    > - Created a drift report feature that compares current feature distributions against historical training data using Evidently.
    > - Updated documentation to reflect the new monitoring capabilities and usage instructions.
    > - Added tests to ensure the correctness of the monitoring functionality, including scoring forecasts and generating drift reports.
- `#206 18:00 a4098c7` +200/-1 (4f) [.github,docs,pyproject.toml,tests] — **feat: add CI workflow and smoke tests for training and retraining pipeline**
- `#207 18:03 7ff0ba6` +2/-1 (1f) [pyproject.toml] — **fix: update plotly dependency to be compatible with evidently requirements**
- `#208 18:11 7c37c06` +1/-1 (1f) [pyproject.toml] — **fix: update mypy python version to 3.13**
- `#209 19:08 8312019` +486/-0 (1f) [docs] — **feat: add architecture review document outlining codebase improvements**
- `#210 19:28 60bfea3` +62/-41 (4f) [Notations.md,gbp] — **refactor: streamline inventory handling in phases and state management**
- `#211 19:48 c11ff99` +159/-172 (10f) [Notations.md,docs,gbp,tests] — **refactor: replace phase_rank_by_timing with stamp_history_ordering in historical loader**
- `#212 20:13 17c14b7` +160/-64 (14f) [Notations.md,docs,gbp,tests] — **feat: introduce ScenarioInputs contract for simulator input tables**
- `#213 20:34 7979feb` +154/-98 (7f) [Notations.md,app,gbp] — **feat: enhance forecasting functionality with month period grid integration**
- `#214 22:44 8ea98f6` +157/-97 (8f) [Notations.md,gbp,tests] — **feat: implement shared naive month forecast and integrate into backtest and monitoring**
- `#215 23:00 b3b48bf` +344/-325 (10f) [Notations.md,gbp,tests] — **Refactor MLflow integration with MlflowStore class**
    > - Introduced MlflowStore class to encapsulate MLflow tracking and registry operations.
    > - Updated backtest, pipeline, and monitoring modules to utilize the new MlflowStore class.
    > - Removed direct MLflow calls and replaced them with methods from MlflowStore.
    > - Simplified the process of configuring MLflow tracking and managing experiments.
    > - Enhanced readability and maintainability of the code by centralizing MLflow logic.
    > - Updated tests to reflect changes in the MLflow integration, ensuring compatibility with the new structure.
- `#216 23:15 7b4385a` +358/-179 (10f) [app,docs,tests] — **feat: implement backend selection for local and API runs, refactor related components**
- `#217 23:34 756a6ec` +183/-88 (7f) [Notations.md,app,docs,tests] — **feat: implement save_scenario_run function to streamline run artifact saving and enhance meta.json with sized state**
- `#218 23:49 525fd63` +166/-47 (7f) [app,tests] — **feat: refactor artifact path handling and enhance metric definitions for clarity**

## 2026-07-12 (вс) — 4 коммитов, +1027/-936

- `#219 00:08 c9383e2` +231/-223 (12f) [Notations.md,docs,gbp] — **feat: enhance documentation and refactor inventory handling for clarity and consistency**
- `#220 01:04 99ff808` +242/-37 (6f) [docs] — **feat: enhance API and documentation to support forecast demand runs and improve clarity on run parameters**
- `#221 22:49 f907f5f` +58/-650 (5f) [app,other_staff,tests] — **feat: add tests for run_scenario wiring to ensure forecast data is preserved during rebalancing**
- `#222 23:24 72ecbe1` +496/-26 (11f) [app,gbp,notebooks,pyproject.toml] — **Add structlog dependency to pyproject.toml**

## 2026-07-13 (пн) — 3 коммитов, +3547/-9385

- `#223 14:22 a9cdbf0` +863/-77 (10f) [Notations.md,app,docs,gbp,tests] — **Refactor ML monitoring and forecasting logic**
    > - Updated drift report to compare only weather and demand-history columns, excluding calendar columns to avoid false positives.
    > - Introduced DRIFT_COLUMNS and DRIFT_NUM_THRESHOLD constants for clarity and configurability.
    > - Adjusted the drift report function to utilize new constants and improved distance measurement logic.
    > - Enhanced test coverage for forecasting scenarios, ensuring demand cuts are recorded correctly and dropped shares are handled.
    > - Added comprehensive documentation for the ML components, detailing the data flow, model families, and evaluation processes.
- `#224 20:14 09b83a1` +1803/-8309 (53f) [AGENTS.md,CLAUDE.md,PROJECT.md,README.md] — **Add documentation for new features and improve existing content**
    > - Created new how-to guides for adding tables to run artifacts, changing demand, debugging invariant violations, and running on forecasts.
    > - Developed a comprehensive plan for reworking documentation to enhance clarity and usability.
    > - Updated simulator configuration to enforce stricter validation rules for run parameters.
    > - Enhanced phase documentation to clarify the execution order and conditions.
    > - Improved comments and docstrings across various modules for better understanding of functionality.
    > - Added checks for malformed data in the rebalancing phase and clarified the handling of empty plans.
    > - Refined the demand forecasting module's documentation to outline the process and structure clearly.
- `#225 22:46 5a57f51` +881/-999 (24f) [Notations.md,app,docs,gbp,tests] — **Refactor data loading and month handling**
    > - Moved month_bounds and normalize_month functions from gbp.ml.data to gbp.loaders.download for better organization.
    > - Updated imports across multiple modules to reflect the new location of month_bounds and normalize_month.
    > - Removed deprecated functions and comments from gbp.ml.data, focusing on forecasting-specific helpers.
    > - Added comprehensive tests for the new download module, ensuring schema harmonization and month handling logic are intact.
    > - Adjusted existing tests to accommodate the refactor, ensuring all functionalities are covered.

## 2026-07-15 (ср) — 18 коммитов, +4131/-1866

- `#226 14:53 17f7b55` +353/-100 (6f) [.claude,CLAUDE.md,Notations.md,README.md] — **refactor: update project description to clarify framework purpose and scenario**
- `#227 15:06 f1da0c9` +129/-14 (2f) [README.md,docs] — **refactor: enhance README with detailed project description and quick start guide**
- `#228 15:42 aa6a825` +105/-105 (31f) [CLAUDE.md,Notations.md,README.md,app,doc] — **Add detailed documentation for scenarios and API**
    > - Introduced a comprehensive guide for flow journal examples in `worked-examples.md`, covering various scenarios including stockouts, trips, redirects, and rebalancing.
    > - Added an API reference in `api.md`, detailing the HTTP service for accessing run artifacts, including endpoints, usage, and access control.
- `#229 15:55 a02595f` +179/-169 (2f) [docs] — **refactor: update README and README_ru for clarity and structure**
- `#230 16:03 daafed5` +169/-31 (5f) [docs] — **refactor: enhance documentation with installation and UI usage guides**
- `#231 16:16 a4dbcf8` +294/-0 (3f) [docs] — **docs: add detailed scenario description for Citi Bike in README and README_ru**
- `#232 16:28 859b063` +157/-140 (10f) [docs] — **refactor: update documentation for clarity and structure; remove concepts.md and add overview.md**
- `#233 16:37 641ae66` +3/-201 (3f) [README.md,docs] — **docs: update README with environment activation instructions and remove outdated documentation**
- `#234 17:04 0b64c78` +320/-116 (4f) [README.md,docs,tests] — **docs: update quickstart and installation documentation for clarity; add automated tests for quickstart script**
- `#235 21:38 a6ec5a7` +716/-271 (16f) [Notations.md,app,docs,gbp,tests] — **Refactor run scenario handling and introduce RunRequest**
    > - Replaced the dictionary-based request structure in `run_scenario` with a typed `RunRequest` dataclass for better clarity and type safety.
    > - Updated the API documentation to reflect the new `RunRequest` structure for the `POST /runs` endpoint.
    > - Enhanced the `apply_saved_forecast` function to streamline the process of applying saved forecasts to resolved model data.
    > - Improved test coverage for the new `RunRequest` structure, ensuring that all entry points and scenarios are properly validated and tested.
    > - Consolidated forecast handling logic to reduce redundancy and improve maintainability across the codebase.
- `#236 22:01 ac1cdee` +409/-90 (3f) [app,tests] — **feat: add evaluation comparison logic and corresponding tests**
- `#237 22:34 b9c6439` +552/-183 (10f) [CLAUDE.md,gbp,tests] — **Enhance simulation engine with improved validation and modularization**
    > - Updated CLAUDE.md with new coding principles emphasizing deep modules, minimalism, and strict typing.
    > - Modified EnvironmentConfig to clarify validation behavior and added documentation for invariants.
    > - Refactored the Environment class to store run violations without raising errors, allowing for more flexible error handling.
    > - Split the DockArrivals phase logic into a separate function for better testability and clarity.
    > - Introduced a new plan_rebalance function to encapsulate the rebalancing logic, improving modularity.
    > - Added apply_rebalance function to manage the execution of rebalancing events in a structured manner.
    > - Created direct tests for phase wiring to ensure the correct ordering of operations in the simulation.
    > - Enhanced validation logic to ensure demand scaling and period limits are respected, with corresponding tests to verify behavior.
- `#238 22:49 240eedc` +106/-26 (3f) [gbp] — **feat: implement canonical phase order and refactor phase handling in simulator**
- `#239 22:58 147c67d` +87/-40 (1f) [gbp] — **feat: implement _finish_forecast function to streamline forecast artifact generation**
- `#240 23:05 4a45bc6` +115/-90 (5f) [gbp,tests] — **feat: refactor backtest comparison logging and retrieval methods**
- `#241 23:14 8a3f643` +102/-47 (6f) [Notations.md,app,gbp] — **feat: introduce PeriodGrid class for consistent period management across modules**
- `#242 23:33 99ba6d0` +171/-118 (14f) [gbp,tests] — **feat: implement scaled demand inputs for consistent demand scaling across simulation phases**
- `#243 23:50 72eb471` +164/-125 (9f) [gbp,tests] — **feat: introduce MlPaths class for centralized data path management across modules**

## 2026-07-16 (чт) — 14 коммитов, +2870/-6943

- `#244 14:06 fd04ecc` +410/-5065 (52f) [.claude,app,gbp] — **Refactor documentation in journal_schema.py and routing.py**
    > - Simplified and condensed docstrings in journal_schema.py to focus on essential information about the Pandera schema and its validation functions.
    > - Updated routing.py docstrings to clarify the purpose of the Routes class and its methods, while removing redundant explanations about routing modes and parameters.
- `#245 14:26 c304b23` +46/-35 (6f) [.claude] — **feat: update documentation references and improve clarity in various skills**
- `#246 15:09 530a11a` +82/-16 (3f) [app,gbp,tests] — **refactor: move the two evaluation metrics into gbp/ml/metrics.py**
    > Move panel_departed_mae and the busy-share aggregation (now the named
    > lost_demand_busy_share) out of app/eval_comparison.py into gbp/ml/metrics.py,
    > next to forecast_metrics and busy_facility_ids they already lean on. Every
    > evaluation metric is now defined in one place; the comparison code calls them
    > and no metric math stays inlined in app/. Adds direct unit tests.
    > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
- `#247 15:12 6ffe3fa` +119/-89 (5f) [app,gbp,tests] — **refactor: move the run recipe into gbp/consumers/run.py**
    > Move RunRequest and the fleet/depot defaults (DEFAULT_TRUCK_HOMES,
    > DEFAULT_N_DEPOTS, DEPOT_IDS, DEFAULT_TRUCK_CAPACITY_BIKES, DEFAULT_TRUCK_RATE,
    > DEFAULT_NUMBER_OF_PERIODS, DEMAND_SOURCES) out of app/runner.py into the new
    > gbp/consumers/run.py, as one typed source. app/runner.py re-exports them and
    > keeps the orchestration and CLI for now; app/artifacts.py imports RunRequest
    > from gbp.consumers.run instead of app/runner. The recipe tests move to
    > tests/test_consumers_run.py, assertions unchanged.
    > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
- `#248 15:15 9b396e8` +767/-15 (24f) [.scratch,app,docs,gbp,tests] — **refactor: move the run-artifact builder into gbp/artifacts.py**
    > Move app/artifacts.py whole (table builders, contracts, save/load, and the
    > data/runs/ path policy) into the new gbp/artifacts.py. The __file__-based
    > data dir and code_version resolve to the same repo root from gbp/, so the
    > saved artifact is byte-for-byte identical. save_scenario_run imports
    > RunRequest from gbp.consumers.run, so no gbp module imports app. Every app
    > caller (runner, backend, api, api_client, ui_shared, views, evaluate) now
    > reads the builder as 'from gbp import artifacts'; DiskBackend keeps its
    > pass-through delegate methods. The round-trip tests import from gbp.artifacts,
    > assertions unchanged.
    > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
- `#249 15:20 650ad30` +272/-287 (9f) [app,gbp,tests] — **refactor: move the run orchestration into gbp/consumers/run.py**
    > Move build_graph_data, run_scenario, run_and_save, and DEFAULT_TRIPS_PATH out
    > of app/runner.py into gbp/consumers/run.py, alongside the recipe. app/runner.py
    > shrinks to the argparse main() that builds a RunRequest and calls the gbp run
    > path, so 'python app/runner.py' is unchanged. app/api.py, app/backend.py, and
    > the Run scenario page start runs through gbp.consumers.run; nothing in gbp
    > imports app. The run_scenario wiring tests move to tests/test_consumers_run.py
    > (patching gbp.consumers.run); test_app_runner.py is deleted. build_graph_data's
    > routing_mode is typed RoutingMode, an imprecision app/ never type-checked.
    > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
- `#250 15:22 7729fa5` +234/-262 (4f) [app,gbp,tests] — **refactor: move the evaluation cluster into gbp/ml/evaluation.py**
    > Combine app/eval_comparison.py (EvalNames, ModelForecast, run_row,
    > build_comparison) and app/evaluate.py (evaluate_month, ensure_forecast,
    > _ensure_run, actual_demand_table, evaluation_dir, CLI) into the new
    > gbp/ml/evaluation.py. It calls run_and_save and the artifacts API, both now in
    > gbp, so the module imports no app. The terminal entry point becomes
    > 'python -m gbp.ml.evaluation --month ...', matching the other gbp.ml commands.
    > app/eval_comparison.py and app/evaluate.py are deleted; the evaluation tests
    > move to tests/test_ml_evaluation.py, assertions unchanged.
    > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
- `#251 15:26 80281c1` +12/-12 (5f) [CLAUDE.md,Notations.md,docs] — **docs: point the canonical text at the new gbp homes**
    > Notations §12 names gbp/artifacts.py everywhere it named app/artifacts.py; §16
    > names run_scenario (gbp/consumers/run.py); the two-level evaluation entry and
    > the CLAUDE.md commands block use 'python -m gbp.ml.evaluation'; the CLAUDE.md UI
    > rule names gbp/artifacts.py. The rules are unchanged -- only the paths and one
    > command. Also swaps the now-deleted 'python app/evaluate.py' command in the
    > live user docs (scenarios, key-components) for the new module invocation.
    > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
- `#252 15:34 1c0b25c` +32/-29 (7f) [docs] — **docs: update the architecture docs for the app -> gbp move**
    > The code-review flagged that the key-component and reference docs still named
    > the moved code as app-side. Point them at the real homes: the overview module
    > table, both architecture diagrams, and its prose; the visualization code map;
    > data-model, flow-journal, ml-toolkit, api.md, and the how-to for adding an
    > artifact table. runner.py is now described as the thin CLI over the gbp run
    > path; the run path, the artifact builder, and the evaluation live in gbp/.
    > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
- `#253 23:21 cba81c5` +397/-494 (6f) [.claude,CLAUDE.md,explanation-diataxis-r] — **feat: add write-explanation skill and explanation-diataxis reference documents**
- `#254 23:24 2ee646f` +2/-2 (1f) [CLAUDE.md] — **docs: clarify language guidelines and exceptions in CLAUDE.md**
- `#255 23:25 748715e` +0/-118 (1f) [explanation-diataxis-ru.md] — **chore: remove Russian explanation-diataxis document**
- `#256 23:44 4064b09` +344/-519 (5f) [docs] — **Add overview documentation and enhance data model explanations**
    > - Introduced a new overview document detailing the framework's architecture and components using mermaid diagrams.
    > - Expanded the data model documentation to clarify the relationships between raw and resolved data, emphasizing the design decisions and their implications.
    > - Removed the outdated overview document from the key components section to streamline the documentation.
- `#257 23:52 f95deba` +153/-0 (1f) [docs] — **feat: add comprehensive overview documentation for the framework**

## 2026-07-17 (пт) — 5 коммитов, +2827/-2414

- `#258 14:54 52c2218` +204/-2401 (5f) [docs,notebooks] — **chore: keep one overview, remove stray notebooks**
    > Pick one of the seven overview drafts as docs/key-components/overview.md
    > and delete the rest. Remove notebooks that serve neither canonical run
    > (check_pipeline, build_wild_inventory, flat_env) and the stray Untitled
    > file. docs/archive/overview.md stays archived.
    > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
- `#259 14:55 73cf78d` +209/-0 (1f) [docs] — **docs: add interview portfolio plan**
    > Five phases: cleanup, research notebook, presentation scripts, minimal
    > Azure deploy, final pass. The positioning rule: platform idea first,
    > then "one domain implemented so far — Citi Bike", then the story is
    > told through that domain only.
    > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
- `#260 14:55 fe71e5b` +66/-11 (1f) [README.md] — **docs: rework README for a first-time reader**
    > Opening paragraph follows the positioning rule (platform, one
    > implemented domain, expansion later). Add the central idea (forecasts
    > judged by operational cost through the simulator), an architecture
    > diagram, the three user roles with their entry points, and the ml
    > install extra.
    > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
- `#261 15:18 688ef36` +1522/-2 (2f) [README.md,notebooks] — **Refactor code structure for improved readability and maintainability**
- `#262 15:52 52c099a` +826/-0 (2f) [docs] — **Добавлен сценарий 45-минутной презентации на русском языке для демонстрации платформы по задачам на графах потоков, включая описание проблемы, цифрового двойника, ребалансировки, прогнозирования спроса и операционного цикла.**

> **пауза 7 дней**

## 2026-07-24 (пт) — 7 коммитов, +15849/-654

- `#263 11:14 43af81f` +12962/-1 (296f) [.claude,.github,docs] — **Add theme.toml for Hugo Book theme configuration**
    > - Created a new theme.toml file for the Hugo Book theme.
    > - Included metadata such as name, license, description, homepage, demo site, tags, and author information.
    > - Set minimum Hugo version requirement to 0.158.0.
- `#264 11:25 4ee8dba` +5/-4 (1f) [.github] — **Update workflow to trigger on city_bike_mvp_accounting branch for site builds**
- `#265 11:52 2467009` +2740/-76 (42f) [.gitignore,Notations.md,docs,scripts] — **Add export script for Jupyter notebooks and update Hugo configuration**
    > - Introduced `export_notebooks.py` to convert Jupyter notebooks into Hugo pages, creating a structured output in the scenario section.
    > - Added new image file `index_9_1.png` for documentation.
    > - Updated `hugo.toml` to include portable links and mount Notations.md as a reference glossary page.
- `#266 12:05 1c1acc6` +87/-540 (8f) [README.md,docs] — **Refactor README and remove deprecated documentation files for clarity and organization**
- `#267 12:06 e8642c0` +10/-11 (1f) [docs] — **Update map.md to clarify out-of-scope items and remove unnecessary notes**
- `#268 12:18 039940a` +31/-15 (8f) [CLAUDE.md,Notations.md,app,tests] — **Update documentation paths to reflect new directory structure**
- `#269 12:22 0ad71c3` +14/-7 (5f) [app,tests] — **Refactor API and test documentation for clarity and organization**

> **пауза 6 дней**

## 2026-07-30 (чт) — 9 коммитов, +5182/-7601

- `#270 15:07 912fea2` +86/-4724 (33f) [.claude,CLAUDE.md,docs] — **Delete obsolete tickets and reports related to the Wayfinder Hugo site project, including decisions on non-site folders, GitHub Pages deployment research, and the deploy pipeline setup. Remove architecture review and model evaluation reports from January 2026, as they are no longer relevant to the current documentation structure.**
- `#271 15:30 c0919ae` +566/-513 (2f) [.claude,Notations.md] — **Refactor code structure for improved readability and maintainability**
- `#272 18:49 65dc9ed` +1/-0 (1f) [.gitignore] — **Add data/osrm/ to .gitignore to exclude OSRM data files from version control**
- `#273 18:56 0404885` +2408/-471 (1f) [notebooks] — **Implement code changes to enhance functionality and improve performance**
- `#274 19:41 d89fe86` +722/-764 (51f) [app,docs,domains,gbp,notebooks,pyproject] — **Refactor package structure and update import paths**
    > - Updated the `pyproject.toml` to include separate top-level packages for `gbp` and `domains`.
    > - Changed import paths in test files to reflect the new package structure, specifically moving from `gbp.loaders` to `domains.citybike.loaders` and updating references to `gbp.model`.
    > - Adjusted imports in various test files including `test_consumers_run.py`, `test_dataloader_raw.py`, `test_download.py`, `test_forecast.py`, `test_ml_features.py`, `test_ml_models.py`, `test_ml_monitoring.py`, `test_ml_smoke.py`, `test_ml_training.py`, and `test_rebalancing.py`.
- `#275 19:49 d3cc63e` +253/-6 (3f) [.github,CLAUDE.md,docs] — **Update CI workflow and documentation for domain restructuring**
- `#276 20:10 e2472e2` +777/-703 (46f) [CLAUDE.md,app,docs,domains,gbp,tests] — **Refactor forecasting module and improve model architecture**
    > - Moved the DemandModel interface to a new module (gbp/ml/model.py) for better organization.
    > - Removed the old model families initialization and replaced it with a dynamic registration system.
    > - Updated the metrics module to import DEMAND_KEYS from the new model module.
    > - Deleted outdated model family implementations from gbp/ml/models/base.py and gbp/ml/models/__init__.py.
    > - Refactored tests to accommodate the new structure, ensuring all tests pass with the updated model registration.
    > - Cleaned up unused functions and imports in various test files, enhancing code clarity and maintainability.
    > - Adjusted import paths in tests to reflect the new module structure under domains/citybike/ml.
- `#277 20:40 589c434` +369/-174 (19f) [CLAUDE.md,app,docs,domains,gbp,tests] — **Refactor Citi Bike integration into the run path**
    > - Moved Citi Bike specific logic from `gbp.consumers.run` to `domains.citybike.run`.
    > - Updated `RunRequest` to include truck fleet parameters in the Citi Bike domain.
    > - Adjusted the rebalancing meta to include fleet details when applicable.
    > - Refactored `build_graph_data` and `run_scenario` to handle Citi Bike specifics.
    > - Modified relevant tests to ensure proper integration and functionality of the new structure.
    > - Updated documentation to reflect changes in the architecture and API.
- `#278 20:40 91bf68c` +0/-246 (1f) [docs] — **Remove split_gbp_ml.md plan document as it is no longer relevant**
