# Dagster IFT RAG 📄

<p align="center">
  <img width = "1000" height = "350" src = "https://dagster.io/images/brand/logos/dagster-primary-horizontal.jpg">
</p>

## Structure 👷

Structured has been borrowed from [Dagster University](https://github.com/dagster-io/project-dagster-university).

```
├── Dagster-Template
├── ├── ift_rag
│       ├── assets
│       ├── configs
│       ├── jobs
│       ├── partitions
│       ├── resources
│       ├── schedules
│       ├── resources
│       ├── sensors
├── ├── ift_rag_tests
├── ├── pyproject.toml
├── ├── setup.cfg
├── ├── .gitignore
└── 
```

To get the [default Dagster project](https://docs.dagster.io/getting-started/create-new-project) structure run:

```bash
dagster project scaffold --name your_name_here
```

## How to run locally 🧰

To see the all dagster commands [click here](https://docs.dagster.io/_apidocs/cli).

```bash
dagster dev
```