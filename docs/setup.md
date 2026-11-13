# Setup

The project declares Python >=3.11,<3.12. Install requirements.txt in an isolated
Python 3.11 environment. Exact pinned installation remains unverified in this
increment; execution used the available Python 3.12 runtime. See
[validation](drift_validation.md) for versions, test commands and limitations.

Place CIC-IDS2017 in data/raw/cicids2017 and run existing preprocessing scripts
before capture experiments. Large data and model files remain excluded from Git.
Kafka/Spark and application services are not configured.
