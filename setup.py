from setuptools import find_packages, setup

setup(
    name="metadata_driven_ingestion",
    version="0.2.4",
    description="Metadata-driven Databricks ingestion framework supporting FILE, JDBC, and HTTP API sources.",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "metadata_ingestion=metadata_driven_ingestion.job:main",
        ]
    },
)
