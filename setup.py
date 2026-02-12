"""Setup configuration for NovaTrack Analytics Pipeline."""

from setuptools import setup, find_packages

setup(
    name="novatrack-pipeline",
    version="1.0.0",
    author="NovaTrack Data Engineering Team",
    description="Data pipeline for NovaTrack Analytics telemetry data",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "requests>=2.31.0",
        "psycopg2-binary>=2.9.9",
        "pandas>=2.1.4",
        "SQLAlchemy>=2.0.25",
        "python-dotenv>=1.0.1",
    ],
)