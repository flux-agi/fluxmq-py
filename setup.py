from setuptools import setup, find_packages

setup(
    name="fluxmq",
    version="0.3.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "nats-py>=2.0.0"
    ],
)