from setuptools import setup, find_packages

install_requires = [
    "nats-py>=2.0.0",
]

import sys
if sys.version_info < (3, 7):
    install_requires.append("dataclasses")

setup(
    name="fluxmq",
    version="0.3.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=install_requires,
)