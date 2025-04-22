from setuptools import setup, find_packages

setup(
    name="fluxmq",
    version="0.15.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    description="FluxMQ for Python",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Eugene Zimnitskiy",
    author_email="karpotkin@gmail.com",
    url="https://github.com/flux/fluxmq-py",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.6",
    install_requires=[
        "nats-py>=2.2.0",
        "paho-mqtt>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.20.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "mypy>=1.0.0",
        ],
        "zenoh": [
            "zenoh>=0.7.0",
        ],
        "all": [
            "zenoh>=0.7.0",
        ],
    },
)