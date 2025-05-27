from setuptools import setup, find_packages

setup(
    name="pyraft",
    version="0.1.0",
    description="Python implementation of the Raft consensus protocol",
    author="Devin AI",
    packages=find_packages(),
    install_requires=[
        "msgpack",
        "aiohttp",
    ],
    python_requires=">=3.7",
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
        "Programming Language :: Python :: 3.12",
    ],
)
