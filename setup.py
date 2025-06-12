from setuptools import setup, find_packages

setup(
    name="csb_validator",
    version="0.6.0",
    description="Validate CSB (Crowdsourced Bathymetry) GeoJSON and other formats",
    author="Clinton Campbell",
    author_email="clint.campbell@colorado.edu",
    url="https://github.com/CI-CMG/csb-validator",
    packages=find_packages(),
    install_requires=[
    "aiofiles"
    ],
    entry_points={
        "console_scripts": [
            "csb-validator=csb_validator.validator:main", 
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)