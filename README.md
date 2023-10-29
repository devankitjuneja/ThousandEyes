# ThousandEyes Data Pipeline

This repository contains a data pipeline that enables the extraction of data from a ThousandEyes HTTP server resource and loads it into a Microsoft SQL Server (MSSQL) database using the Data Load Tool (DLT) Python library.

## Getting Started

Before you can use this pipeline, make sure you have the following prerequisites in place:

- [Data Load Tool (DLT) Python library](https://dlthub.com/docs/intro): Install DLT and set up your DLT environment. You can find detailed installation instructions and documentation at the provided link.

## Installation

1. Clone this repository to your local machine:
 ```bash
 git clone https://github.com/yourusername/ThousandEyes.git
 cd ThousandEyes
 pip install -r requirements.txt
 ```
2. Run

```bash
python thousand_eyes.py --window=1d
```
