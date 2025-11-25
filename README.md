# dodona-export

## Description
ALMA monitoring database (text-based, a.k.a. Dodona) exporter

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Installation
Instructions on how to install and set up the project.
IMPORTANT: this app requires Python 3.6

```bash
git clone <repository-url>
cd mon-to-influx
python3.6 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage
Examples of how to use the project.

```bash
./bin/getDodonaData.py --help
./bin/getDodonaData.py --daysback 10 --abm DV01 --lru LORR --monitor AMBIENT_TEMPERATURE
./bin/getDodonaData.py --daysback 2 --abm DV01 --lru IFProc0 --monitor GAINS

```

Output is the CSV file: `dodona_export.csv`

## Requirements
- Python 3.6
- Dependencies listed in `requirements.txt`

## Contributing
Guidelines for contributing to the project.

## License
License information.

## Author
José L. Ortiz