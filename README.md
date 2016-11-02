# LAPD Stop Data

## Requirements

### System requirements

* Python 2.7 (and virtualenv)
* Oracle instant client for cx_Oracle
    * [OS X installation](https://gist.github.com/thom-nic/6011715)
    * Windows installation:
        * Not sure, but should be something along the lines of installing Oracle Instant Client (for Python 2.7), exporting some environment variables, and using `pip` to install the package.
        * <http://stackoverflow.com/a/35634151/4410590>

### Python packages
* [Flask](http://flask.pocoo.org)
* [pandas](http://pandas.pydata.org)
* [cx_Oracle](https://cx-oracle.readthedocs.io)

## Setup

    $ virtualenv ve
    $ pip install -r requirements.txt

## Development

    $ source ve/bin/activate
    $ ./run.sh