# Name
slurp - an nzb downloader

# Synopsis
`$ slurp download [-t] [-n num_threads] [-u user [-p password]] -s host[:port] -f nzb_filename`

`$ slurp list -f nzb_filename`

# Description
The *slurp* utility reads an *nzb* file and downloads all its files.

# Installation
```
git clone https://github.com/jpsutton/slurp
git submodule update
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```
