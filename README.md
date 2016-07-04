# SLURM Reports generator

This utility is useful to analyze the usage of a SLURM computing
cluster and to create reports allowing to track the cluster usage. It
uses the accounting data provided by 'sacct' to create the
reports. It's not useful for billing but is used for capacity planning
and to tailor an adequate Scheduling Policy.

## Requirements

It's a Python program. It's been developed using Python 2.7.6 and uses
Python's Standard libraries plus:

* numpy (for histogram creation)

# Installation

This project uses the standard Python packaging system. Therefore,
just run:

```
    python setup.py install
```

It will also automatically install any dependencies needed.

This setup does not create an appropriate config file. If you need one
to customize the tool's behaviour, read the following section. By
default config files are looked for in the current directory.

## Configuration file

The configuration file uses INI file's syntax. The options available
are:

```
[general]
report_title=Report for  my cluster
avail_cpu_number=20
sacct_path=/bin/sacct
```

* sacct\_path: Path to the sacct command. This is the option most
  likely to need configuration (default: /bin/sacct)
* report\_title: Provides the title for the Report generated (default: Report)
* avail\_cpu\_number: Number of CPUs available to compute the usage on
  the period (default: None)

# Usage

Show help:

```
    ./report_colas.py --help
```

Show standard report (using defaults):

```
    ./report_colas.py --start 2016-05-30 --end 2016-06-01
```

