# SLURM Reports generator

This utility is useful to analyze the usage of a SLURM computing cluster and
to create reports allowing to track the cluster usage. It uses the accounting
data provided by 'sacct' to create the reports. It's not useful for billing
but is used for capacity planning and to tailor an adequate Scheduling
Policy.

## Requirements

It's a Python program. It's been developed using Python 2.7.6 and uses Python's
Standard libraries plus:

* numpy (for histogram creation)

# Installation

There is no real installation process required, but some configuration is
needed for a successful run. You should have a Configuration file, either
in the same directory as the command or in a configured address (can be
overridden through a command-line option). No standard default has been
set, so you could modify the code (to store conf in your $HOME):

```
-config.set("general", "configuration_file_path", "./config")
+config.set("general", "configuration_file_path", "/home/user/.slurm_report")
```

The minimal contents of the config file are:

```
[general]
sacct_path=/bin/sacct
```

## Usage

Show help:

```
    ./report_colas.py --help
```

Show standard report (using defaults):

```
    ./report_colas.py --start 2016-05-30 --end 2016-06-01
```

## Configuration file

The configuration file uses INI file's syntax. The options available are:

```
[general]
report_title=Report for  my cluster
avail_cpu_number=20
sacct_path=/bin/sacct
```

* sacct\_path: Path to the sacct command. This is the only mandatory option.
* report\_title: Provides the title for the Report generated (default: Report)
* avail\_cpu\_number: Number of CPUs available to compute the usage on the period (default: None)

