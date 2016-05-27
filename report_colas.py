#!/usr/bin/env python

import sys
import argparse
#Calling sacct
import subprocess
#Regular expressions
import re
#Histograms
import numpy as np
import ConfigParser

sacct_command = [
                 "/opt/perf/bin/sacct",
                 "-a", 
                 "-o", 
                 "jobid,user,partition,qos,alloccpus,state,exitcode,elapsed,time", 
                 "--noheader", 
                 "-X", 
                 "-P"
                ]

#                         10m  20m  30m   1h   2h    3h    4h    5h    6h    7h    8h    9h   10h   11h   12h   24h    48h    72h     7d
time_bins = [0,60,120,300,600,1200,1800,3600,7200,10800,14400,18000,21600,25200,28800,32400,36000,39600,43200,86400,172800,259200,604800]
pattern_date_format = re.compile("^20[0-9][0-9]-[0-9]+-[0-9]+$")
pattern_time = re.compile("[0-9]+")
def parse_time(timestring):
    days = 0
    hours = 0
    minutes = 0
    seconds = 0
    time = pattern_time.findall(timestring)
    if len(time) == 4:
        days = int(time[0]) * 24 * 3600
        hours = int(time[1]) * 3600
        minutes = int(time[2]) * 60
        seconds = int(time[3])
    elif len(time) == 3:
        hours = int(time[0]) * 3600
        minutes = int(time[1]) * 60
        seconds = int(time[2])
    elif len(time) == 2:
        minutes = int(time[0]) * 60
        seconds = int(time[1])
    return days + hours + minutes + seconds


class Statistics:
    jobs = {} #Hold per user entries
    hours = {} #Hold per user entries
    times = [] # list of tuples (elapsed, timelimit, accuracy)
    total_entries = 0
    total_completed = 0
    total_timeout = 0
    total_failed = 0
    total_node_fail = 0
    total_cancelled_auto = 0
    total_cancelled_user = 0
    total_running = 0
    total_unknown = 0
    total_compute_hours = 0
    pattern_time = re.compile("[0-9]+")
    pattern_cancelled_user = re.compile("^CANCELLED by [0-9]+$")

    def aggregate_job_data(self,job_fields):
        if len(job_fields) < 9:
            pass
        self.total_entries += 1
        self.count_per_user(job_fields[1], job_fields[4], job_fields[7], job_fields[2], job_fields[3])
        partition = job_fields[2]
        cpus = job_fields[4]
        self.count_job_status(job_fields[5])
      
        duration = parse_time(job_fields[7])
        timelimit = parse_time(job_fields[8])
        if job_fields[5] == "COMPLETED":
            if timelimit > 0:
                accuracy = 100.0 * duration / timelimit
            else:
                accuracy = 0
            self.times.append(tuple([duration, timelimit, accuracy]))

    def count_per_user(self, username, cpu, duration, partition, qos): 
        cpu_hours = self.compute_cpu_hours(cpu, duration)
        if username in self.jobs:
            self.jobs[username] += 1
        else:
            self.jobs[username] = 1
        if username in self.hours:
            self.hours[username] += cpu_hours
        else:
            self.hours[username] = cpu_hours

    def compute_cpu_hours(self, cpu, duration):
        total = parse_time(duration) * int(cpu) / 3600.
        self.total_compute_hours += total
        return total

    def count_job_status(self, job_status):
        if job_status == "COMPLETED":
            self.total_completed += 1
        elif job_status == "TIMEOUT":
            self.total_timeout += 1
        elif job_status == "FAILED":
            self.total_failed += 1
        elif job_status == "NODE_FAIL":
            self.total_node_fail += 1
        elif job_status == "CANCELLED":
            self.total_cancelled_auto += 1
        elif job_status == "RUNNING":
            self.total_running += 1
        elif self.pattern_cancelled_user.match(job_status): # CANCELLED by uid
            self.total_cancelled_user += 1
        else:
            self.total_unknown += 1

    def summary_report(self, title):
        print title
        print "Data gathered between %s - %s"    % (args.start, args.end)
        print "-" * 48
        print "Jobs submitted:               %6d  (%6.2f %%)" % ( self.total_entries,        float(self.total_entries)/self.total_entries * 100 )
        print "Jobs executed successfully:   %6d  (%6.2f %%)" % ( self.total_completed,      float(self.total_completed)/self.total_entries * 100 )
        print "Jobs executed but timed out:  %6d  (%6.2f %%)" % ( self.total_timeout,        float(self.total_timeout)/self.total_entries * 100 )
        print "Jobs executed but failed:     %6d  (%6.2f %%)" % ( self.total_failed,         float(self.total_failed)/self.total_entries * 100 )
        print "Jobs where the node failed:   %6d  (%6.2f %%)" % ( self.total_node_fail,      float(self.total_node_fail)/self.total_entries * 100 )
        print "Jobs cancelled automatically: %6d  (%6.2f %%)" % ( self.total_cancelled_auto, float(self.total_cancelled_auto)/self.total_entries * 100 )
        print "Jobs cancelled by user:       %6d  (%6.2f %%)" % ( self.total_cancelled_user, float(self.total_cancelled_user)/self.total_entries * 100 )
        print "Jobs still running:           %6d  (%6.2f %%)" % ( self.total_running,        float(self.total_running)/self.total_entries * 100 )
        if self.total_unknown > 0:
            print "WARNING: unknown state: %s"   % self.total_unknown

    def user_consumption_report(self):
        print "%10s   %8s   %-12s" % ("Username", "Jobs", "Cpu_hours")
        print "-" * 42
        for key in sorted(self.jobs.keys()):
            print "%10s   %8d   %9.2f (%6.2f %%)" % (key, self.jobs[key], self.hours[key], self.hours[key]/self.total_compute_hours * 100)
        print "-" * 42
        print "%10s   %8d   %9.2f (%6.2f %%)" % ("Total", self.total_entries, self.total_compute_hours, self.total_compute_hours/4166.40) # 416640 cpu hours in a week


    def timelimit_histogram(self):
        values, limits = np.histogram([i[1] for i in self.times],bins=time_bins)
        print "Timelimit table"
        print "%15s | %6s | %8s - %8s" % ("accuracy (%)", "amount", "percent", "cumulat")
        print "-" * 46
        cum = 0
        for i in range(0, len(values)):
            percent = 100.0 * values[i] / self.total_completed
            cum += percent
            print "%6d - %6d | %6d | %6.2f %% - %6.2f %%" % (limits[i], limits[i+1], values[i], percent, cum)

    def elapsed_histogram(self):
        values, limits = np.histogram([i[0] for i in self.times],bins=time_bins)
        print "Elapsed table"
        print "%15s | %6s | %8s - %8s" % ("accuracy (%)", "amount", "percent", "cumulat")
        print "-" * 46
        cum = 0
        for i in range(0, len(values)):
            percent = 100.0 * values[i] / self.total_completed
            cum += percent
            print "%6d - %6d | %6d | %6.2f %% - %6.2f %%" % (limits[i], limits[i+1], values[i], percent, cum)

    def accuracy_histogram(self):
        values, limits = np.histogram([i[2] for i in self.times],bins=[0,5,10,20,30,40,50,60,70,80,90,100,200,300,400,500,600,700,800,900,1000])
        print "Accuracy table"
        print "%13s | %6s | %8s - %8s" % ("accuracy (%)", "amount", "percent", "cumulat")
        print "-" * 44
        cum = 0
        for i in range(0, len(values)):
            percent = 100.0 * values[i] / self.total_completed
            cum += percent
            print "%5.0f - %5.0f | %6d | %6.2f %% - %6.2f %%" % (limits[i], limits[i+1], values[i], percent, cum)
    
def dump_configuration(config):
    for section in config.sections():
        for element in config.items(section):
            print section, element

def valid_date_string(string):
    if not pattern_date_format.match(string):
         msg="%r is not a valid date string. Format is YYYY-MM-DD" % string
         raise argparse.ArgumentTypeError(msg)
    return string

# Main

# Configuration defaults
defaults = {}
config = ConfigParser.SafeConfigParser(defaults, allow_no_value=True)
config.add_section("general")
config.set("general", "report_title", "Report")
config.set("general", "configuration_file_path", "./config")

# Parse options
parser = argparse.ArgumentParser(description='Report on job scheduler usage')
parser.add_argument('--start',       help='Date where the period starts', required=True, action='store', type=valid_date_string)
parser.add_argument('--end',         help='Date where the period ends',   required=True, action='store', type=valid_date_string)
parser.add_argument('-u', '--user',  help='Analyze a specific user',      action='store', nargs='*')
parser.add_argument('-c', '--config',help='Path to config file',          action='store')
parser.add_argument('--debug',       help='Print lots of internal information', action="store_true")
parser.add_argument('--histogram',   help='Add histogram reports', choices=['all','none','timelimit','elapsed','accuracy'], default='all')

args = parser.parse_args()

# Override configuration file location
if args.config != None:
    config.set("general", "configuration_file_path", args.config)

# Read configuration file 
config.read(config.get("general", "configuration_file_path"))

# Apply cli options
if args.debug:
    dump_configuration(config)

if args.user:
     sacct_command.append("-u" + ','.join(args.user))

sacct_command.append("-S" + args.start)
sacct_command.append("-E" + args.end)

try:
    results = Statistics()
    for line in subprocess.check_output(sacct_command).split("\n"):
        if line: 
            results.aggregate_job_data(line.split("|"))

    results.summary_report(config.get("general", "report_title"))
    print ""
    results.user_consumption_report()
    if args.histogram == 'elapsed' or args.histogram == 'all':
        print ""
        results.elapsed_histogram()
    if args.histogram == 'timelimit' or args.histogram == 'all':
        print ""
        results.timelimit_histogram()
    if args.histogram == 'accuracy' or args.histogram == 'all':
        print ""
        results.accuracy_histogram()

except subprocess.CalledProcessError as e:
    print "Execution error in:"
    print "%s returned code %s" % (e.cmd, e.returncode)
    print "Message is %s" % e.output

