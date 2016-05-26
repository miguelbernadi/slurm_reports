#!/usr/bin/env python

import sys
import argparse
import subprocess
import re

sacct_command = [
                 "/opt/perf/bin/sacct",
                 "-a", 
                 "-o", 
                 "jobid,user,partition,qos,alloccpus,state,exitcode,elapsed,time", 
                 "--noheader", 
                 "-X", 
                 "-P"
                ]

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
        duration = job_fields[7]
        timelimit = job_fields[8]


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

    def summarize_output(self):
        print "Report for SLURM usage at CNAG"
        print "Data gathered between %s - %s"    % (args.start, args.end)
        print "-" * 46
        print "Jobs submitted:               %6d  (%6.2f)" % ( self.total_entries,        float(self.total_entries)/self.total_entries * 100 )
        print "Jobs executed successfully:   %6d  (%6.2f)" % ( self.total_completed,      float(self.total_completed)/self.total_entries * 100 )
        print "Jobs executed but timed out:  %6d  (%6.2f)" % ( self.total_timeout,        float(self.total_timeout)/self.total_entries * 100 )
        print "Jobs executed but failed:     %6d  (%6.2f)" % ( self.total_failed,         float(self.total_failed)/self.total_entries * 100 )
        print "Jobs where the node failed:   %6d  (%6.2f)" % ( self.total_node_fail,      float(self.total_node_fail)/self.total_entries * 100 )
        print "Jobs cancelled automatically: %6d  (%6.2f)" % ( self.total_cancelled_auto, float(self.total_cancelled_auto)/self.total_entries * 100 )
        print "Jobs cancelled by user:       %6d  (%6.2f)" % ( self.total_cancelled_user, float(self.total_cancelled_user)/self.total_entries * 100 )
        print "Jobs still running:           %6d  (%6.2f)" % ( self.total_running,        float(self.total_running)/self.total_entries * 100 )
        if self.total_unknown > 0:
            print "WARNING: unknown state: %s"   % self.total_unknown

        print ""
        print "%10s   %8s   %-12s   %-9s" % ("Username", "Jobs", "Cpu_hours", "%")
        print "-" * 42
        for key in sorted(self.jobs.keys()):
            print "%10s   %8d   %9.2f   %6.2f" % (key, self.jobs[key], self.hours[key], self.hours[key]/self.total_compute_hours * 100)
        print "-" * 42
        print "%10s   %8d   %9.2f   %6.2f" % ("Total", self.total_entries, self.total_compute_hours, self.total_compute_hours/4166.40) # 416640 cpu hours in a week

    
# Main

# Parse options
parser = argparse.ArgumentParser(description='Report on job scheduler usage')
parser.add_argument('--start', help='Date where the period starts')
parser.add_argument('--end',   help='Date where the period ends')

args = parser.parse_args()

# Validate inputs
pattern_date_format = re.compile("^20[0-9][0-9]-[0-9]+-[0-9]+$")

if args.start == None or args.end == None:
     print "You must specify the limits for the period of the report."
     sys.exit()

if not pattern_date_format.match(args.start) or not pattern_date_format.match(args.end):
     print "The appropriate date format is of the form YYYY-MM-DD"
     sys.exit()

# Apply the validated inputs
sacct_command.append("-S" + args.start)
sacct_command.append("-E" + args.end)

try:
    results = Statistics()
    for line in subprocess.check_output(sacct_command).split("\n"):
        if line: 
            results.aggregate_job_data(line.split("|"))

    results.summarize_output()

except subprocess.CalledProcessError as e:
    print "Execution error in:"
    print "%s returned code %s" % (e.cmd, e.returncode)
    print "Message is %s" % e.output

