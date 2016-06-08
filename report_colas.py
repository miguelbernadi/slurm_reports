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
#Handle dates
import datetime

sacct_command = [
                 "", #Placeholder for sacct binary
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
    """ Parse a string representing the duration of a job into a number of seconds elapsed. Supplied string is of format 00-00:00:00 """
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
    return days + hours + minutes + seconds

def parse_date(datestring):
    """ Convert a string representing a date to a naive datetime object """
    date_array = datestring.split("-")
    return datetime.date(int(date_array[0]),int(date_array[1]),int(date_array[2]))

class UserRecord:
    """ Structure to store aggregated job record statistics on a user basis """
    def __init__(self, name):
        self.username = name
        self.qos_jobs = {} # qos_name: [number, cpuh]
        self.partition_jobs = {}

    def add_record(self, cpu, cpu_hours, partition, qos):
        """ Add a record's data to the user's history """
        if qos in self.qos_jobs:
            q = self.qos_jobs[qos]
            self.qos_jobs[qos] = [ q[0] + 1, q[1] + cpu_hours ]
        else:
            self.qos_jobs[qos] = [ 1, cpu_hours ]
        if partition in self.partition_jobs:
            p = self.partition_jobs[partition]
            self.partition_jobs[partition] = [ p[0] + 1, p[1] + cpu_hours ]
        else:
            self.partition_jobs[partition] = [ 1, cpu_hours ]

    def total_jobs(self):
        """ Return the number of jobs recorded for the user """
        count = 0
        for i in self.partition_jobs.keys():
            count += self.partition_jobs[i][0]
        return count
        
    def total_cpuh(self):
        """ Return the number of compute hours recorded for the user """
        count = 0
        for i in self.partition_jobs.keys():
            count += self.partition_jobs[i][1]
        return count

    def jobs_qos(self, qos):
        return self.qos_jobs[qos]

    def jobs_partition(self, partition):
        return self.partition_jobs[partition]
 
class Data:
    """ Stores the analysed data """
    users = {} # hold user -> UserRecord relations
    times = [] # list of tuples (elapsed, timelimit, accuracy)
    total_entries = 0
    total_completed = 0
    total_timeout = 0
    total_failed = 0
    total_node_fail = 0
    total_cancelled_auto = 0
    total_cancelled_user = 0
    total_running = 0
    total_requeued = 0
    total_pending = 0
    total_unknown = 0
    total_compute_hours = 0
    pattern_time = re.compile("[0-9]+")
    pattern_cancelled_user = re.compile("^CANCELLED by [0-9]+$")

    def aggregate_job_data(self,job_fields):
        """ Process the records one row at a time """
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
            accuracy = 100.0 * duration / timelimit
            self.times.append(tuple([duration, timelimit, accuracy]))

    def count_per_user(self, username, cpu, duration, partition, qos): 
        """ Create per user recordings of job statistics """
        cpu_hours = self.compute_cpu_hours(cpu, duration)
        if username not in self.users:
            self.users[username] = UserRecord(username)
        self.users[username].add_record(cpu, cpu_hours, partition, qos) 

    def compute_cpu_hours(self, cpu, duration):
        """ Compute the cpu hours of a job """
        total = parse_time(duration) * int(cpu) / 3600.
        self.total_compute_hours += total
        return total

    def count_job_status(self, job_status):
        """ Compute the number of jobs in each status """
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
        elif job_status == "REQUEUED":
            self.total_requeued += 1
        elif job_status == "PENDING":
            self.total_pending += 1
        elif self.pattern_cancelled_user.match(job_status): # CANCELLED by uid
            self.total_cancelled_user += 1
        else:
            self.total_unknown += 1

class Report:
    """ Displays results """

    def __init__(self, data):
        self.data = data

    def summary_report(self, title):
        """ Main report output, including overall job execution details """
        print title
        print "Report generated on                    %s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        print "Data gathered between           %s - %s"    % (args.start, args.end)
        print "-" * 55
        print "Jobs submitted:                      %6d  (%6.2f %%)" % ( self.data.total_entries,        float(self.data.total_entries)        / self.data.total_entries * 100 )
        print "Jobs executed successfully:          %6d  (%6.2f %%)" % ( self.data.total_completed,      float(self.data.total_completed)      / self.data.total_entries * 100 )
        print "Jobs executed but timed out:         %6d  (%6.2f %%)" % ( self.data.total_timeout,        float(self.data.total_timeout)        / self.data.total_entries * 100 )
        print "Jobs executed but failed:            %6d  (%6.2f %%)" % ( self.data.total_failed,         float(self.data.total_failed)         / self.data.total_entries * 100 )
        print "Jobs where the node failed:          %6d  (%6.2f %%)" % ( self.data.total_node_fail,      float(self.data.total_node_fail)      / self.data.total_entries * 100 )
        print "Jobs cancelled automatically:        %6d  (%6.2f %%)" % ( self.data.total_cancelled_auto, float(self.data.total_cancelled_auto) / self.data.total_entries * 100 )
        print "Jobs cancelled by user:              %6d  (%6.2f %%)" % ( self.data.total_cancelled_user, float(self.data.total_cancelled_user) / self.data.total_entries * 100 )
        print "Jobs still pending:                  %6d  (%6.2f %%)" % ( self.data.total_pending,        float(self.data.total_pending)        / self.data.total_entries * 100 )
        print "Jobs requeued:                       %6d  (%6.2f %%)" % ( self.data.total_requeued,       float(self.data.total_requeued)       / self.data.total_entries * 100 )
        print "Jobs still running:                  %6d  (%6.2f %%)" % ( self.data.total_running,        float(self.data.total_running)        / self.data.total_entries * 100 )
        if self.data.total_unknown > 0:
            print "WARNING: unknown state: %s"   % self.data.total_unknown
        print "-" * 55

    def user_consumption_report(self, total_avail_cpuh):
        """ Report that shows per user consumption """
        print "%10s   %19s   %20s" % ("Username", "Jobs", "Cpu_hours")
        print "-" * 55
        for key in sorted(self.data.users.keys()):
            jobs = self.data.users[key].total_jobs()
            cpuh = self.data.users[key].total_cpuh()
            print "%10s   %8d (%6.2f %%)   %9.2f (%6.2f %%)" % (key, jobs, 100.0 * jobs / self.data.total_entries , cpuh, 100.0 * cpuh / self.data.total_compute_hours)
        print "-" * 55
        print     "%10s   %8d              %9.2f (%6.2f %%)" % ("Total", self.data.total_entries, self.data.total_compute_hours, 100.0 * self.data.total_compute_hours / total_avail_cpuh)

    def histogram(self, title, header, bins):
        """ Report showing a histogram table """
        values, limits = np.histogram([i[2] for i in self.data.times],bins)
        print title
        print "%15s | %6s | %8s - %8s" % (header, "amount", "percent", "cumulat")
        print "-" * 46
        cum = 0
        for i in range(0, len(values)):
            percent = 100.0 * values[i] / self.data.total_completed
            cum = 100.0  * sum(values[:i + 1]) / self.data.total_completed
            print "%6d - %6d | %6d | %6.2f %% - %6.2f %%" % (limits[i], limits[i+1], values[i], percent, cum)
    
def dump_configuration(config):
    """ Dump the configuration object's contents (debugging)"""
    for section in config.sections():
        for element in config.items(section):
            print section, element

def valid_date_string(string):
    """ Verify if the supplied string is a validly formatted date """
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
config.set("general", "sacct_path", "/bin/sacct")

# Parse options
parser = argparse.ArgumentParser(description='Report on job scheduler usage')
parser.add_argument('--start',       help='Date where the period starts', required=True, action='store', type=valid_date_string)
parser.add_argument('--end',         help='Date where the period ends',   required=True, action='store', type=valid_date_string)
parser.add_argument('-u', '--user',  help='Analyze a specific user',      action='store', nargs='*')
parser.add_argument('-c', '--config',help='Path to config file',          action='store', default='./config')
parser.add_argument('--debug',       help='Print lots of internal information', action="store_true")
parser.add_argument('--histogram',   help='Add histogram reports', choices=['all','none','timelimit','elapsed','accuracy'], default='all')

args = parser.parse_args()


# Read configuration file 
config.read(args.config)

# Apply cli options
if args.debug:
    dump_configuration(config)

if args.user:
     sacct_command.append("-u" + ','.join(args.user))

sacct_command.append("-S" + args.start)
sacct_command.append("-E" + args.end)

try:
    start = parse_date(args.start)
    end   = parse_date(args.end) + datetime.timedelta(days=1)
    timedelta = end - start

    total_avail_cpuh = int(config.get("general", "avail_cpu_number")) * timedelta.total_seconds() / 3600.0

    data = Data()
    report = Report(data)
    sacct_command[0] = config.get("general", "sacct_path")
    for line in subprocess.check_output(sacct_command).split("\n"):
        if line: 
            data.aggregate_job_data(line.split("|"))

    report.summary_report(config.get("general", "report_title"))
    print ""
    report.user_consumption_report(total_avail_cpuh)
    if args.histogram == 'elapsed' or args.histogram == 'all':
        print ""
        report.histogram("Elapsed table", "time (s)", time_bins)
    if args.histogram == 'timelimit' or args.histogram == 'all':
        print ""
        report.histogram("Timelimit table", "time (s)", time_bins)
    if args.histogram == 'accuracy' or args.histogram == 'all':
        print ""
        report.histogram("Accuracy table", "accuracy (%)", [0,10,20,30,40,50,60,70,75,80,85,90,91,92,93,94,95,96,97,98,99,100,200])

except subprocess.CalledProcessError as e:
    print "Execution error in:"
    print "%s returned code %s" % (e.cmd, e.returncode)
    print "Message is %s" % e.output

