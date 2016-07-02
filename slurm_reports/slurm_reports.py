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

def cli():
    """ Entrypoint for the CLI tool """
    Slurm_Reports().main()

class UserRecord(object):
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

class Data(object):
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
    total_avail_cpuh = 0
    pattern_time = re.compile("[0-9]+")
    pattern_cancelled_user = re.compile("^CANCELLED by [0-9]+$")

    def __init__(self, total_avail_cpuh, start_date, end_date):
        self.total_avail_cpuh = total_avail_cpuh
        self.start_date = start_date
        self.end_date = end_date

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

    def get_elapsed_values(self):
        """ Return array containing elapsed times for all jobs in set """
        return [i[0] for i in self.times]

    def get_timelimit_values(self):
        """ Return array containing timelimit times for all jobs in set """
        return [i[1] for i in self.times]

    def get_accuracy_values(self):
        """ Return array containing accuracy values for all jobs in set """
        return [i[2] for i in self.times]

class Report(object):
    """ Displays results """

    def __init__(self, data):
        self.data = data

    def summary_report(self, title):
        """ Main report output, including overall job execution details """
        print title
        print "Report generated on                    %s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        print "Data gathered between           %s - %s"    % (self.data.start_date, self.data.end_date)
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

    def user_consumption_report(self):
        """ Report that shows per user consumption """
        print "%10s   %19s   %20s" % ("Username", "Jobs", "Cpu_hours")
        print "-" * 55
        for key in sorted(self.data.users.keys()):
            jobs = self.data.users[key].total_jobs()
            cpuh = self.data.users[key].total_cpuh()
            print "%10s   %8d (%6.2f %%)   %9.2f (%6.2f %%)" % (key, jobs, 100.0 * jobs / self.data.total_entries , cpuh, 100.0 * cpuh / self.data.total_compute_hours)
        print "-" * 55
        print     "%10s   %8d              %9.2f (%6.2f %%)" % ("Total", self.data.total_entries, self.data.total_compute_hours, 100.0 * self.data.total_compute_hours / self.data.total_avail_cpuh)

    def histogram(self, title, header, bins, data):
        """ Report showing a histogram table """
        values, limits = np.histogram(data, bins)
        print title
        print "%15s | %6s | %8s - %8s" % (header, "amount", "percent", "cumulat")
        print "-" * 46
        cum = 0
        for i in range(0, len(values)):
            percent = 100.0 * values[i] / self.data.total_completed
            cum = 100.0  * sum(values[:i + 1]) / self.data.total_completed
            print "%6d - %6d | %6d | %6.2f %% - %6.2f %%" % (limits[i], limits[i+1], values[i], percent, cum)

pattern_time = re.compile("[0-9]+")

def parse_date(datestring):
    """ Convert a string representing a date to a naive datetime object """
    date_array = datestring.split("-")
    return datetime.date(int(date_array[0]), int(date_array[1]), int(date_array[2]))

def parse_time(timestring):
    """
    Parse a string representing the duration of a job into a number of
    seconds elapsed. Supplied string is of format 00-00:00:00
    """
    time = pattern_time.findall(timestring)
    if len(time) == 4:
        return int(time[0]) * 24 * 3600 + int(time[1]) * 3600 + int(time[2]) * 60 + int(time[3])
    elif len(time) == 3:
        return int(time[0]) * 3600 + int(time[1]) * 60 + int(time[2])

class Slurm_Reports(object):
    def __init__(self):
        self.sacct_command = [
            "", #Placeholder for sacct binary
            "-a",
            "-o",
            "jobid,user,partition,qos,alloccpus,state,exitcode,elapsed,time",
            "--noheader",
            "-X",
            "-P"
        ]
        defaults = {}
        self.config = ConfigParser.SafeConfigParser(defaults, allow_no_value=True)
        self.time_bins = [
            0, 60, 120, 300, 600, 1200, 1800, 3600, # 10m, 20m, 30m, 1h
            7200, 10800, 14400, 18000, 21600, # 2h, 3h, 4h, 5h, 6h,
            25200, 28800, 32400, 36000, 39600, # 7h, 8h, 9h, 10h, 11h
            43200, 86400, 172800, 259200, 604800 # 12h, 24h, 48h, 72h, 7d
        ]
        self.pattern_date_format = re.compile("^20[0-9][0-9]-[0-9]+-[0-9]+$")
        self.args = self.parse_args()
        self.report = ""

    def parse_args(self):
        """ Parse the CLI arguments """
        # Parse options
        parser = argparse.ArgumentParser(description='Report on job scheduler usage')
        parser.add_argument('--start', help='Date where the period starts',
                            required=True, action='store', type=self.valid_date_string)
        parser.add_argument('--end', help='Date where the period ends',
                            required=True, action='store', type=self.valid_date_string)
        parser.add_argument('-u', '--user', help='Analyze a specific user',
                            action='store', nargs='+')
        parser.add_argument('-c', '--config', help='Path to config file',
                            action='store', default='./config')
        parser.add_argument('--debug', help='Print lots of internal information',
                            action="store_true")
        subparsers = parser.add_subparsers(title='subcommands')

        parser_histo = subparsers.add_parser('histogram')
        parser_histo.set_defaults(func=self.args_histo)
        parser_histo.add_argument('--mode', help='Histogram modes',
                                  choices=['all', 'none', 'timelimit', 'elapsed', 'accuracy'],
                                  default='all')

        parser_report = subparsers.add_parser('report')
        parser_report.set_defaults(func=self.args_report)
        parser_report.add_argument('--mode', help='Report modes',
                                   choices=['all', 'summary', 'user'], default='all')

        return parser.parse_args()

    def dump_configuration(self):
        """ Dump the configuration object's contents (debugging)"""
        for section in self.config.sections():
            for element in self.config.items(section):
                print section, element

    def valid_date_string(self, string):
        """ Verify if the supplied string is a validly formatted date """
        if not self.pattern_date_format.match(string):
            msg = "%r is not a valid date string. Format is YYYY-MM-DD" % string
            raise argparse.ArgumentTypeError(msg)
        return string

    def args_report(self):
        """ Present the appropriate reports depending on CLI options """
        if self.args.mode == "summary" or self.args.mode == "all":
            self.report.summary_report(self.config.get("general", "report_title"))
            print ""
            if self.args.mode == "user" or self.args.mode == "all":
                self.report.user_consumption_report()

    def args_histo(self):
        """ Present the appropriate histograms depending on CLI options """
        if self.args.mode == 'elapsed' or self.args.mode == 'all':
            print ""
            self.report.histogram("Elapsed table", "time (s)", self.time_bins, self.report.data.get_elapsed_values())
        if self.args.mode == 'timelimit' or self.args.mode == 'all':
            print ""
            self.report.histogram("Timelimit table", "time (s)", self.time_bins, self.report.data.get_timelimit_values())
        if self.args.mode == 'accuracy' or self.args.mode == 'all':
            print ""
            self.report.histogram("Accuracy table", "accuracy (%)", [0,10,20,30,40,50,60,70,75,80,85,90,91,92,93,94,95,96,97,98,99,100,200], self.report.data.get_accuracy_values())

    #CLI entrypoint
    def main(self):
        # Configuration defaults
        self.config.add_section("general")
        self.config.set("general", "report_title", "Report")
        self.config.set("general", "sacct_path", "/bin/sacct")

         # Read configuration file
        self.config.read(self.args.config)

        # Apply cli options
        if self.args.debug:
            self.dump_configuration()

        if self.args.user:
            self.sacct_command.append("-u" + ','.join(self.args.user))

        self.sacct_command.append("-S" + self.args.start)
        self.sacct_command.append("-E" + self.args.end)

        try:
            start = parse_date(self.args.start)
            end = parse_date(self.args.end)
            timedelta = end - start + datetime.timedelta(days=1)

            total_avail_cpuh = int(self.config.get("general", "avail_cpu_number")) * \
                               timedelta.total_seconds() / 3600.0

            data = Data(total_avail_cpuh, self.args.start, self.args.end)
            self.sacct_command[0] = self.config.get("general", "sacct_path")
            for line in subprocess.check_output(self.sacct_command).split("\n"):
                if line:
                    data.aggregate_job_data(line.split("|"))

            self.report = Report(data)
            self.args.func()

        except subprocess.CalledProcessError as e:
            print "Execution error in:"
            print "%s returned code %s" % (e.cmd, e.returncode)
            print "Message is %s" % e.output
