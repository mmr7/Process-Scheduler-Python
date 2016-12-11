# Mortaza Rasulzada
# Edwin Francois
# CISC - 361
# Programming Project: Scheduling and Deadlock Avoidance

import sys
import queue
from prettytable import PrettyTable
import string

# Class to represent the system
class System:
    def __init__(self, time, memory, devices, quantum):
        self.time = time
        self.total_memory = memory
        self.avail_mem = memory
        self.total_dev = devices
        self.avail_dev = devices
        self.quantum = quantum
        self.holdqueue1 = queue.PriorityQueue() #SJF Queue
        self.holdqueue2 = queue.Queue() #FIFO Queue
        self.readyqueue = queue.Queue() #Queue()
        self.waitqueue = queue.Queue()
        self.completequeue = queue.Queue()  # complete queue
        self.requestqueue = queue.Queue()   # request queue
        self.releasequeue = queue.Queue()   # release queue
        self.cpuqueue = queue.Queue()       # cpu queue
        self.timer = 0    # global timer
        self.tempquantum = quantum    # temporary qunatum used in cpu

    # prints the attributes of the system
    def to_string(self):
        t = PrettyTable([' System Attribute', 'Numerical Value'])
        t.add_row(['Time #', self.time])
        t.add_row(['total_mem', self.total_memory])
        t.add_row(['avail_mem', self.avail_mem])
        t.add_row(['starting_dev', self.total_dev])
        t.add_row(['tot_avail_dev', self.avail_dev])
        t.add_row(['quantum', self.quantum])
        print(t)

# Class for device requests
class Request:
    def __init__(self, time, job_number, devices_requested):
        self.time = time
        self.job_number = job_number
        self.devices_requested = devices_requested

# Class for device releases
class Release:
    def __init__(self, time, job_number, devices_requested):
        self.time = time
        self.job_number = job_number
        self.devices_requested = devices_requested


# Class to represent a job
class Job:
    def __init__(self, arrival_time, number, memory, devices, run_time, priority):
        self.number = number     # job number
        self.arrivalTime = arrival_time
        self.memory = memory
        self.devices = devices
        self.run_time = run_time
        self.priority = priority
        self.turnaround = 0   # turn around time for each job

    # Checks if the Jobs are of equal run time
    def __eq__(self, other):
        try:
            return self.run_time == other.run_time
        except AttributeError:
            return NotImplemented

    # Of two jobs, returns true if "other" has a longer run time than "self" (used by priority queue to determine
    # priority)
    def __lt__(self, other):
        try:
            return self.run_time < other.run_time
        except AttributeError:
            return NotImplemented

    # prints the contents of a job
    def to_string(self):
        t = PrettyTable([' Job Attribute', 'Numerical Value'])
        t.add_row(['Job #', self.number])
        t.add_row(['Arrival Time', self.arrivalTime])
        t.add_row(['Required Memory', self.memory])
        t.add_row(['Devices', self.devices])
        t.add_row(['Priority', self.priority])
        t.add_row(['Runtime Remaining', self.run_time])
        t.add_row(['Turnaround Time', self.turnaround])
        print(t)

# initialize system parameters to 0
total_system = System(0, 0, 0, 0)


# entry point
def main():
    # my code here
    print("Starting scheduler")
    file = sys.argv[1]
    print("Running scheduler with file = " + file)
    with open(file) as lines:  # open the file
        for line in lines:
            process(line) # process each line one at a time

    # this makes sure to keep the process going until everyhting is in the complete queu
    # basically makes sure no job gets unfinished when the program ends
    while total_system.readyqueue.empty() is False & total_system.holdqueue1.empty() is False & \
            total_system.holdqueue2.empty() is False & \
            total_system.waitqueue.empty() is False:
        newTime = total_system.timer + 1
        TimerFunction(newTime)

    # system is finished, so print final state of system
    print("FINISHED! Final State of System: ")
    system_turnaround = total_system.timer - total_system.time
    print("The system turn around time is: " + str(system_turnaround))
    setup_status()
    total_system.to_string()


# Dispatch function to handle all lines
def process(line):
    args = line_to_args(line)   # get arguments from line as an array

    # update the time based on new arrival
    # we print the final state of the system after the
    # program is finished automatically, so no need for D 9999
    if int(args[0]) != 9999:
        if int(args[0]) != int(total_system.timer):
            TimerFunction(int(args[0]))     # call timer function with new time to be updated

    # prints what line we are processing
    print("Processing line " + line)

    # any line that starts with C
    # but in our case it initializes the system
    if line[0] == 'C' :
        print("processing configuration")
        setup_system(line)
        total_system.to_string()

    # process a job arrival, determines what hold queue it goes in
    elif line[0] == 'A':
        print("processing job arrival")
        job = setup_job(line)
        handle_job(job)

    # device request, put the request in the request queue
    elif line[0] == 'Q':
        print("processing device request")
        request = setup_request(line)
        req(request)

    # device release, put the release in the release queue
    elif line[0] == 'L':
        print("processing device release")
        release = setup_release(line)
        rel(release)

    # print the current state of system or final state
    elif line[0] == 'D':
        print("processing display")
        print("CURRENT STATUS OF SYSTEM: ")
        setup_status()

    #not a valid line
    else:
        print("line was invalid .... \"" + line + "\"")


# print current system status
# prints all the queue contents and job contents
def setup_status():
    # check if complete queue is empty
    if total_system.completequeue.qsize() == 0:
        print("complete queue is empty")
    else:
        print("Complete queue contents: ")
        for i in total_system.completequeue.queue:
            i.to_string()    # prints contents of complete queue

    # check if hold queue 1 is empty
    if total_system.holdqueue1.qsize() == 0:
        print("hold queue 1 is empty")
    else:
        print("hold queue 1 contents:")
        for i in total_system.holdqueue1.queue:
            i.to_string()    #

    # check if hold queue 2 is empty
    if total_system.holdqueue2.qsize() == 0:
        print("hold queue 2 is empty")
    else:
        print("hold queue 2 contents:")
        for i in total_system.holdqueue2.queue:
            i.to_string()

    # check if ready queue is empty
    if total_system.readyqueue.qsize() == 0:
        print("ready queue is empty")
    else:
        print("ready queue contents:")
        for i in total_system.readyqueue.queue:
            i.to_string()

    # check if wait queue is empty
    if total_system.waitqueue.qsize() == 0:
        print("wait queue is empty")
    else:
        print("wait queue contents:")
        for i in total_system.waitqueue.queue:
            i.to_string()

    # check if cpu queue is empty
    if total_system.cpuqueue.qsize() == 0:
        print("cpu is empty")
    else:
        print("job thats is running on cpu:")
        for i in total_system.cpuqueue.queue:
            i.to_string()


# Setup system with specified args
def setup_system(line):
    global total_system
    args = line_to_args(line)
    total_system = System(int(args[0]), int(args[1]), int(args[2]), int(args[3]))
    print("System has been set up")
    return total_system


# Instantiates a job object based on the input parameters
def setup_job(line):
    args = line_to_args(line)
    job = Job(int(args[0]), int(args[1]), int(args[2]), int(args[3]), int(args[4]), int(args[5]))
    print("Job has been set up")
    return job

# Instantiates a request object based on the input parameters
def setup_request(line):
    args = line_to_args(line)
    request = Request(int(args[0]), int(args[1]), int(args[2]))
    print("Request has been set up")
    return request

# Instantiates a job release based on the input parameters
def setup_release(line):
    args = line_to_args(line)
    release = Release(int(args[0]), int(args[1]), int(args[2]))
    print("Release has been set up")
    return release

# Handle's enqueueing a job based on priority and how much memory is available
def handle_job(job):
    print("job memory: " + str(job.memory))
    print("system memory: " + str(total_system.total_memory))

    # check if job can be accepted or not
    if job.memory > total_system.total_memory:
        print("Rejected. This job needs more memory than the total memory that the system has.")

    # not enough devices
    elif job.devices > total_system.total_dev:
        print("Rejected. This job needs more devices than the system has.")

    # accepted, put in correct hold queue
    else:
        if job.priority == 1:
            total_system.holdqueue1.put(job)
            print("Enqueued in hold Queue 1.")
        else:
            total_system.holdqueue2.put(job)
            print("Enqueued in hold Queue 2.")

    #update all the queues, move jobs if needed
    move_jobs()

# puts requests in request queue
def req(request):
    total_system.requestqueue.put(request)

#puts release in release queue
def rel(release):
    total_system.releasequeue.put(release)

# cpu function
def cpu():
    print("CPU Processing")

    # check if any job is in the ready queue
    if total_system.readyqueue.empty():
        print("ReadyQueue is empty")

    else:
        #get the first job in ready queue
        cpu_job = total_system.readyqueue.queue[0]

        # only add if there is no job in the cpu
        if total_system.cpuqueue.qsize() == 0:
            total_system.cpuqueue.put(cpu_job)

        # print current job in cpu
        print("JOB: " + str(cpu_job.number))

        # check if we have a device request
        if total_system.requestqueue.empty() is False:

            # check if job is in the cpu
            if cpu_job.number == total_system.requestqueue.queue[0].job_number:
                # have enough devices, grant request and put job at back of ready queue
                if total_system.avail_dev > total_system.requestqueue.queue[0].devices_requested:
                    print("REQUEST MATCHED WITH JOB")
                    total_system.avail_dev -= total_system.requestqueue.queue[0].devices_requested
                    total_system.readyqueue.queue[0].devices += total_system.requestqueue.queue[0].devices_requested
                    total_system.readyqueue.put(cpu_job)  # put in back of ready queue
                    total_system.readyqueue.get()  # remove from the front of ready queue
                    total_system.requestqueue.get()         # remove from request queue
                    total_system.readyqueue.task_done()
                    total_system.tempquantum = total_system.quantum    # update quantum
                    total_system.cpuqueue.get()     # remove from the cpu queue
                    total_system.cpuqueue.task_done()
                    print("Device Request granted, Quantum interrupted. Placed back in ready queue")

                # not enough devices, put job in waitqueue
                else:
                    total_system.readyqueue.queue[0].devices += total_system.requestqueue.queue[0].devices_requested
                    total_system.waitqueue.put(cpu_job)  # add to wait queue
                    total_system.requestqueue.get()   # remove from front of request queue
                    total_system.readyqueue.task_done()
                    total_system.tempquantum = total_system.quantum     # reset quantum
                    total_system.cpuqueue.get()  # remove from the cpu queue
                    total_system.cpuqueue.task_done()
                    print("Device Request accepted, But job is in ready queue")

            # job is not in the cpu, drop request
            else:
                print("Request dropped, job " + str(total_system.requestqueue.queue[0].job_number) + " not in CPU")
                total_system.requestqueue.get()    # remove from front of request queue
                total_system.cpuqueue.get()  # remove from the cpu queue
                total_system.cpuqueue.task_done()

        # check if we have any release requests
        elif total_system.releasequeue.empty() is False:

            # check if job is in cpu
            if cpu_job.number == total_system.releasequeue.queue[0].job_number:
                print("Release accepted, quantum interrupted")
                # check if the devices requested to be released is greater than the devices that the job has
                # accept release accordingly
                # if it is greater, then release whatever devices is possible
                if total_system.releasequeue.queue[0].devices_requested > total_system.readyqueue.queue[0].devices:
                    total_system.avail_dev += total_system.readyqueue.queue[0].devices    # release devices
                    total_system.readyqueue.queue[0].devices = 0;
                    total_system.releasequeue.get()   # remove from release queue

                # job has enough devices
                else:
                    total_system.avail_dev += total_system.releasequeue.queue[0].devices_requested    # release devices
                    total_system.readyqueue.queue[0].devices -= total_system.releasequeue.queue[0].devices_requested
                    total_system.releasequeue.get()   # remove from release queue
                # check if we can move jobs from wait to ready queue
                if total_system.waitqueue.empty() is False:
                    if total_system.waitqueue.queue[0].devices < total_system.avail_dev:
                        total_system.readyqueue.put(total_system.waitqueue.get())

            # job not in cpu, release request dropped
            else:
                print("Release dropped, job " + str(total_system.releasequeue.queue[0].job_number) + " not in CPU")
                total_system.releasequeue.get_nowait()

        # no device requests or releases
        else:
            cpu_job.run_time -= 1     # subtract the cpu run time by 1
            print("cpu runtime after subtracting 1: " + str(cpu_job.run_time))   # print cpu run time left
            total_system.tempquantum -= 1    # subtract the quantam by 1
            print("quantam after subtracting 1: " + str(total_system.tempquantum))   # display remaining quantam time

            # if job has finished the run time, place in complete queue
            if cpu_job.run_time == 0:
                total_system.tempquantum = total_system.quantum    # reset quantum
                total_system.avail_mem += cpu_job.memory    #release the memory of job
                total_system.avail_dev += total_system.readyqueue.queue[0].devices   #release devices of job
                total_system.completequeue.put(cpu_job)  # put job in complete queue
                total_system.readyqueue.get()     # remove from ready queue
                cpu_job.turnaround = total_system.timer - cpu_job.arrivalTime    # calculate arrival time
                total_system.cpuqueue.get()  # remove from the cpu queue
                total_system.cpuqueue.task_done()
                print("Job " + str(cpu_job.number) + " is completed, Placed in complete Queue")
                # check if anything can be removed form wait queue
                if total_system.waitqueue.empty() is False:
                    if total_system.waitqueue.queue[0].devices < total_system.avail_dev:
                        total_system.readyqueue.put(total_system.waitqueue.get())

            # quantum finished, do round robin. reset quantum.
            elif total_system.tempquantum == 0:
                print("QUANTAM IS 0!")
                total_system.readyqueue.put(cpu_job)    # put job in back of ready queue
                total_system.readyqueue.get()     # remove job from the front of the queue
                total_system.readyqueue.task_done()
                total_system.tempquantum = total_system.quantum      # reset quantum
                total_system.cpuqueue.get()  # remove from the cpu queue
                total_system.cpuqueue.task_done()

    # update queues, move jobs if needed be.
    move_jobs()

# updates queues, moves jobs if needed be
def move_jobs():
    # check if any job can be moved from hold queue 1

    if total_system.holdqueue1.empty() is False:  # hold queue 1 is not empty
        if total_system.holdqueue1.queue[0].memory < total_system.avail_mem:
            print("There is memory available. Queueing in ready queue.")
            total_system.avail_mem -= total_system.holdqueue1.queue[0].memory   # allocat ememory for the job
            total_system.readyqueue.put(total_system.holdqueue1.get())     # put in ready queue
        else:
            print("There is not yet enough memory to move job from hold Queue 1 to ready Queue")

    # check if any job can be moved from hold queue 2
    else:
        print("holdqueue1 is empty, checking holdqueue2")
        if total_system.holdqueue2.empty() is False:    # hold queue 2 is not empty
            print("There is memory available. Queueing in ready queue.")
            if total_system.holdqueue2.queue[0].memory < total_system.avail_mem:
                total_system.avail_mem -= total_system.holdqueue2.queue[0].memory    # allocate memory for job
                total_system.readyqueue.put(total_system.holdqueue2.get())   # put in ready queue
            else:
                print("There is not yet enough memory to move job from hold Queue 2 to ready Queue")

        #hold queue 2 is empty
        else:
            print("holdqueue2 is empty also")

# Timer function.
def TimerFunction(updateTime):
    z = updateTime - total_system.timer   # how much the time has to be updated till
    for x in range(0,z):
        cpu()      # call cpu function
        total_system.timer += 1    # increase time
        print("TIME: " + str(total_system.timer))   # print timer


# Turn a line with letters/numbers/etc in to an array
# that can be used to fill classes. Produced arrays
# are just the arguments in numbers, each argument
# belonging in its own index and in order

# parse the line from file accordingly
def line_to_args(line):
    print("\"" + ''.join(filter(lambda c: c.isdigit() or c == ' ', line))[1:] + "\"")
    return (''.join(filter(lambda c: c.isdigit() or c == ' ', line))[1:]).split(" ")

if __name__ == "__main__":
    main()