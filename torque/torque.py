#!/usr/bin/env python
'''
Environment variables:
PBS_VERBOSITY
PBS_DRYRUN   if you set this variable, no new jobs will be submitted,
but finished jobs will be retrieved.
'''

from subprocess import *
import commands, os, re, string, sys
from htp.ssh import ssh
from htp.rsync import *
from threading import Thread

class JobSubmitted(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
    def __str__(self):
        return string.join(self.args,'')

class JobInQueue(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
    def __str__(self):
        return string.join(self.args,'')

class JobRunning(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
    def __str__(self):
        return string.join(self.args,'')

class JobHold(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
    def __str__(self):
        return string.join(self.args,'')

class JobErrorStatus(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
    def __str__(self):
        return string.join(self.args,'')

class UnknownJobStatus(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
    def __str__(self):
        return string.join(self.args,'')

class JobDone(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
    def __str__(self):
        return string.join(self.args,'')

class PBS_MemoryExceeded(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
        mem = r'(\d+)kb'

        reg = re.compile(mem)
        print 'ARGS: =',self.args
        usedmem,requestedmem = reg.findall(str(self.args))
        print 'PBS_MEM_ERR: Resubmit the job with at least %skb of memory' % usedmem

        self.mem = int(usedmem)

        print 'PBS_MEM_ERR:', string.join(self.args)

class PBS_CputExceeded(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args
        cput = r'(\d+)'

        reg = re.compile(cput)
        cputused,cputlimit = reg.findall(str(args))
        print 'PBS_CPUT_ERR: Ran out of time.'

        self.cput = int(cputlimit)

        print 'PBS_CPUT_ERR:', string.join(self.args)

class PBS_Terminated(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args

class PBS_UnknownError(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args

class LAMMPI_Error(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args

class FORTRAN_Error(exceptions.Exception):
    def __init__(self,args=None):
        self.args = args

class PBS(list):
    '''
    This is a module to interact with the Torque queue system to
    find jobs and manipulate them.

    Usage for pbs running on local machine:
    torque = Torque()

    Usage for pbs running on remote machine as a user:
    torque = Torque('jkitchin@beowulf.cheme.cmu.edu')

    '''
    def __init__(self,
                 host=None):
        list.__init__(self)

        self.host = host
        #if host == None:
        #    self.host = 'gilgamesh.cheme.cmu.edu'
        #else:
        #    self.host = host

        #use an env var to set verbosity level of outputting information
        self.verbosity = int(os.environ.get('PBS_VERBOSITY',0))

        #default options for qsub
        self.qsub_mem = 750*1024 #in kb
        self.qsub_cput = (7*24,0,0) #(hours,minutes,seconds)
        self.qsub_opts = '-j oe'

        #make sure pbs responds a little
        status,output = self.getstatusoutput('qstat -B')
        if status == 0:
            output = output.split('\n')
            fields = output[2].split()
            self.server = fields[0]
        else:
            print output
            raise Exception, "qstat -B did not respond"

    def __str__(self):
        s = ''
        for job in self:
            s += str(job) + '\n'
        return s

    def getstatusoutput(self,cmd):
        ''' run command either locally or remotely and get output and status '''
        if self.host is None:
            status,output = commands.getstatusoutput(cmd)
        else:
            status,output = ssh(cmd,self.host)

        return status,output

    def qmgr(self,*cmds):
        'run commands through qmgr via python'
        for cmd in cmds:
            status,output = self.getstatusoutput('qmgr -c "%s"' % cmd)
            if status is not None:
                print output

    def qmove(self,destination,*jobids):

        for jobid in jobids:
            status,output = self.getstatusoutput('qmove %s %s' % (destination,jobid))
            if status is not None:
                print output

    def qenable(self,*queue):
        '''
        enable queues on beowulf
        '''

        if len(queue) == 0:
            queue = ['@%s' % self.server]

        print queue
        cmd = 'qenable %s' % string.join(queue,' ')
        status,output = self.getstatusoutput(cmd)
        return status,output


    def qdisable(self,*queue):
        '''
        disable queues on beowulf
        '''

        if len(queue) == 0:
            queue = ['@%s' % self.server]

        print queue
        cmd = 'qdisable %s' % string.join(queue,' ')
        status,output = self.getstatusoutput(cmd)
        return status,output


    def qsuspend(self,*queue):
        '''
        suspend queues on beowulf

        '''

        if len(queue) == 0:
            queue = ['@%s' % self.server]

        print queue
        cmd = 'qstop %s' % string.join(queue,' ')
        status,output = self.getstatusoutput(cmd)
        return status,output

    def qstart(self,*queue):
        ''' set queues to running state
        '''

        if len(queue) == 0:
            queue = ['@%s' % self.server]

        cmd = 'qstart %s' % string.join(queue,' ')
        status,output = self.getstatusoutput(cmd)
        return (status,output)

    def ruptime(self):
        '''
        not a pbs command, but ruptime is used
        to check the status of the cluster.
        typical output is:
        c3n14       down  28+07:20
        c3n15         up   4+01:07,     0 users,  load 2.00, 2.00, 1.92
        '''
        status,getrup = self.getstatusoutput('ruptime')
        print status,getrup
        if getrup.strip() == 'ruptime: no hosts in /var/spool/rwho':
            print getrup
            return

        nodes = []
        for line in getrup.split('\n'):

            node = {}

            fields = line.split(',')
            if len(fields) == 5:
                name,status,uptime = fields[0].split()
                node['name'] = name
                node['status'] = status
                node['uptime'] = uptime

                nusers,users = fields[1].split()
                node['nusers'] = nusers
                load,oneload = fields[2].split()
                oneload = float(oneload)
                fiveload = float(fields[3])
                fifteenload = float(fields[4])
                node['load'] = (oneload,fiveload,fifteenload)

            elif len(fields) == 1:
                name,status,uptime = fields[0].split()
                node['name'] = name
                node['status'] = status
                node['uptime'] = uptime
            else:
                print 'Can not interpret ruptime output'
                print line

            nodes.append(node)

        return nodes

    def pbsnodes(self,arg=None,*nodelist):
        ''' offers some of pbsnodes functionalities

        arg is a character string:
        '-a'  returns all the nodes
        '-c' Clear OFFLINE or DOWN from  listed  nodes.   The  listed
            nodes are "free" to be allocated to jobs.
        '-d' mark nodes as down and unavailable
        '-l' list nodes marked
        '-o' mark nodes as offline
        '-r' clear OFFLINE from the nodes

        '''

        if len(nodelist) > 0 and arg is None:
            for node in nodelist:
                status,output = self.getstatusoutput('pbsnodes %s' % node)
        elif len(nodelist) > 0 and arg is not None:
            for node in nodelist:
                status,output = self.getstatusoutput('pbsnodes %s %s' % (arg,node))
        elif arg is not None:
            status,output = self.getstatusoutput('pbsnodes %s' % arg)
        else:
            status,output = self.getstatusoutput('pbsnodes')

        return status,output

    def parse_nodes(self):
        ''' parse the output of pbsnodes -a'''
        Nodelist = []

        status,output = self.getstatusoutput('pbsnodes -a')
        output = output.split('\n')

        hostre = re.compile('^\c')
        for line in output:
            #only host lines start with a character
            if  hostre.match(line):
                node = {}
                continue

            if line == '':
                Nodelist.append(node)

            line.strip()
            if line is not '':
                tag,value = line.split('=')
                node[tag.strip()] = value.strip()

        return Nodelist

    def poll(self):
        '''
        update jobs in queue. this parses the output of qstat -f, and
        it takes a pretty long time.
        '''
        del self[:]

        status,output = self.getstatusoutput('qstat -f')

        output = output.split('\n')
        if output == ['']:
            return

        # lines that start a tag = value match this regexp
        linere = re.compile('^    [a-zA-Z]')
        # continued lines have more spaces in them
        linecont = re.compile('^        ')
        jobidre = re.compile('^Job Id:')

        for line in output:
            if line == '':
                self.append(job)

            # now get detailed information for each job
            if jobidre.search(line):
                job = {}
                tag,value = line.split(':')
                job[tag.strip()]= value.strip()

            if linere.search(line):
                #we have matched that the line starts a tag = value
                # split the line with ' = ' to distinguish it from '='
                # which also occurs frequently in the values
                tag,value = line.split(' = ')
                tag=tag.strip()
                value=value.strip()
                job[tag] = value
            elif linecont.search(line):
                #a couple of tags have multiline values
                # they don't match the regexp above, so i
                # just add these lines to the tag
                job[tag] += line.strip()

    def threadpoll(self):

        ''' Threading example'''
        # this could be faster but I can't qet it to complete

        status,output = self.getstatusoutput('qselect')

        output = output.split('\n')

        joblist = []
        for jobid in output:
            j = job(jobid,self.host)
            joblist.append(j)
            j.start()

        # delete all jobs currently held
        del self[:]

        for j in joblist:
            print j.jobid
            j.join()
            self.append(j.job)


    def fastpoll(self):
        '''refresh the list of jobs'''

        status, output = self.getstatusoutput('qstat')
        if status is not 0:
            raise Exception, 'qstat does not work on this host'

        output = output.split('\n')
        #remove all entries currently stored
        del self[:]
        # now parse output
        for line in output[3:]:
            fields = line.split()
            if len(fields) == 6:

                (jobid, name, user, timeuse, status, queue) = fields

                job = {}
                job['Job Id'] = jobid
                job['Job_Name'] = name
                job['euser'] = user
                job['resources_used.cput'] = timeuse
                job['queue'] = queue
                job['job_state'] = status

                self.append(job)

    def qdel(self,*jobids):
        '''
        kill a job by its jobid
        '''

        status,output = self.getstatusoutput('qdel %s' % string.join(jobids,' '))

        return (status,output)

    def qdelp(self,*jobids):
        '''
        kill a job by its jobid
        '''

        status,output = self.getstatusoutput('qdel -p %s' % string.join(jobids,' '))

        return (status,output)

    def qhold(self,hold_list,*jobids):
        '''
        The  qhold  command requests that a server place one or more holds on a
        job.  A job that has a hold is not eligible for  execution.

        -h hold_list   Defines the types of holds to be placed on the job.

        The hold_list argument is a string consisting of one  or
        more  of the letters "", "", or "" in any combination or
        the character "n" or "p".  The hold type associated with
        each letter is:

            u - USER

            o - OTHER

            s - SYSTEM

            n - None

            p - Bad password
        '''

        for jobid in jobids:
            status,output = self.getstatusoutput('qhold -h "%s" %s' % (hold_list,
                                                                       jobid))

            if status is not 0:
                print output


    def qrls(self,hold_list='uos',*jobids):
        '''
        The qrls command removes or releases holds which exist on batch jobs.

        -h hold_list   Defines  the types of hold to be released from the jobs.
        The hold_list option argument is a string consisting  of
        one or more of the letters "", "", an "" in any combina-
        tion, or one or more of the letters "" or "p".  The hold
        type associated with each letter is:

        u - USER

        o - OTHER

        s - SYSTEM

        n - None

        p - Bad password
        '''

        for jobid in jobids:
            status,output = self.getstatusoutput('qrls -h "%s" %s' % (hold_list,
                                                                      jobid))

            if status is not 0:
                print output


    def qsig(self,signal='suspend',*jobids):
        '''
        The  signal  argument  is  either  a  signal  name, e.g.
        SIGKILL, the signal name without the  SIG  prefix,  e.g.
        KILL,  or  a unsigned signal number, e.g. 9.  The signal
        name SIGNULL is allowed; the server will send the signal
        0  to the job which will have no effect.  Not all signal
        names will be recognized by qsig signal name, try  issu-
        ing the signal number instead.

        Two special signal names, "suspend" and "resume", [note,
        all lower case], are used to suspend  and  resume  jobs.
        When   suspended,  a  job  continues  to  occupy  system
        resources but is not executing and is  not  charged  for
        walltime.   Manager or operator privilege is required to
        suspend or resume a job.
        '''


        status,output = self.getstatusoutput('qsig -s %s %s' % (signal,string.join(jobids,' ')))
        return (status,output)

    def qalter(self,attributes,*jobids):
        '''
        see qalter

        If a job is running, the  only  resources  that  can  be  modified  are
        cputime and walltime.  These can only be reduced.

        If  a  job is queued, requested modifications must still fit within the
        queue's and server's job resource limits.  If a requested  modification
        to a resource would exceed the queue's or server's job resource limits,
        the resource request will be rejected.

        qalter  [-a  date_time] [-A account_string] [-c interval] [-e path] [-h
        hold_list] [-j join] [-k keep] [-l resource_list] [-m mail_options] [-M
        user_list]  [-N  name]  [-o path] [-p priority] [-q destination] [-r c]
        [-S path] [-u user_list] [-W additional_attributes] job_identifier_list



        pbs.alter('-l cput=100:00:00,mem=670mb', '145456.beowulf')
        '''


        status,output = self.getstatusoutput('qalter %s %s' % (attributes,
                                                               string.join(jobids,' ')))
        return (status,output)

    def findjobs(self,**kwargs):
        '''
        pbs.findjobs(user='jkitchin')

        you can search for any field name from qstat -f

        '''

        foundjobs = []
        #now look through all the jobs
        for job in self:
            #for each job, make sure all the keys match
            # assume it matches
            match = True
            for key in kwargs.keys():
                if job.get(key,None) != kwargs[key]:
                    match = False
            #now if you get here and match = true, then it is a job you
            #are looking for.
            if match:
                foundjobs.append(job)

        return foundjobs

    def findnodes(self):
        '''
        returns a list of nodes. each node is a dictionary with all
        the properties in it.

        here is a typical output for a node
        {'resources_available.ncpus': '4',
        'jobs': '145126.beowulf/0, 145127.beowulf/1, 147353.beowulf/2',
        'resources_assigned.mem': '3993600kb',
        'license': 'l',
        'resv_enable': 'True',
        'resources_available.mem': '4150308kb',
        'ntype': 'PBS',
        'state': 'free',
        'no_multinode_jobs': 'True',
        'resources_available.host': 'c4n19',
        'Host': 'c4n19',
        'resources_assigned.ncpus': '3',
        'pcpus': '4',
        'Port': '15002',
        'resources_available.arch': 'linux'}

        '''
        Nodelist = []

        status,output = self.getstatusoutput('pbsnodes')
        if status is 0:
            print output
##        for node in NODES:

##            status,output = self.getstatusoutput('pbsnodes %s' % node)
##            if status is 0:
##                output = output.split('\n')
##                host = output[0].strip()
##                node = {}
##                for line in output[1:]:
##                    line.strip()
##                    if line is not '':
##                        tag,value = line.split('=')
##                        node[tag.strip()]=value.strip()

##                Nodelist.append(node)

##        return Nodelist

    def qsub(self,
             jobfiles,
             QSUB=None,
             remotedir=None):

        '''
        jobfiles is an iterable that contains all the files needed for the job.
        The job script is jobfiles[0]

        remotedir is the directory to run the job in on the remote server.
        this should be a unique directory for this job because the src files
        will be copied into it,
        and allthe files in it copied back. AND the remotedir will be deleted
        after the job is done

        server is assumed to be the same place as the PBS server host

        QSUB is the command to use to submit the job, including
        options. If it is None, the command is constructed.

        this function is designed to raise Exceptions unless the job is
        finished without
        PBS errors. Then it returns True!
        '''

        jobfile = jobfiles[0]
        rcfile = jobfile + '.rc'

        if QSUB is None:
            QSUB = 'qsub -j oe -l cput=24:00:00,mem=499mb'

        server = self.host

        jobdonefile = jobfile + '.done'
        if os.path.exists(jobdonefile):
            raise JobDone, 'That job is done. delete %s to resubmit it.' % jobdonefile

        remotedirfile = jobfile + '.remotedir'

        if remotedir is None and os.path.exists(remotedirfile):
            remotedir = open(remotedirfile,'r').readline().strip()
        elif remotedir is None:
            import tempfile
            i,tmpdirname = tempfile.mkstemp(dir='.')
            path,tmpdirname = os.path.split(tmpdirname)

            remotedir = 'tmp/%s' % tmpdirname

            #mkstemp actually makes the file, so we delete it
            os.close(i)
            os.unlink(tmpdirname)
        else:
            remotedir = remotedir #arg passed into function

        f = open(remotedirfile,'w')
        f.write(remotedir)
        f.close()

        pushbackfile = jobfile + '.pushback'
        pullbackfile = jobfile + '.pullback'
        qsubfile = jobfile + '.qsubcmd'

        #see if job has been submitted before
        jobid_file = jobfile + '.jobid'
        if os.path.exists(jobid_file):
            f = open(jobid_file,'r')
            jobid = f.readline().strip()
            f.close()

            self.fastpoll()
            # see if job is in the queue still
            for job in self:
                if job['Job Id'] == jobid:

                    if job['job_state'] == 'Q':
                        if self.verbosity > 1:
                            print '%s still in the queue' % jobid
                        raise JobInQueue, '%s still in the queue' % jobid
                    elif job['job_state'] == 'R':
                        if self.verbosity > 1:
                            print '%s is running' % jobid
                        raise JobRunning, '%s is running' % jobid
                    elif job['job_state'] == 'H':
                        raise JobHold, '%s is in Hold status' % jobid
                    elif job['job_state'] == 'E':
                        raise JobErrorStatus, '%s is in Error status' % jobid
                    elif job['job_state'] == 'C':
                        print '%s is done' % jobid
                    else:
                        raise UnknownJobStatus, '%s is in unknown state: %s'% (jobid,
                                                                               job['job_state'])

            if self.verbosity > 1:
                print '%s is not in the queue anymore' % jobid

            # if you get here, it was not in the queue anymore
            # now we need to copy the results back
            src = '%s:%s/' % (server,remotedir)

            if self.verbosity > 1:
                print 'copying back remote results: ',src

            status,output = rsync(src,'.')

            #we have made it this far, we should now remove the remote
            #directory.
            if self.verbosity > 1:
                print 'removing remote directory: ',remotedir
            cmd = 'rm -fr  %s' % (remotedir)
            status, output = ssh(cmd,server)
            if self.verbosity > 1:
                print 'removing remote directory status: ',status

            #now remove some files we don't need anymore
            os.unlink(jobid_file)

            nodefile = 'pbs.%s.nodes' % jobid
            if os.path.exists(nodefile):
                os.unlink(nodefile)

            if os.path.exists(pushbackfile):
                os.unlink(pushbackfile)

            if os.path.exists(pullbackfile):
                os.unlink(pullbackfile)

            if os.path.exists(remotedirfile):
                os.unlink(remotedirfile)

            if os.path.exists(qsubfile):
                os.unlink(qsubfile)

            # now lets try to check for batch errors like memory
            # exceeded or cput exceeded
            jobnumber, host = jobid.split('.')
            joboutputfile = jobfile[0:15] + '.o%s' % jobnumber

            #this may not exist if user killed job before it started
            #I also assume here that output and error have been joined
            if os.path.exists(joboutputfile):
                f = open(joboutputfile,'r')

                #now lets hunt for errors in the output file
                for line in f:

                    if '=>> PBS: job killed: mem' in line:
                        raise PBS_MemoryExceeded, line

                    elif '=>> PBS: job killed: cput' in line:
                        raise PBS_CputExceeded,line

                    elif 'Terminated' in line:
                        raise PBS_Terminated,line

                    elif '=>> PBS:' in line:
                        raise PBS_UknownError, line

                    elif 'ERROR: LAM/MPI' in line:
                        for line2 in f:
                            if 'ERROR' in line2:
                                print line2
                            if 'ssh' in line2:
                                print line2
                        raise LAMMPI_Error, line

                    elif 'forrtl' in line or 'SIGSEGV' in line:
                        raise FORTRAN_Error, line

                f.close()

            return True

        if os.environ.get('PBS_DRYRUN',None) is not None:
            print 'Dry run detected. exiting'
            return

        # this job needs to be submitted if you get here
        if self.verbosity > 1:
            print 'Submitting job:'
        destination = '%s:%s' % (server,remotedir)

        #make sure destination directory exists
        status,output = ssh('mkdir -p %s' % remotedir,server)

        #1 copy files to remote system
        status,output = rsync(jobfiles,destination)

        #2 submit job
        cmds = ['cd %s' % remotedir,
                '%s %s' % (QSUB, jobfile)]

        cmd = string.join(cmds,'; ')

        status,output = ssh(cmd,server)
        if status is not 0:
            print '==================================='
            print output
            print '==================================='
            raise PBS_UnknownError

        #we should save the jobid
        f = open(jobid_file,'w')
        f.write(output)
        f.close()

        #copy jobid file to remotedir so we can tell on that end
        #what this temp dir is for.
        rsync(jobid_file,destination)

        #get user and hostname to copy results back to
        import platform
        uname = platform.uname()
        hostname = uname[1]
        status,user = commands.getstatusoutput('whoami')

        f = open(qsubfile,'w')
        f.write('%s %s\n' % (QSUB, jobfile))
        f.close()


        f = open(pullbackfile,'w')
        f.write('#!/bin/tcsh -x\n')
        f.write('rsync -avz %s:%s/ .\n' % (self.host,remotedir))
        f.write('ssh %s@%s rm -fr %s\n' % (user, self.host, remotedir))
        f.write('#end')
        f.close()
        os.chmod(pullbackfile,0755)


        f = open(pushbackfile,'w')
        f.write('#!/bin/tcsh -x\n')
        f.write('rsync -avz . %s@%s:%s\n' % (user,hostname,os.getcwd()))
        f.close()
        os.chmod(pushbackfile,0755)
        rsync(pushbackfile,destination)

        raise JobSubmitted,output




if __name__ == '__main__':


    pbs = PBS()
    #pbs.fastpoll()
    #print pbs.ruptime()
    print pbs.pbsnodes()

#    pbs.qsig('suspend','149131', '149402')

#    pbs.fastpoll()

#    s,p = pbs.pbsnodes('-a')
#    print p

    nodes = pbs.findnodes()
    print nodes

#    jobs = pbs.findjobs(job_state='H')

#    jobs = pbs.findjobs(euser='jkitchin')
 #   for job in jobs:
  #      print job['Job Id']



    #for job in pbs:
    #    if 'c1n' in job.get('exec_host',''):
    #        print job['Job Id'], job['exec_host']





##     print pbs.kill(jobid)

##     for job in pbs.FindJobs(user='jkitchin'):
##         print job
