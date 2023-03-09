##############################################################################
##############################################################################
##                                                                          ##
##                              ifcrab.py                                   ##
##                                                                          ##
##            Tool for submitting jobs to ifca.                             ##                        
##############################################################################
##############################################################################
import os, sys, optparse, stat
import ROOT as r
import subprocess


##############################################################################
##############################################################################
templateIFCA = """#!/bin/bash

source /cvmfs/cms.cern.ch/cmsset_default.sh
pushd CMSSWRELEASE/src
eval `scramv1 runtime -sh`
pushd
cmsRun JOBNAME 
"""
##############################################################################
##############################################################################

##############################################################################
##############################################################################
randomTemplate = """
RandomNumberGeneratorService = cms.Service("RandomNumberGeneratorService",
    generator = cms.PSet(
        initialSeed = cms.untracked.uint32(RANDOMINPUT),
        engineName = cms.untracked.string('HepJamesRandom')
    )
)
process.RandomNumberGeneratorService.generator.initialSeed = RANDOMINPUT
"""
##############################################################################
##############################################################################

##############################################################################
##############################################################################
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
##############################################################################
##############################################################################

##############################################################################
##############################################################################
ifcrabLogo = """
 ____  _____   __  ____    ____  ____
|    ||     | /  ]|    \  /    ||    \ 
 |  | |   __|/  / |  D  )|  o  ||  o  )
 |  | |  |_ /  /  |    / |     ||     |
 |  | |   _]   \_ |    \ |  _  ||  O  |
 |  | |  | \     ||  .  \|  |  ||     |
|____||__|  \____||__|\_||__|__||_____|
                                       
"""
def logo():
    print(bcolors.HEADER + ifcrabLogo + bcolors.ENDC)
##############################################################################
##############################################################################

##############################################################################
##############################################################################
def message(idm, text):

    if idm == 'error':
        print(bcolors.FAIL + '[Error] ' + bcolors.ENDC + text)
    elif idm == 'log':
        print(bcolors.OKGREEN + '[Log] ' + bcolors.ENDC + text)
    elif idm == 'warning':
        print(bcolors.WARNING + '[Warning] ' + bcolors.ENDC + text)

##############################################################################
##############################################################################

##############################################################################
##############################################################################
def validFile(a):
    try:
        f = r.TFile(a)
        if f.IsZombie():
            f.Close()
            return False
        else:
            return True
    except:  
        return False
##############################################################################
##############################################################################


##############################################################################
##############################################################################
def prepareJob(job):

    message('log', 'Processing job: ' + job['id'])

    textTemplate = open(job['templateFile']).read()
    if not job['type'] == 'GEN':
        textTemplate = textTemplate.replace('INPUT', job['inputFile'])
    textTemplate = textTemplate.replace('OUTPUT', job['outputFile'])
    textTemplate = textTemplate.replace('NEVENTS', job['nEvents'])
   
    if job['type'] == 'GEN':
        textTemplate = textTemplate + '\n\n' + randomTemplate
        textTemplate = textTemplate.replace('RANDOMINPUT', job['id']+str(1))

    conf = open(job['conf'], 'w')
    conf.write(textTemplate)
    conf.close()

    text = templateIFCA
    text = text.replace('CMSSWRELEASE', job['cmssw'])
    text = text.replace('JOBNAME', job['conf'])
    confsh = open(job['confsh'], 'w')
    confsh.write(text)
    confsh.close()
    #message('log', 'sbatch -o ' + job['log'] + ' -e ' + job['error'] + ' --quos=' + job['queue'] + ' --partition=cloudcms ' + job['confsh'])
    #subprocess.run(['sbatch', '-o', job['log'], '-e', job['error'], '--qos='+job['queue'], '--account=cms', '--partition=cloudcms', job['confsh']])
    #subprocess.run(['sbatch', '-o', job['log'], '-e', job['error'], '--qos='+job['queue'], '--account=cms', '--partition=cloudcms', job['confsh']])
    pr = subprocess.Popen(['sbatch', '-o', job['log'], '-e', job['error'], '--qos='+job['queue'], '--account=cms', '--partition=cloudcms', job['confsh']], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pr.communicate()
##############################################################################
##############################################################################

##############################################################################
##############################################################################
if __name__ == '__main__':

    parser = optparse.OptionParser(usage='usage: %prog [options] path', version='%prog 1.0')
    parser.add_option('-c', '--conf'     , action='conf'    , type='string', dest='configuration',   default='',         help='Configuration file.')
    parser.add_option('-n', '--number'   , action='store'   , type='string', dest='eventsPerJob',    default='',         help='Number of events per job.')
    parser.add_option('-N', '--njobs'    , action='store'   , type='string', dest='numberOfJobs',    default='',         help='Number of jobs.')
    parser.add_option('-o', '--output'   , action='store'   , type='string', dest='outputDirectory', default='',         help='Output directory.')
    parser.add_option('-i', '--input'    , action='store'   , type='string', dest='inputDirectory',  default='',         help='Input directory.')
    parser.add_option('-w', '--work'     , action='store'   , type='string', dest='workDirectory',   default='',         help='Work directory.')
    parser.add_option('-l', '--logs'     , action='store'   , type='string', dest='logLocation',     default='',         help='Location of the logs.')
    parser.add_option('-r', '--release'  , action='store'   , type='string', dest='cmsswrelease',    default='',         help='Location of CMSSW_X_Y_Z')
    parser.add_option('-q', '--queue'    , action='store'   , type='string', dest='queue',           default='',         help='Queue name.')
    parser.add_option('-t', '--template' , action='store'   , type='string', dest='template',        default='',         help='Name of the template')
    parser.add_option('-T', '--type'     , action='store'   , type='string', dest='Type',            default='',         help='Type of job')
    (opts, args) = parser.parse_args()

    logo()
    ##############################################################################
    ####################### Checking validity of inputs ##########################
    ##############################################################################
    Type = opts.Type
    outputDirectory = opts.outputDirectory
    workDirectory = opts.workDirectory
    logLocation = opts.logLocation
    cmsswrelease = opts.cmsswrelease
    template = opts.template
    inputDirectory = opts.inputDirectory
    queue = opts.queue
    eventsPerJob = opts.eventsPerJob
    numberOfJobs = opts.numberOfJobs

    if opts.configuration != '' and os.path.exists(opts.configuration):
        message('log', 'The configuration will be read from ' + opts.configuration)
        for l in open(opts.configuration).readlines():
            try:
                exec(l)
            except:
                message('error', 'Error parsing configuration file ' + l)
                sys.exit()
        if not 'ifcrab_OutputDirectory' in locals():
                message('error', 'Output directory not present in configuration file.')
                sys.exit()
        if not 'ifcrab_Type' in locals():
                message('error', 'Type not present in configuration file.')
                sys.exit()
        if not 'ifcrab_workDirectory' in locals():
                message('error', 'Working directory not present in configuration file.')
                sys.exit()
        if not 'ifcrab_logLocation' in locals():
                message('error', 'Log directory not present in configuration file.')
                sys.exit()
        if not 'ifcrab_cmsswrelease' in locals():
                message('error', 'CMSSW release not present in configuration file.')
                sys.exit()
        if not 'ifcrab_template' in locals():
                message('error', 'Template not present in configuration file.')
                sys.exit()
        if not 'ifcrab_inputDirectory' in locals() and Type != 'GEN':
                message('error', 'Input directory not present in configuration file.')
                sys.exit()
        if not 'ifcrab_queue' in locals():
                message('error', 'Queue not present in configuration file.')
                sys.exit()
        if not 'ifcrab_eventsPerJob' in locals():
                message('error', 'Events per job not present in configuration file.')
                sys.exit()
        if not 'ifcrab_numberOfJobs' in locals():
                message('error', 'Number of jobs not present in configuration file.')
                sys.exit()
        Type = ifcrab_Type
        outputDirectory = ifcrab_outputDirectory
        workDirectory = ifcrab_workDirectory
        logLocation = ifcrab_logLocation
        cmsswrelease = ifcrab_cmsswrelease
        template = ifcrab_template
        if Type == 'GEN':
            inputDirectory = ''
        else:
            inputDirectory = ifcrab_inputDirectory
        queue = ifcrab_queue
        eventsPerJob = str(ifcrab_eventsPerJob)
        numberOfJobs = str(ifcrab_numberOfJobs)

    else:
        message('log', 'Taking configuration from command line')

    if opts.Type != 'GEN' and opts.Type != 'SIM' and opts.Type != 'DIGIPremix' and opts.Type != 'HLT' and opts.Type != 'RECO' and opts.Type != 'MINIAOD' and opts.Type != 'NTUPLE':
        message('error', 'Unknown kind of job')
        sys.exit()
    if not os.path.exists(opts.outputDirectory):
        message('log', 'Output directory does not exist, creating it...')
        os.mkdir(opts.outputDirectory)
    
    if not os.path.exists(opts.workDirectory):
        message('error', 'Work directory does not exist')
        sys.exit()

    if not os.path.exists(opts.logLocation):
        message('log', 'Log directory does not exist, creating it...')
        os.mkdir(opts.logLocation)
    
    if not os.path.exists(opts.cmsswrelease):
        message('error', 'CMSSW release does not exist')
        sys.exit()
    
    if not os.path.exists(opts.template):
        message('error', 'The template file does not exist')
        sys.exit()
    
    if opts.Type != 'GEN' and not os.path.exists(opts.inputDirectory):
        message('error', 'The input directory file does not exist')
        sys.exit()

    if opts.queue != 'cms_main' and opts.queue != 'cms_high':
        message('error', 'The queue does not exist')
        sys.exit()
        
    if opts.Type == 'GEN' and (int(opts.eventsPerJob) <= 50 or int(opts.eventsPerJob) > 3000):
        message('error', 'The number of events per job has to be in the range [50, 3000].')
        sys.exit()
    
    if opts.Type == 'GEN' and (int(opts.numberOfJobs) <= 0 or int(opts.numberOfJobs) > 2000):
        message('error', 'The number of jobs has to be in the range [1, 2000].')
        sys.exit()

    ##############################################################################
    ## Get the number of jobs from the number of input files if it's not GEN ##
    ##############################################################################
    listOfInputFiles = []
    nJobs = 0
    if Type != 'GEN':
        for i in os.listdir(inputDirectory):
            if i.find('.root') != -1:
                listOfInputFiles.append(inputDirectory + '/' + i)
        nJobs = len(listOfInputFiles)    
    else:
        nJobs = int(numberOfJobs)

    message('log', 'The number of jobs to produce is: ' + str(nJobs))
    ##############################################################################
    ###                     Get the list of output files                       ###
    ##############################################################################
    listOfOutputFiles = []
    nOutputFiles = 0
    for i in os.listdir(outputDirectory):
        if i.find('.root') != -1:
            if validFile(outputDirectory + '/' + i):
                listOfOutputFiles.append(i)
            else:
                os.remove(outputDirectory + '/' + i) 
    nOutputFiles = len(listOfOutputFiles)    
       
    message('log', 'The number of output files already produced is: ' + str(nOutputFiles))
    ##############################################################################
    ###                     Get the list of output files                       ###
    ##############################################################################
    if nOutputFiles == nJobs:
        message('warning', 'All the jobs are finished, please move forward')
        sys.exit()
    
    #Running on number of jobs
    for j in range(0, nJobs):
        job = {}
        job['id'] = str(j)
        job['type'] = opts.Type
        if opts.Type != 'GEN':
            job['inputFile'] = listOfInputFiles[j]
            job['nEvents'] = '-1'
        else:
            job['inputFile'] = ''
            job['nEvents'] = eventsPerJob
        
        job['templateFile'] = template
        job['cmssw'] = cmsswrelease 
        job['queue'] = queue 
        job['outputFile'] = outputDirectory + '/' + Type + '_' + str(j) + '.root'
        job['conf'] = workDirectory + '/conf_' + Type + '_' + str(j) + '.py'
        job['confsh'] = workDirectory + '/conf_' + Type + '_' + str(j) + '.sh'
        job['error'] = logLocation + '/log_' + Type + '_' + str(j) + '.err'
        job['log'] = logLocation + '/log_' + Type + '_' + str(j) + '.log'
        if not os.path.exists(job['outputFile']):
            prepareJob(job)

   
 


