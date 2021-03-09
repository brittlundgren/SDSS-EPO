'''
Dr. Britt Lundgren
Austin Shank

Created -      06 Dec 2020
Last updated - 24 Feb 2021

--- --- --- --- --- --- --- ---
Panoptes documentation:
https://panoptes-python-client.readthedocs.io/en/v1.1/index.html
https://panoptes-python-client.readthedocs.io/_/downloads/en/latest/pdf/
https://www.zooniverse.org/talk/18/737080?comment=1224201&page=1

--- --- --- --- --- --- --- ---
For easy access:
I AM DELETING THIS PROJECT
'''

''' ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
IMPORTS
'''
import os, sys, platform

os.system('pip install --upgrade numpy')

import gc
import multiprocessing  # for multiprocessing parallelism
from multiprocessing import Pool
multiprocessing.set_start_method('fork', force=True)
coreCount = 1
print(platform.system())
if(platform.system() == 'Linux'):
    coreCount = multiprocessing.cpu_count()
print(str(coreCount) + ' available logical processors')

from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
import glob
import csv

try:
    import panoptes_client
except ModuleNotFoundError as e:
    print('panoptes_client not found, installing package.')
    os.system('pip install panoptes-client')
    import panoptes_client    
from panoptes_client import Panoptes, Project, SubjectSet, Subject
from panoptes_client.panoptes import PanoptesAPIException

from requests.exceptions import ConnectionError

# SciServer libraries
try:
    import astropy
except ModuleNotFoundError as e:
    print('astropy not found, installing package.')
    os.system('pip install astropy')
    import astropy
from astropy import units as u
from astropy.io import ascii, fits
from astropy.table import Table, unique, Column
try:
    import astroquery
except ModuleNotFoundError as e:
    print('astroquery not found, installing package.')
    os.system('pip install astroquery')
    import astroquery
#from astroquery.sdss import SDSS

import random
import numpy as np
from numpy import median, std
from imageio import imwrite 
import pandas
from math import sqrt
import pylab as pl
from pylab import *
import matplotlib.pyplot as plt 
import matplotlib.colors
from matplotlib.colors import colorConverter
import matplotlib.transforms as mtransforms
import matplotlib.ticker as mticker
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
import matplotlib.pylab as pylab
params = {'xtick.labelsize':'xx-large', 'ytick.labelsize':'xx-large'}
pylab.rcParams.update(params)

from shutil import copyfile
import socket
import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout

from datetime import datetime
import time
import sys

stdout = sys.stdout
matplotlib.use('agg')

print('Imports complete')

def getNow():
    return str(datetime.now())

def sampleFunction(corei, totalCores, spots):
    print('Here we go')
    for s in range(corei, len(spots), totalCores):
        time.sleep(1)
        print('nice say {} - spot {}'.format(corei, s))

def plot_box(someX, gradelab, xVals):
    # overplot shaded box
    x1 = 374.
    x2 = 2700.
    dx = x2-x1
    xcent = 0
    ycent = 0
    if someX<xVals[0][1]:
        xcent = x1+dx*((float(someX)-xVals[0][0])/(xVals[0][1]-xVals[0][0]))
        ycent = (715.+245.)/2.
    elif someX < xVals[1][1]:
        xcent = x1+dx*((int(someX)-xVals[1][0])/(xVals[1][1]-xVals[1][0]))
        ycent = (1387.+857.)/2.
    else:
        xcent = x1+dx*((int(someX)-xVals[2][0])/(xVals[2][1]-xVals[2][0]))
        ycent = (2026.+1502.)/2.
    #pl.gca().add_patch(Rectangle((xcent-75, ycent-350), 200, 600, facecolor="red", alpha=0.2))
    #pl.text(xcent+15, ycent-150, gradelab, color='red')
    # print "plotting coords:"
    return xcent, ycent
    
''' ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

panoptesConnect - 
Used to connect to a Zooniverse account via username and password using the Panoptes API.

Parameters - 
    user :: String username for account connection.
    pw :: String password for account connection.

Returns - 
    0 :: If we make it through the connection fine, returns zero, else we get an error.

Sample - 
...

--- --- --- --- 
'''
def panoptesConnect(user, pw):
    
    return Panoptes.connect(username=user, password=pw)


''' ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

createZooniverseProject - 
Used to create basic Zooniverse projects using the Panoptes API.

Parameters - 
    projName :: String name of the project.
    projDesc :: String description of the project.
    primLang :: String primary language of the project. For english as primary language, use "en".
    flag_hidden :: Boolean flag for creating public/private project. For private project, use True.

Returns - 
    project :: Zooniverse project we have just created.

Sample - 
createZooniverseProject('sample', 'this is a sample desc', 'en', true)

--- --- --- --- 
'''
def createZooniverseProject(projName, projDesc, primLang, flag_hidden):
    
    print('--- --- --- ---')
    print('Establishing connection to Zooniverse and creating project')
    
    notSaved = True
    saveCheck = 0
    project = None
    connected = False
    
    while not connected:
        url = 'http://zooniverse.org/'
        print('Attempting connection.')
        try:
            response = requests.get(url, timeout=0.2)
        except ConnectionError as ce:
            print(ce)
        except HTTPError as he:
            print(he)
        except Timeout as to:
            print(to)
        else:
            print(response)
            connected = True
    
    while(notSaved and (saveCheck < 5)):
        notSaved = False
        #Make a new project
        project = Project()
        
        #Project name
        #tutorial_project.display_name = ('{}_test'.format(now))
        project.display_name = projName
        saveCheck += 1

        #Project description
        project.description = projDesc

        #Project language
        project.primary_language = primLang
        
        #Project visibility
        project.private = flag_hidden       
                   
        try:
            project.save()
        except PanoptesAPIException as e:
            print('!!! {} , Waiting 10 seconds...'.format(e))
            notSaved = True
            for i in range(0, 10):
                print('... Waiting {}...'.format(i))
                time.sleep(3)
            project.delete()
            saveCheck += 1
            
    print('Project successfully created.')
    
    return project

''' ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

createSubjectSet - 
Used to create subject sets for Zooniverse projects using the Panoptes API.

Parameters - 
    projName :: String name of the project.
    projDesc :: String description of the project.
    primLang :: String primary language of the project. For english as primary language, use "en".
    flag_hidden :: Boolean flag for creating public/private project. For private project, use True.

Returns - 
    subjectSet :: Subject set array to have metadata and subjects added later.

Sample - 
createZooniverseProject('sample', 'this is a sample desc', 'en', true)

--- --- --- --- 
'''
def createSubjectSet(subjName, project):
    
    #Create the subject set
    subjectSet = SubjectSet()
    
    #Link to the appropriate project
    subjectSet.links.project = project
    
    #Set display name of subject set
    subjectSet.display_name = subjName
    
    #Save subject set to the project
    subjectSet.save()
    
    return subjectSet

def deleteFiles(flag_delete, subjTo):
    if(flag_delete):
        deleteThese = glob.glob(subjTo + '/*')    
        for dt in deleteThese:
            #print(dt)
            deleteFiles = glob.glob(dt + '/*')
            for df in deleteFiles:
                #print(df)
                if os.path.exists(df):
                    #print('delete file')
                    os.remove(df)
                else:
                    print("The file does not exist")
            if os.path.exists(dt):
                #print('delete dir')
                os.rmdir(dt)
            else:
                print("The directory does not exist")                
        os.rmdir(subjTo)
    return 0
        
def makeMetadataFile(metadata, newDirStr):
    
    metadataFile = open((newDirStr + '/metadata.txt'), 'w+')
    for key in metadata:
        writeLine = str(key) + ',' + str(metadata[key]) + '\n'
        metadataFile.write(writeLine)
    metadataFile.close()
    
    return 0
    
def unwrapMetadataFile(f):
    
    metadataFile = open(f, 'r')
    
    if f[-5:] == '.txt':
        for line in metadataFile:
            lineSplit = line.split(delim=',')
            metadata[lineSplit[0]] = lineSplit[1]   
        
    return metadata
    
def pushSubject(subjectSet, project, imageLocations, metadata, livePost):
    
    if(livePost):
        subject = Subject()
        subject.links.project = project
        
        for image in imageLocations:
            subject.add_location(image)
            
        subject.metadata.update(metadata)
        
        notSaved = True
        while(notSaved):
            notSaved = False       
            try:
                subject.save()
            except ConnectionError as e:
                print('{} , TRYING AGAIN'.format(e))
                notSaved = True
            
        subjectSet.add(subject) 
        
        return subject
    
    else:
        return None
       
def zooniverseTutorial(args, fs):
    
    mdHead = []
    mdf = glob.glob(fs+'/*.csv')[0]
    
    metadataTable = Table.read(mdf)
    for item in metadataTable:
        print(fs+'/'+item['image'])
        metadata = {}
        metadata['#index'] = str(item['index'])
        metadata['#fileName'] = str(item['image'])
        metadata['ra'] = str(item['ra'])
        metadata['dec'] = str(item['dec'])
        pushSubject(args['subjectSet'], args['project'], [fs+'/'+str(item['image'])], metadata, True)

def unwrapFunction(argsArray):
    
    #print('working on ', os.getpid())
    item = argsArray[0]
    args = argsArray[1]
    customArgs = argsArray[2]
    
    args['mainFunction'](item, args, customArgs)
    
    return 0
    
# --- --- --- --- --- --- --- ---

def parseFiber(f2, args, customArgs):
    
    args['mainFunction'](f2, args, customArgs)
    #sdssQSOAbsProject(f2, args, customArgs)
    #galexLLProject(f2, args, customArgs)

def processFiberList(coreIndex, coreCount, files, args, customArgs):
    
    newSubjects = []
    for k in range(coreIndex, len(files), coreCount):
        if(k%100 == 0):
            print('Parsing file #{} of {}'.format(k, len(files)))
        parseFiber(files[k], args, customArgs)   

def createSubjects(args, customArgs):
    
    os.mkdir(args['imageDestination'])
    
    files = []
    for loc in args['datasetLocations']:
        print('Gathering from ', loc)
        if loc[-5:] in ['.fits', '.csv']:
            items = Table.read(loc)
            files.append(items)
            '''
            hdul = fits.open(loc)
            f = hdul[1].data
            files.append(f)
            hdul.close()
            '''
        elif os.path.isdir(loc):
            files.append(glob.glob(loc + args['directoryStructure']))
        else:
            print('*Data structure not recognized.*')        
    
    print('File total - {}'.format(len(files)))
    coreCount = args['coreCount']
    processList = []
    
    time.sleep(1)
    
    '''
    for f in files:
        mapArray = []
        for item in f[:10]:
            mapArray.append([item, args, customArgs])
        p = multiprocessing.Pool(coreCount)
        p.map(unwrapFunction, mapArray)
        p.close()
        p.join()
        args['customArgsIndex'] += 1
    '''
    for f in files:
        for coreIndex in range(0, args['coreCount']):
            p = multiprocessing.Process(target=processFiberList, args=(coreIndex, coreCount, f[:30], args, customArgs))
            processList.append(p)
            p.start()      
        for p in processList:
            p.join()
        args['customArgsIndex'] += 1
        
    print('--- --- --- ---')        

def makeZooniverseProject(args, customArgs, tutorial=False):
    
    if(args['F_livePost']):
        #Connect to Zooniverse account
        connection = panoptesConnect(args['username'], args['password'])
        args['zooniverseConnection'] = connection

        #Create new project
        project = createZooniverseProject(args['projectName'], args['projectDescription'], args['primaryLanguage'], args['F_private'])
        args['project'] = project

        #Create new subject set
        subjectSet = createSubjectSet(args['subjectSetTitle'], args['project'])
        args['subjectSet'] = subjectSet
    else:
        args['project'] = None
        args['subjectSet'] = None       

    #Create new subjects and populate project with filled subject set
    if tutorial:
        zooniverseTutorial(args, args['datasetLocations'][0])
    else:
        createSubjects(args, customArgs)

    return args
    
def pushNewSubjectSet(args, customArgs, projID):
    
    args['F_livePost'] = True
    
    connection = panoptesConnect(args['username'], args['password'])
    args['zooniverseConnection'] = connection

    #Get existing project
    project = Project(projID)
    if project == None:
        print('Could not find this project')
        return None
    print(project.display_name)
    args['project'] = project

    #Create new subject set
    subjectSet = createSubjectSet(args['subjectSetTitle'], args['project'])
    args['subjectSet'] = subjectSet     

    #Create new subjects and populate project with filled subject set
    createSubjects(args, customArgs)

    return args
    
def run(username, password, projectName, dsLocations, customArgs={}, verbose=False, newProject=True, projID=-1, tutorial=False):
    
    args = {}
    
    args['username'] = username
    args['password'] = password
    args['projectName'] = projectName
    
    if type(dsLocations) == list:
        args['datasetLocations'] = dsLocations
    else:
        args['datasetLocations'] = [dsLocations]
 
    if verbose:
        print('not done')
    else:
        args['projectDescription'] = 'This project was created by ' + username + ' using the Zooniverse Tutorial script.'
        args['primaryLanguage'] = 'en'
        args['subjectSetTitle'] = 'Zooniverse Tutorial Subject Set'
        args['coreCount'] = multiprocessing.cpu_count()
        args['customArgsIndex'] = 0
        args['F_private'] = False
        args['F_livePost'] = True
    
    if newProject:
        makeZooniverseProject(args, customArgs, tutorial)
    else:
        pushNewSubjectSet(args, customArgs, projID)
    
    
