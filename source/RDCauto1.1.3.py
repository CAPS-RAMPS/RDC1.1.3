#_____CHANGE LOG_____#
    #1.1.3 2019-10-xx
        #- Change reading routine to work with v9.13 RAMP firmware
        #- Modify trackers to work with v9.13 RAMP firmware

    #1.1.2 2019-05-08
        #Fixed compatibility issue w/new firmware RAMP-PPA boxes installed on new PPAs

    #1.1.1 2019-04-01
        #Updated rawFileReader to parse in data from PPAs with new firmware

    #0.1.1 2018-8-31
        #BCM information can now be parsed in
        #Reading functions are now in a separate file for better organization
        #Criteria for NO DATA flag changed, now looks at data yield/sampling period
        #Tracker-specific constants will now override general constants
        #Fixed bug in flatline detection (flatlines not reported)
        #Fixed bug in file writing (crash when parameter defined, is parsed, but not in order)
        #.py file runs from the source folder, .exe/.app files run from working directory
        #Modified working directory when running .app file under MacOS

    #v0.1.0 2018-8-8
        #Renamed to Data Cleaner (big change, I know)
        #Defaults are now read from ini file, rather than hard-coded
        #Execution parameters now stored in a self-contained class
        #Added option to process all files stored in a given directory
        #Code will read multiple ramp folders in the same directory
        #Code will automatically look for SD and server files
        #MLRs removed        #Difference between using no MLR and blank MLR
        #Partial SD files will now be concatenated
        #Code can handle duplicate dates across multiple ramp directories
        #Utilizes multiple cores for faster processing
        #Options added regarding parallel allocation and number of processes
        #Added option to remove suspicious data
        #Error flags added to gap reports
        #Code can parse both old and new line formats of PPA
        #Automatically selects correct ECHEM labels
        #Tracks parameters that are not output to the cal file
        #Attempt to correct bad years(e.g. 2065-3-1 to 2017-3-1)
        #Differentiates between no sensors and no errors for sensors
        #Tracks if sensors are disconnected/connected
        #Option to hide ephemeral error flags
        #Pulls formerly hard-coded constants from an ini file
        #Optionally tracks code performance
        #Supporting files (constants,bounds,settings, etc.) moved to subdirectories
        #Script works independently of current working directory
        #Choose cal file outputs in defaults or via format file
        #Can now read plantower info from the prototype ramp
        #Checks that sensors are pushing values 
        #Script can be converted to application without losing functionality (as far as I know)

#TO DO#
    #Estimated time to completion    #Needs more datar
    #Don't lose shit if no sensor mix entry
    #Log exceptions

import os
import datetime
import string
import sys
import time
import copy
import multiprocessing
from multiprocessing import Pool,cpu_count
from confReader import config
from rawFileReader import read

#Version
NAME="RAMP Data Cleaner"
VERSION="1.1.2"
REVISION="2019-05-08"

#Subfolders
SETTINGS="Settings"
TEMPLATES="templates"
CONSTANTS="Constants"
PERFORMANCE="Performance"
OUTPUT="Output"

#File names
TEMPLATE="template.ini"
DEPENDENCIES="dependencies.ini"
RUNFILE="run.ini"
CRITERIA="bounds.ini"
CONST="const.ini"
ECHEM="SensorMix.csv"
PERF="Performance.csv"

#Absolute Paths:
if getattr(sys,'frozen',False):
    SCRIPTPATH=sys.executable
    SCRIPTDIR=os.path.dirname(SCRIPTPATH)
    WORKDIR=SCRIPTDIR
else:
    SCRIPTPATH=os.path.abspath(__file__)
    SCRIPTDIR=os.path.dirname(SCRIPTPATH)
    WORKDIR=os.path.dirname(SCRIPTDIR)
if WORKDIR.endswith('MacOS'):
    WORKDIR=os.path.dirname(os.path.dirname(os.path.dirname(WORKDIR)))
TEMPLPATH=os.path.join(WORKDIR,SETTINGS,TEMPLATES,TEMPLATE)
DEPENDPATH=os.path.join(WORKDIR,SETTINGS,TEMPLATES,DEPENDENCIES)
RUNPATH=os.path.join(WORKDIR,SETTINGS,RUNFILE)
CRITPATH=os.path.join(WORKDIR,CONSTANTS,CRITERIA)
CONSTPATH=os.path.join(WORKDIR,CONSTANTS,CONST)
ECHEMPATH=os.path.join(WORKDIR,CONSTANTS,ECHEM)
PERFPATH=os.path.join(WORKDIR,PERFORMANCE,PERF)

#________________CLASS DECLARATIONS______________________________#

#Config File Reader


#Script Parameters:
class runParams(object):
#Contains user inputs that determine how the program will run
    def __init__(self):
        self.param=config.importDict(TEMPLPATH)
        (self.runPath,self.echemPath)=self.setDefPaths() #Path to Defaults.csv,if it exists
        self.yesterday=datetime.date.today()-datetime.timedelta(days=1)
        self.rampDict=dict() #Maps RAMP number to RAMP object
        self.rParamDict=dict() #e.g. Raw Directory:Paths, Auto Checks:Toggles
        self.echemDict=dict()
        self.writeReverseDict() #auto-Populates self.rParamDict

    def __repr__(self):
        #Uniquely represents a runParams object as a string containing
        #All of its parameters and mapped values
        sOut="" #Stores the output string
        for key in self.param:
            sOut+="\n" #Separate categories by blank lines
            for subkey in self.param[key]:
                sOut+=subkey+' : ' #Adds parameter name to output string
                member=self.param[key][subkey] #i.e. value of parameter
                if (type(member)==list or type(member)==set):
                    if key!='Output': member=sorted(member) #Sorts things like dates,ramp nums, etc.
                    strList=stringify(member) #Converts lists/sets/etc. into a string
                    sOut+=runParams.cleanLine(str(strList))+"\n" #Cleans up line before adding it
                else: sOut+=str(self.param[key][subkey])+'\n'
        return sOut

    def get(self,param):
        #Fetches a given parameter from the self.param dictionary using the reverse dictionary
        #Raises a KeyError if the entry is not there
        if param not in self.rParamDict: raise KeyError("No parameter '%s' found" %param)
        else:
            category=self.rParamDict[param]
            return self.param[category][param]

    def setDefPaths(self):
        #Imports from global variables the path to echem and runInfo files
        #Catches if these files do not exist and crash the program
        runPath=RUNPATH
        echemPath=ECHEMPATH
        if os.path.exists(runPath):
            if os.path.exists(echemPath): return (runPath,echemPath)
            else: 
                raise FileNotFoundError('Sensor Mix file was not found. Check Path:\n%s' %echemPath)
        else: raise FileNotFoundError('run info file was not found. Check Path:\n%s' %runPath)

    def writeReverseDict(self):
        for key in self.param:
            for subkey in self.param[key]: 
                self.rParamDict[subkey]=key

    def loadParams(self):
        print("\nLoading Parameters")
        self.loadEchem()
        #Separate loader that takes care of importing parameters and verifying them:
        loader=config(self.param,WORKDIR)
        dependencies=config.importDict(DEPENDPATH)
        self.param=loader.load(self.runPath,check=True,dependencies=dependencies)
        if loader.noErrors(): 
            print("Parameters Loaded:")
            print(self)
            self.consolidateRampInfo()
            print("Parameters reorganized")
        else:
            raise RuntimeError("Error(s) in loading parameters:\n%s" 
                                %(config.write.dict2str(loader.errors)))

    def loadEchem(self):
        #Loads in a dictionary mapping ramp number to echem sensor mix
            echem=open(self.echemPath,'r')
            line=echem.readline()
            while line!="":
                line=removeChars(line,{"\n"})
                line=line.split(",")
                rampNum=int(line[0])
                #Save in format ramp -> [S1,S2,S3,S4] e.g. 154->["CO","SO2","NO2","O3"]
                self.echemDict[rampNum]=line[1:]
                line=echem.readline()

    def consolidateRampInfo(self):
        #Consolidates path,echem, and output dictionaries into a dict or RAMP objects
        #self.rampDict will store the dict in format rampNo.->RAMP obj
        pathDict=config.pull.ramps.all(self.get("Raw Directory"),returnPathDict=True)
        echemDict=self.echemDict
        outDict=self.getOutDict() #Get a dictionary mapping RAMP->output
        for rampNum in self.get("Ramp Nums"):
            if rampNum in echemDict: 
                echemLine=echemDict[rampNum] #Load the sensor mix if available
                ramp=RAMP(rampNum,echemLine) #Create the RAMP object w/echem line
            else:
                error="\nECHEM info was not found for RAMP #%d" %rampNum
                solution="Update the SensorMix file and run again"
                raise KeyError("%s\n%s"%(error,solution))
            if rampNum in outDict: ramp.output=outDict[rampNum] #Add output string if available
            pathList=pathDict[rampNum] #Load the list of directories found for the RAMP
            ramp.addDirs(pathList) #Add them in
            self.rampDict[rampNum]=ramp

    def getOutDict(self):
        #Returns a dictionary mapping:
        #rampNo.->"order"->order line of output
        #and rampNo->"params"->dictionary of headers mapped to list of parameters
        if self.get("Output Format File"):
            #Load a format file if given
            return runParams.loadFormatFile(self.get("Output File Name"),self.get("Ramp Nums"))
        else:
            #Otherwise, assign the same output to every RAMP
            outDict=dict()
            output=self.param["Output"]
            runParams.writeOutput2Ramps(output,self.get("Ramp Nums"),outDict)
            return outDict

    @staticmethod
    def loadFormatFile(fileName,rampNums=None): 
        #Loads an output dictionary from a file
        restKwrd="rest" #String that specifies ramps not enumerated in the output file
        outDict=dict()
        path2File=os.path.join(WORKDIR,OUTPUT,fileName) #Construct path to the output file
        loadedDict=config.importDict(path2File) #dumbLoaded dictionary
        if type(rampNums)==set:
            #Create a set of ramps that did not have an output mapped:
            notDoneSet=copy.copy(rampNums)
            #First load in the enumerated entries:
            for rNumStr in loadedDict:
                rampSet=config.pull.ramps.nums(rNumStr)
                #Skip iteration of the loop in case ramp Set could not be loaded
                if type(rampSet)==str and rampSet.startswith("Error Parsing"): continue
                else:rampSet=rampSet&notDoneSet #Look at intersection of parsed set and selection
                if rampSet!=set():
                    #If there are ramps in format file that are also part of the selection
                    output=loadedDict[rNumStr] 
                    if output!=set() and output!=dict() and output!=None: #Ensure that a dud was not passed. 
                        #If a dud was, passed, lump the ramps with "rest"
                        #write output map to outDict
                        runParams.writeOutput2Ramps(output,rampSet,outDict) 
                        notDoneSet-=rampSet #update notDoneSet
            #Write output map to ramps that were not enumerated in the file:
            if restKwrd in loadedDict:
                output=loadedDict[restKwrd]
                runParams.writeOutput2Ramps(output,notDoneSet,outDict)
            #If no 'rest' keyword present and there are ramps that do not have an output dict:
            elif notDoneSet!=set(): 
                raise KeyError("The following RAMPs do not have an output specified:",
                                "\n%s" %str(notDoneSet))
        #If function was not given a set of ramps to look for, dumb-load as much as possible
        else:
            for rNumStr in loadedDict:
                rampSet=config.pull.ramps.nums(rNumStr)
                if type(rampSet)==str and rampSet.startswith("Error Parsing"): continue
                output=loadedDict[rNumStr]
                runParams.writeOutput2Ramps(output,rampSet,outDict)
        return outDict

    @staticmethod
    def writeOutput2Ramps(output,rampSet,outDict):
        #Populates the outDict with each ramp in the rampSet mapped as:
        #ramp->"order"-> order string of output
        #As well as ramp->"params"->dictionary of headers mapped to list of parameters
        #Doesn't return, uses aliasing on the outDict
        for ramp in rampSet:
            outDict[ramp]=  { #keeps track of order and header dictionaries as separate entries
                            "order" : None,
                            "params": dict()
                            }
            outDict[ramp]["order"]=output["Order"]
            skipHeaders={"Order","Output File Name"} #Entries which don't contain header information
            for header in output:
                if header not in skipHeaders:
                    if type(output[header])==str:
                        outDict[ramp]["params"][header]=[output[header]]
                    else:
                        outDict[ramp]["params"][header]=output[header]

    @staticmethod
    def cleanLine(s):
        toRemove={'"',"'","","\t","\n"}
        return removeChars(s,toRemove)

#RAMP properties
class RAMP(object):
    def __init__(self,num,echemLine=["S1","S2","S3","S4"]):
        self.num=num
        self.dirs=  {
                    "Server": set(),
                    "SD": set()
                    }
        self.echem=echemLine
        self.output=None

    def __eq__(self,other):
        return self.num==other.num

    def __repr__(self):
        return "RAMP "+str(self.num)

    def __str__(self):
        return str(self.num)

    def __hash__(self):
        return hash(self.num)

    def addDir(self,path):
        if path in self.dirs: return
        else: self.dirs.add(path)

    def addDirs(self,paths):
        #Check paths for server and SD, allocate appropriately to self.dirs dict
        for path in paths:
            if path.endswith("s%d"%(self.num)): self.dirs["Server"].add(path)
            else: self.dirs["SD"].add(path)

    @staticmethod
    def nums(rampSet):
        rSet=set()
        for ramp in rampSet:
            rSet.add(ramp.num)
        return rSet

#File Types:
class rampFile(object):
    def __init__(self,ramp,path):
        self.ramp=ramp
        self.path=path
        self.dir=os.path.dirname(path)
        if os.path.exists(path): self.size=self.getSize()
        else: self.size=0
        self.io=None

    def __repr__(self):
        return self.path

    def open(self, mode='r'):
        if not mode.startswith('r'):
            #Create directory if one does not exist and file is in writing mode
            if not os.path.exists(self.dir): os.makedirs(self.dir)
        self.io=open(self.path,mode)

    def close(self):
        try: 
            self.io.close()
            self.io=None #Cleans up to make object serializable
        except: pass

    def seek(self,param):
        self.io.seek(param)

    def read(self):
        return self.io.read()

    def tell(self):
        return self.io.tell()

    def getSize(self):
        return os.path.getsize(self.path)

    def readline(self):
        return self.io.readline()

    def exists(self):
        return os.path.exists(self.path)

    def write(self,s):
        self.io.write(s)

class dataFile(rampFile):
    def __init__(self,ramp,date,path):
        self.date=date
        if path!=None:
            self.path=path
            self.dir=os.path.dirname(self.path)
        elif "dir" in kwargs:
            self.dir=kwargs["dir"]
            self.path=os.path.join()
        else:
            raise KeyError("A path or a dir must be specified for a data file")
        super().__init__(ramp,self.path)

class rawFile(dataFile):
    def __init__(self,ramp,date,path,SD=False,concat=False):
        super().__init__(ramp,date,path)
        self.SD=SD
        self.concat=concat
        self.dateStr=rawFile.get.dateFormatCorrection(date,SD)
        self.fName=self.dateStr+rawFile.ext(SD=SD)
        (self.start,self.end)=(None,None)

    def __repr__(self):
        if self.SD: typeStr="SD"
        else:       typeStr="Server"
        if self.concat: cStr="Concatenated "
        else:           cStr=""
        return "%s%s file at %s" %(cStr,typeStr,self.path)

    def open(self,mode='r',updateEndPoints=False,forceUpdate=False):
        if updateEndPoints==True:
            if forceUpdate==True: self.updateEndPoints()
            elif (self.start==None or self.end==None): self.updateEndPoints()
        if mode=='r':
            self.io=open(self.path,encoding='ascii',errors='surrogateescape')
        else: super().open(mode)

    def open4Writing(self):
        #Create directory if one does not exist:
        if not os.path.exists(self.path): os.makedirs(self.dir)
        self.io=open(self.path,'w')

    def close(self,updateEndPoints=False,forceUpdate=False):
        #Closes the io stream
        #Optionally updates the start and end stamps of the file
        if updateEndPoints==True:
            if forceUpdate==True: self.updateEndPoints()
            elif (self.start==None or self.end==None): self.updateEndPoints()
        super().close()

    def updateEndPoints(self):
        self.size=self.getSize() #Updates size every time a file is closed
        try: (self.start,self.end)=rawFile.get.startEndStamps(self,openFile=True)
        except: pass

    @staticmethod
    def ext(SD=False):
        #Returns the appropriat file extension
        if SD: return ".TXT"
        else: return "-raw.txt"

    class get(object):
        @staticmethod
        def fNameFromDate(date,SD=False):
            dateStr=dateFormatCorrection(date,SD)
            return "%s%s" %(dateStr,ext(SD))

        @staticmethod
        def serverFile(ramp,date,folder):
            #Checks if the server file is in the given directory
            #Returns a rawFile object if so, None otherwise
            dateStr=rawFile.get.dateFormatCorrection(date,SD=False)
            path=os.path.join(folder,dateStr+rawFile.ext(SD=False))
            if os.path.exists(path): return rawFile(ramp,date,path=path)
            else: return None

        @staticmethod
        def sdFile(ramp,date,folder):
            #Checks if a file with that date is in a given SD directory
            #Returns a rawFile object if so, None otherwise
            file=None
            dateStr=rawFile.get.dateFormatCorrection(date,SD=True)
            path1=os.path.join(folder,'DATA',dateStr+rawFile.ext(SD=True)) #In case of .../ramp no./DATA/file
            path2=os.path.join(folder,'USB',dateStr+rawFile.ext(SD=True)) #In case of .../ramp no./USB/file
            path3=os.path.join(folder,dateStr+rawFile.ext(SD=True)) #In case of .../ramp no./file
            possiblePaths={path1,path2,path3}
            validPaths=rawFile.get.validPathSet(possiblePaths)
            if len(validPaths)==0: return None
            elif len(validPaths)==1:
                (validPath,)=validPaths #Unpack valid path into a tuple
                return rawFile(ramp,date,path=validPath,SD=True)
            else: #Concatenate into outer SD folder if needed
                return rawFile.get.bestFile(validPaths,folder)

        @staticmethod
        def validPathSet(pathList):
            #takes a list of paths, return a set of valid ones
            validSet=set()
            for path in pathList:
                if os.path.exists(path):
                    validSet.add(path)
            return validSet

        @staticmethod
        def dateFormatCorrection(date,SD=False):
            date=str(date)
            if SD:
                dtLen=6
                date=date.split('-')
                date[0]=date[0][2:] #removes the first two digits in the year
                date=''.join(date)
                date=date[:dtLen] #truncates in case datetime is given
            else:
                date=date.split('-')
                date[-1]=str(int(date[-1])) #Removes the zero in front of day (server format)
                date='-'.join(date)
            return date 

        @staticmethod
        def concatenatedPartialFiles(f1,f2,svPath):
            #Concatenates f1 and f2 into a file on save path svPath based on timestamps
            fOut=None #Variable that stores the final concatenated file
            f3SD=f1.SD and f2.SD #Format as SD file only if both f1 and f2 are SD files
            f3=rawFile(f1.ramp,f1.date,svPath,SD=f3SD,concat=True) #The concatenated file
            f1.open(updateEndPoints=True,forceUpdate=False) #Open files in case concatenation is required
            f2.open(updateEndPoints=True,forceUpdate=False)
            if f1.start and f1.end and f2.start and f2.end: #If all start and end stamps are defined
                if f1.start>=f2.end: #i.e. file 1 starts after filed 2 ends
                    f3.open('w')
                    #Default .open method for raw files is read-only 
                    rawFile.write2f3(f2,f1,f3) #Write f2, then f1 to f3
                    fOut=f3 #Set concatenated file as output
                elif f2.start>=f1.end: #i.e. file 1 ends before file 2 starts
                    f3.open('w')
                    rawFile.write2f3(f1,f2,f3) #Write f1, then f2 to f3
                    fOut=f3
                elif f2.start<=f1.start and f2.end>=f1.end: fOut=f2
                #i.e. if f2 starts earlier and ends later than f1, just use f2
                elif f2.start>=f1.start and f2.end<f1.end: fOut=f1 
                #same, but for f1

                #Check file sizes, go for the larger one
                elif f1.size>=f2.size: fOut=f1
                elif f2.size>=f1.size: fOut=f2
                elif f1.SD==True: fOut=f1
                else: fOut=f2
            elif f2.start and f2.end: fOut=f2 #IF start and end stamps are defined only for f2, use f2
            elif f1.start and f1.end: fOut=f1   
            f1.close()#Close all files that may have been open
            f2.close()
            if fOut==f3:
                f3.close(updateEndPoints=True,forceUpdate=False)
                fOut.close(updateEndPoints=True,forceUpdate=False)
            else: fOut.close()
            return fOut

        @staticmethod
        def startEndStamps(f,openFile=False):
            #Gets the first and last time stamps of a raw file. optionally uses io.
            (startStamp,endStamp)=(None,None)
            if openFile: f.open() #If told, open the file
            line=f.readline()
            while line!="": #Go through the file line by line
                dt=rawFile.get.lineDateTime(line,f.date) #Parse out date stamp from the line
                if dt:
                    if startStamp==None: startStamp=dt
                    endStamp=dt
                line=f.readline()
            f.seek(0) #Returns file parser to beginning in case file is open after return
            if openFile: f.close() #If told to open the file, close it
            return (startStamp,endStamp)

        @staticmethod
        def lineDateTime(line,date=None):
            #Gets the dateStamp from the line in a raw file using one of the read methods
            line=line.split("X")
            for elem in line:
                if elem.startswith("DATE"):
                    tStampDict=read.timeStamp(elem,date)
                    if tStampDict!=None:
                        return tStampDict["DATETIME"]
                    else: return None
            return None

        @staticmethod
        def bestFile(fSet,concatDir):
            bestFile=None
            if None in fSet: fSet.remove(None)
            if len(fSet)==0: return None
            elif len(fSet)==1: 
                (bestFile,)=fSet #unpacks the only element in the set
                return bestFile
            else:
                if len(fSet)==2: #Base case of two files being written to a third
                    (f1,f2)=fSet
                    svPath=rawFile.get.concatFilePath(f1,f2,concatDir)
                    bestFile=rawFile.get.concatenatedPartialFiles(f1,f2,svPath)
                else:
                    tempDir="temp" #Directory that temporaily stores concatenated files
                    tempDir=os.path.join(concatDir,tempDir)
                    tempDirPresent= os.path.exists(tempDir) #Figure out if tempDir was there before
                    if not tempDirPresent: os.makedirs(tempDir) #creates tempDir if wasn't there before
                    bestFile=rawFile.get.bestFileRecursively(fSet,concatDir,tempDir)
                    if not tempDirPresent: 
                        import shutil
                        shutil.rmtree(tempDir) #removes tempDir if wasn't there before
            return bestFile

        @staticmethod
        def bestFileRecursively(fSet,concatDir,tempDir,depth=0):
            if len(fSet)==0: return None
            if len(fSet)==1: #Base case for one file given
                (bestFile,)=fSet #unpacks the only element in the set
                return bestFile
            else:              
                if depth==0: svDir=concatDir
                else:        svDir=tempDir
                if len(fSet)==2: #Base case of two files being written to a third
                    (f1,f2)=fSet
                    #Decides what file format to use when saving concatenated file:
                    svPath=rawFile.get.concatFilePath(f1,f2,svDir)
                else:
                    depth+=1
                    fList=list(fSet) #Creates a list that can be split and fed back in recursively
                    listMidPt=len(fList)//2
                    #Split the list into two parts:
                    (subList1,subList2)=(fList[:listMidPt],fList[listMidPt:])
                    #Recursive calls on subLists:
                    f1=rawFile.get.bestFileRecursively(subList1,concatDir,tempDir,depth)
                    f2=rawFile.get.bestFileRecursively(subList2,concatDir,tempDir,depth)
                    svPath=rawFile.get.concatFilePath(f1,f2,svDir)
                return rawFile.get.concatenatedPartialFiles(f1,f2,svPath)

        @staticmethod
        def concatFilePath(f1,f2,svDir):
            #Determines full path to a hypothetical concatenated file
            #Using parameters of f1, f2, and a save directory
            if f1!=None and f2!=None:
                if not f1.SD: dateStr=f1.dateStr
                else: dateStr=f2.dateStr
                f3SD=f1.SD and f2.SD #Only format bestFile as SD if both f1 and f2 are
            elif f1!=None:
                f3SD=f1.SD
                dateStr=f1.dateStr
            elif f2!=None:
                f3SD=f2.SD
                dateStr=f2.dateStr
            f3Name=dateStr+rawFile.ext(SD=f3SD)
            svPath=os.path.join(svDir,f3Name)
            return svPath

    @staticmethod
    def write2f3(f1,f2,f3):
        #Writes the contents of two files into 1
        #Write the contents of f1, followed by contents of f2 to f3
        line=f1.readline()
        while line!="":
            if checkASCII(line): f3.write(line)
            line=f1.readline()
        line=f2.readline()
        while line!="":
            if checkASCII(line): f3.write(line)
            line=f2.readline()
        #Set endpoints to the same value as for f1 and f2:
        f3.start=f1.start
        f3.end=f2.end

class calFile(dataFile):
    def __init__(self,ramp,date,path):
        super().__init__(ramp,date,path)
        self.echem=ramp.echem
        self.output=ramp.output

    def writeStartLine(self):
        #Format: 
        #([Column Name 1,..., Column Name n], Delimiter in raw file)#
        (params,order)=(copy.copy(self.output['params']),copy.copy(self.output['order']))
        params["ECHEM"]=self.echem
        (params,order)=calFile.orderParams(params,order)

        self.blankLine=calFile.genBlankLine(params)
        self.order=order

        vals=flatten(params)
        apStr=('_%s'+',') %str(self.ramp)
        outStr=apStr.join(vals)
        outStr+='\n'
        self.write(outStr)

    @staticmethod
    def ext(): #file extension (after the date)
        return "-cal.txt"

    @staticmethod
    def genBlankLine(order):
        lFormat=[]
        for element in order:
            lFormat.append(','*(len(element)-1))
        return lFormat

    @staticmethod
    def orderParams(params,order=None):
    #Converts a parameter dictionary into a list sorted by the order list
    #Converts order list to a dictionary
        ordDict=dict()
        ordParams=[]
        if order==[]: return(ordParams,ordDict)
        elif order==None:
            i=0
            for elem in params.keys():
                ordParams+=[params[elem]]
                ordDict[elem]=i
                i+=1
        else:
            i=0
            for elem in order:
                ordParams+=[params[elem]]
                ordDict[elem]=i
                i+=1
        return (ordParams,ordDict)

    @staticmethod
    def create(rawFile,runInfo):
    #Creates a calFile object with the appropriate ramp, date, and path
        ramp=rawFile.ramp
        calDir=runInfo.get("Output Directory")
        rampStr="s"+str(ramp) #All processed subdirectories start with "s"
        dirStr=os.path.join(calDir,rampStr)
        date=rawFile.date
        path=os.path.join(dirStr,str(date)+calFile.ext())
        out=calFile(ramp,date,path)
        return out

class errorFile(rampFile):
    def __init__(self,ramp,directory):
        self.extension="_checks.txt"
        path=self.setPath(ramp,directory)
        super().__init__(ramp,path)

    def setPath(self,ramp,directory):
        return os.path.join(directory,str(ramp)+self.extension)

    def writeStartLine(self,raw):
        dateStr=str(raw.date)
        if raw.SD: dateStr+=" (SD)"
        dateStr=dateStr+':\n'
        self.write(dateStr)

#Error Tracker:
class errorTracker(object):
    def __init__(self,runInfo,cal,rawSize,errorFile=None):
        ramp=cal.ramp
        self.fSize=rawSize
        self.file=errorFile
        self.remove=runInfo.get("Auto Remove")
        self.dispEphErr=runInfo.get("Show Instantaneous Errors")
        self.echem=cal.echem
        self.params=set(cal.order.keys())
        self.loadCriteria()
        self.loadConst()
        self.setErrorTracking(runInfo)

    def loadCriteria(self):
        #Parses the CRITERIA file in the program's directory to determine
        #criteria for throwing error flags
        self.pSet=  {"CO2","T","RH","P","CO","SO2","NO","NO2","O3","VOC",
                    "BATT","MET","CPC","CPCFLOW","CPC_T",
                    "CPCPULSE","PM010","PM025","PM100"}
        #Parameters that the parser will look for in the criteria file
        self.bDict=dict() #stores criteria with parameters in pSet as keys
        crit=CRITPATH
        #looks for criteria file in the program's working directory
        crit=open(crit,'r')
        line=crit.readline()
        while line!="":
            while line.startswith("#"): #Skip over comments
                line=crit.readline()
            try:
                line=line.split(",")
                if line[0] in self.pSet: #If parameter in the text file is present in self.pSet
                    self.bDict[line[0]]=errorTracker.parseCritLine(line)
            except: pass
            line=crit.readline()
        crit.close()

    def loadConst(self):
        #Loads constants from a file to be later used in individual trackers
        self.const=dict()   #Stores in format: {trackerName:{parameter:value}}
        const=CONSTPATH
        try: const=open(const,'r')
        except: raise FileNotFoundError("No file in the following path: %s" %const)
        line=const.readline()
        category=None
        while line!="": #Loop that reads the file text line by line
            entry=None
            while line.startswith("#"): #Skip over comments
                line=const.readline()
            if line.startswith("[") and line.endswith("]\n"): #Indicators of category name
                category=line[1:-2] #Saves the category, chops off brackets and \n
                if category=="": category==None
                elif category not in self.const: self.const[category]=dict() #Adds category to dict
            elif category!=None and len(line)>1: 
                if line.endswith("\n"): line=line[:-1]  #Removes the \n at the end of every line
                try:
                    line=line.split("=") #parameter=value
                    parameter=line[0]
                    value=line[1]
                    if ":" in value: entry=str2TimeDelta(value) #Try to read a time in h:m:s format
                    else: #Must be either a float or an integer
                        try: 
                            if "." in value or "e" in value:
                                entry=float(value) #Implies a floating-point value
                            else: entry=int(value) #Integer by default
                        except: pass
                    if entry!=None:
                        self.const[category][parameter]=entry
                except: line=const.readline()
            line=const.readline()

    def collectCriteria(self):
    #Collects loaded data on bounds,spike, and flatline criteria into
    #tags seen in the raw file
        boundsDict=dict()
        constDict=dict()
        collectionDict= { # Maps category to which criteria it requires for error tracking
                        "DATE": [],
                        "CO2":  ["CO2","T","RH"],
                        "ECHEM":["CO","SO2","NO","NO2","O3","VOC"],
                        "TSI":  ["CPC","CPC_T","CPCPULSE"],
                        "ADI":  ["CPCFLOW"],
                        "MET":  ["MET"],
                        "BATT": ["BATT"],
                        "PPA":  ["T","RH","P","PM010","PM025","PM100"],
                        "STAT": []  
                        }
        for key in collectionDict:
            boundsDict[key]=dict()
            constDict[key]=dict() 
            for param in self.const["General"]: #Transfer general values to eaach tracker
                constDict[key][param]=self.const["General"][param]
            if key in self.const:
                #Copy tracker specific constants and
                #override general values with ones specific for the tracker
                for param in self.const[key]:
                    constDict[key][param]=self.const[key][param]
            for subkey in collectionDict[key]:
                boundsDict[key][subkey]=self.bDict[subkey]
        #   A   A   A   A
        #   |   |   |   |
        #Reorganizes criteria in the following format: 
        #Category:{parameter1:crtieria, paramter2:criteria, etc.}
        #e.g. CO2:{T:{lower:-20C,upper:50C,spike:1ug*m^-3*min^-1,flatT: 5h,0m,0s ,flatS:0.1C/hr}}
        return (boundsDict,constDict)

    def setErrorTracking(self,runInfo):
        #Initializes helper tracker objects that'll handle their own categories 
        if self.file or self.remove:
            (bDict,cDict)=self.collectCriteria()
            self.subTrackers=\
                { #Creates tracker helpers and passes them the required parameters
                "DATE":     tGapTracker(runInfo.get("Time Gap"),self.fSize,cDict["DATE"])
                ,"CO2":     valTracker(runInfo,"CO2",bDict["CO2"],cDict["CO2"])
                ,"ECHEM":   eChemTracker(runInfo,"ECHEM",bDict["ECHEM"],cDict["ECHEM"],self.echem)
                ,"MET":     metTracker(runInfo,"MET",bDict["MET"],cDict["MET"])
                ,"TSI":     tsiTracker(runInfo,"TSI",bDict["TSI"],cDict["TSI"])
                ,"ADI":     adiTracker(runInfo,"ADI",bDict["ADI"],cDict["ADI"])
                ,"PPA":     ppaTracker(runInfo,"PPA",bDict["PPA"],cDict["PPA"])
                ,"BATT":    battTracker(runInfo,"BATT",bDict["BATT"],cDict["BATT"])
                ,"STAT":    statTracker(runInfo,"STAT",bDict["STAT"],cDict["STAT"])
                }

    def push(self,param,L=None):
        #Directs date from a category to the appropriate tracker
        if param in self.subTrackers: #Could potentially improve runtime by selecting option at initialization
            time=self.subTrackers["DATE"].stamp["current"]
            dt=self.subTrackers["DATE"].stamp["change"] #Calls last change in datetime from tGapTracker
            #if dt==datetime.timedelta(0): return None #Throw out duplicate time stamps
            if self.remove:  return self.subTrackers[param].push(L,time,dt)
            else: 
                self.subTrackers[param].push(L,time,dt)
                return L
        else: return L

    def badLine(self,line): pass

    def publishReport(self):
        #Handles writing to the error checks file
        if self.file:
            dateTracker=self.subTrackers["DATE"]
            self.file.write("\t<_Begin Report_>\n")
            for key in self.subTrackers:
                self.subTrackers[key].publish(self.file,self.dispEphErr,dateTracker)
            self.file.write("\n\t<_End Report_>\n\n")

    @staticmethod
    def parseCritLine(line):
         #Line order: Upper bound, lower bound, spike slope criterion (units/minute),...
        #...,flatline time criterion (h:m:s), flatline slope criterion (units/hour)
        try: (lower,upper)=(float(line[1]),float(line[2]))
        except: (lower,upper)=(None,None)
        try: spike=float(line[3])
        except: spike=None
        try:
            spikeT=line[4].split(":")
            spikeT=[int(i) for i in spikeT]
            spikeT=datetime.timedelta(hours=spikeT[0],
                        minutes=spikeT[1],seconds=spikeT[2])
        except: spikeT=None
        try:
            flatT=line[5].split(":")
            flatT=[int(i) for i in flatT]
            flatT=datetime.timedelta(hours=flatT[0],
                        minutes=flatT[1],seconds=flatT[2])
        except: flatT=None
        try: flatS=float(line[6])
        except: flatS=None
        return {
                "lower":lower
                ,"upper":upper
                ,"spike":spike
                ,"spikeT":spikeT
                ,"flatT":flatT
                ,"flatS":flatS
                }

class valTracker(object):
    #Parent to other tracker objects (except time gap tracker)
    def __init__(self,runInfo,name,crit,const):
        self.autoChecks=runInfo.get("Auto Checks")
        self.connCrit=runInfo.get("Time Gap")
        self.name=name
        #self.decode=valTracker.getDecode(name)
        self.vals={"last": dict(), "current": dict(), "change": dict(), "output": dict()}
        self.crit=crit      #Criteria for bounds, spikes, flatlines, etc.
        self.const=const    #Constants such as disagreement criteria, time between posts, etc.
        self.flagNames=set()
        self.doNotAutoRemove=set()
        self.flat=dict()    
        self.flatDict=dict()#Stores flatline objects in a map: timeStamp -> flatLine obj
        self.eFlags=dict()  #Stores error flags as timeStamp -> error Flag
        self.ddt=dict()     #Stores a few downsampled points in memory to track noise and spikes
        self.ndFlag=dict()    #Stores the information needed for NO DATA flag
        self.ddtOn=False

    def setupddt(self):
        self.ddtOn=True
        for param in self.crit: #Set up ddtTracker for each parameter listed in the bounds file
            self.ddt[param]=ddtTracker(critT=self.crit[param]["spikeT"],
                                        critLen=self.const["ddtNumPts"])

    def push(self,val,time,dt):
        #Gives the tracker a new set of values to analyze
        if val!=None:
            self.vals["last"]=self.vals["current"] #Updates self.vals
            self.vals["current"]=val #Sets the current readings to what was pushed to the object
            self.vals["output"]=copy.deepcopy(self.vals["current"]) #Output line will be cleaned by checkAgainstCriteria()
            if self.autoChecks: self.checkConn(time,dt)
            if self.autoChecks: self.checkParsed(time,dt)
            self.timeDerivative(time,dt)
            self.checkAgainstCriteria(time,dt)
            return self.vals["output"]
        return None

    def checkConn(self,time,dt):
        #Keeps track of when the sensor has been connected and for how long, going from how
        #frequently the tracker receives pushes
        if time!=None:
            if "CONN" not in self.eFlags: 
                self.eFlags["CONN"]=[{  "start" : time,
                                        "end"   : time,
                                        "dur"   : datetime.timedelta(0),
                                        "lines" : 1
                                    }]
            elif dt!=None:
                lastConn=self.eFlags["CONN"][-1] #Reads the last element of the connection list
                if time-lastConn["end"]>self.connCrit: 
                    #Start a new conn entry if D/C time is longer than a single time gap 
                    self.eFlags["CONN"].append({
                                                "start" : time,
                                                "end"   : time,
                                                "dur"   : dt,
                                                "lines" : 1
                                                }) 
                #If a sensor has been disconnected for more than one post length, create a new connection entry
                else:
                    #Otherwise, update current connection entry
                    lastConn["end"]=time
                    lastConn["dur"]+=dt
                    lastConn["lines"]+=1

    def checkParsed(self,time,dt):
        #Adds flags based on how much data is being parsed in
        if dt!=0:
            curVals=self.vals["current"]
            #Initialize dataYield tracker if not already:
            if "General" not in self.ndFlag: self.ndFlag["General"]=dataYield(time,self.const)
            else: #Update the tracker if initialized:
                dataIn=not (curVals==None or (type(curVals)==dict and noneDict(curVals)))
                flagOut=self.ndFlag["General"].update(dt,time,data=dataIn)
                if flagOut!=None:
                    (flagName,time)=flagOut
                    self.addFlagEntry(None,flagName,time,rm=False) #Add a flag if the tracker says so
                if dataIn: #If there is data parsed in, check subcategories
                    for key in curVals:
                        #Initialized trackers for subcategories:
                        if key not in self.ndFlag: self.ndFlag[key]=dataYield(time,self.const)
                        entry=curVals[key]
                        #Determine whether there is data in the entry:
                        entryIn=not (entry==None or (type(entry)==dict and noneDict(entry)))
                        keyFlagOut=self.ndFlag[key].update(dt,time,data=entryIn)
                        if keyFlagOut!=None:
                            (flagName,time)=keyFlagOut
                            #Add a flag if the tracker say s so:
                            self.addFlagEntry(key,flagName,time,rm=False) 

    def timeDerivative(self,time,dt): 
        #Gets time derivative from last and current stamps
        last=self.vals["last"]
        current=self.vals["current"]
        if last!=None:
            for key in last:
                if (key.endswith("FLAG") or key in self.flagNames): return #Filters out error flags
                elif key not in self.ddt and self.ddtOn: self.ddt[key]=ddtTracker()
                if last[key]!=None and current[key]!=None and dt!=None:
                    change=last[key]-current[key]
                    if self.ddtOn: self.ddt[key].push(time,current[key],change,dt)
                    if dt.seconds>0: 
                        minuteChange=change*60.0/dt.seconds #Rate of change per minute
                        self.vals["change"][key]=(minuteChange,change)
                    else:
                        self.vals["change"][key]=None #In case of duplicate time stamps
                else: self.vals["change"][key]=None

    def checkAgainstCriteria(self,time,dt):
        #Decided whether values are out of bounds, spikes, error flags, or flatlines
        current=self.vals["current"] #Just to do less typing
        change=self.vals["change"]
        crit=self.crit
        for key in current:
            if key in crit:
                if key not in self.eFlags: self.eFlags[key]=dict()
                if current[key]==None: return #Skip iteration if value couldn't be read
                if ((crit[key]["lower"] and current[key]<crit[key]["lower"])
                    or (crit[key]["upper"] and current[key]>crit[key]["upper"])):
                    #If either out of bound criterion exists and is met
                    flag="OOB"
                    if key in self.doNotAutoRemove: remove=False
                    else: remove=True
                    self.addFlagEntry(key,flag,time,rm=remove)
                elif change!=dict(): #No spikes are reported during the OOB flag
                    cChange=change[key]
                    if cChange!=None: #Checks that change between two lines has been established
                        if self.ddtOn: self.trackNoiseAndSpike(key)
                        postChange=abs(change[key][1])  #change in value between two posts
                        self.trackFlatLine(key,current[key],postChange,crit[key],time,dt) 

    def trackNoiseAndSpike(self,key):
        minuteChange=self.vals["change"][key][0] #assigning variables to save on typing
        crit=self.crit[key]["spike"]
        critT=self.crit[key]["spikeT"]
        ddt=self.ddt[key]
        if crit==None: return #Skips if no spike criterion
        if not ddt.enoughData(): return #Skips noise/spike detection if insufficient data
        elif abs(minuteChange)>crit:
            if (abs(ddt.dVal*60)>crit):
            #If average derivative exceeds spike criterion and spike time criterion is met:
                #Avg. deriv. multiplied by 60 as it is output in units/s
                flag="SPIKE"
                self.addFlagEntry(key,flag,ddt.tList,False)
            elif abs(ddt.madVdt*60)>crit:
                #If the average of the abs(derivatives) is high
                flag="NOISE"
                self.addFlagEntry(key,flag,ddt.tList,False)

    def trackFlatLine(self,key,current,change,crit,time,dt):
        #Determines whether rate of change and its duration meet the criteria for a flatline
        if change==None: return #catches the case where out of bound values are removed
        if key not in self.flat: self.flat[key]=None
        if self.flat[key]==None and crit['flatS'] and crit['flatT'] and change<crit['flatS']:
            #Starts tracking the flatline if the criteria are given
            #And change between posts is less than some pre-defined value
            if key not in self.flatDict: self.flatDict[key]=set()
            #Adds parameter to flatline dictionary if not already there
            self.flat[key]=flat(current,time,time,dt,crit['flatT'],crit['flatS']) #initializes flatline as an object
        elif self.flat[key]: #Only works triggered if flatline criteria exist
            if self.flat[key].continues(current,dt):
            #if critical derivative is not exceeded, add time to flatline
                if self.flat[key].largeEnough():
                    #If flatline large enough, try to overwrite it in dictionary 
                    self.flatDict[key].discard(self.flat[key]) #Removes flatline object from set if present
                    self.flat[key].update(time,dt)
                    self.flatDict[key].add(self.flat[key])
                else: self.flat[key].update(time,dt)
            else: #Ends the flatline if critical derivative is exceeded
                self.flat[key]=None

    def checkFlag(self,time,flagName,normFlag,fDict=None):
        #Compares the error flags to a normal flag. Adds to the error dict if flag is abnormal
        #Optionally takes a dictionary of expected error flags and what they mean
        flag=self.vals["current"][flagName]
        if not(checkASCII(str(flag))) or flag==None: 
            self.vals["output"][flagName]=None #Removes non-ascii and NoneType flags
            return #Ensures that invalid flags are not compared or written
        if flag!=normFlag:
            if flagName not in self.eFlags: self.eFlags[flagName]=dict() 
            #adds the flag to the error dictionary if not already present
            if fDict!=None: #if passed a set of valid flags
                if flag in fDict: #if a flag is a valid error flag
                    fInterp=fDict[flag] #Flag interpretation from the dictionary
                    if fInterp!=None: #If a known error flag is detected, add the occurence time to a list
                        self.addFlagEntry(flagName,fInterp,time,False)
                else: self.vals["output"][flagName]=None #Removes the flag from the output, if unknown
            else:
                #creates a list of occurences of the flag if not list of valid ones is given
                self.addFlagEntry(flagName,flag,time)

    def addFlagEntry(self,key,flag,time,rm=False):
        #Adds the given flag for a given parameter to a list
        #Optionally removes the entry from the output list
        nonExcFlag={"SPIKE","NOISE"}
        #Add key and flag to error dict if not already present:
        #If key is not specified, just adds the error flag
        if key==None:
            if flag not in self.eFlags: self.eFlags[flag]=list()
            #Add stamp or stamps depending on whether a single or a list of time stamps was given
            if type(time)==list: self.eFlags[flag]+=time
            else: self.eFlags[flag].append(time)
            #Auto remove the flagged entries if they aren't spikes or noise
            if rm and (flag not in nonExcFlag): self.vals["output"]=None
        else:
            if key not in self.eFlags: self.eFlags[key]={flag:list()}
            elif flag not in self.eFlags[key]: self.eFlags[key][flag]=list()
            #Add stamp or stamps depending on whether a single or a list of time stamps was given
            if type(time)==list: self.eFlags[key][flag]+=time
            else: self.eFlags[key][flag].append(time)
            #Auto remove the flagged entries if they aren't spikes or noise
            if rm and (flag not in nonExcFlag): self.vals["output"][key]=None

    def publish(self,file,DER,dateTracker):
        #DER (boolean) Display Ephemeral Errors
        if file:
            report=self.compileReport(DER,dateTracker)
            file.write("\n\t"+self.name+':')
            file.write(report)

    def compileReport(self,DER,dateTracker):
        #DER: display ephemeral errors
        report=""
        catIndent=2 #Num(tabs) to category (e.g. CO2, T, RH, etc.)
        lineIndent=3 #Num(tabs to actual error stamp)
        self.mergeErrors()
        cStatus=valTracker.report.conn.status(dateTracker,self.eFlags,self.const,DER)
        report+=cStatus
        if self.eFlags==dict() or self.noErrors(): #Skips if no error flags found for category
            if cStatus=="\tConnected": return (report+"\tOK") 
            else: return report
        for param in self.eFlags:
            if param=="CONN": continue
            elif valTracker.noErrSubcat(self.eFlags[param]): continue 
            #Skips parameter if no error flags found for subcategory
            else:
                if type(self.eFlags[param])==dict: #For maps parameter->error->time series
                    subReport=""
                    intervals=list()
                    for error in self.eFlags[param]:
                        if error=="FLAT":
                            #Flatlines have a different format, and thus need a different parser
                            intervals+=valTracker.flatList2Intervals\
                                        (error,self.eFlags[param][error])
                        else:
                            intervals+=valTracker.dtList2Intervals\
                                                    (error,self.eFlags[param][error],DER,self.const)
                    subReport=valTracker.printIntervals(intervals,lineIndent,DER)
                    if subReport=="": continue #If nothing found after ephemeral errors are removed, skip category
                    else: report+='\n'+catIndent*"\t"+param+":"+subReport
                else: #Otherwise error is specified without parameter (for category in general)
                    intervals=valTracker.dtList2Intervals(param,self.eFlags[param],DER,self.const)
                    subReport=valTracker.printIntervals(intervals,lineIndent,DER)
                    if subReport=="": continue #If nothing found after ephemeral errors are removed, skip category
                    else: report+=catIndent*"\t"+subReport
        return report

    def mergeErrors(self):
        #merges error flags dictionary with flatline dictionary
        efSet=set(self.eFlags.keys())
        flSet=set(self.flatDict.keys())
        allParams=efSet.union(flSet) #Combined set of keys from both dictionaries
        for param in allParams:
            if param not in self.eFlags: self.eFlags[param]=dict()
            if param in self.flatDict: self.eFlags[param]["FLAT"]=self.flatDict[param]
        self.flatDict=None #clears flatDict to not take up RAM

    def noErrors(self):
        #Checks that error subdictionaries are empty
        for param in self.eFlags:
            if param=="CONN": continue #Skips the continuity parameter (not needed)
            subCat=self.eFlags[param]
            if type(subCat)==dict:
                for err in subCat:
                    if len(subCat[err])!=0: return False
            elif subCat==None: continue #Skip error entries with no time stamps in them
            elif len(subCat)!=0: return False
        return True

    class report(object):
        
        class conn(object):
            @staticmethod
            def status(dateTracker,eFlags,const,DER):
                #Determines whether sensors were connected or not. Adds nots to the report and returns it
                outReport=""
                (fStart,fEnd,gSet)=(dateTracker.stamp["start"],dateTracker.stamp["end"],
                                                                    dateTracker.gapSet)
                if "CONN" not in eFlags: outReport="\tDisconnected" #Means tracker never received a push
                else:
                    #Margin of error when comparing to end of day (defined in constants file)
                    eodMargin=const["eodMargin"] 
                    conn=eFlags["CONN"]
                    if len(conn)==1:
                        conn=conn[0]
                        (cStart,cEnd)=(conn["start"],conn["end"]) #Connection start and end times
                        if abs(fStart-cStart)<=eodMargin and abs(fEnd-cEnd)<=eodMargin:
                            outReport="\tConnected"
                        else:
                            #Prints CON and D/C messages if not near the end of day for the ramp
                            if abs(fStart-cStart)>eodMargin: 
                                outReport+="\tConnected at %s" %str(cStart)
                            if abs(fEnd-cEnd)>eodMargin: 
                                outReport+="\tDisconnected at %s" %str(cEnd)
                    elif len(conn)==0: outReport="\tDisconnected"
                    else:
                        outReport+=valTracker.report.conn.parseList(conn,const,dateTracker,DER)
                        if outReport=="":
                            #i.e. if all C and D/C stamps fall either on end of day or time gaps
                            return "\tConnected"
                return outReport

            @staticmethod
            def parseList(conn,const,dateTracker,DER):
                outReport=""
                #re-import file start and end times, as well as the set of time gaps
                (fStart,fEnd,gSet)=\
                            (dateTracker.stamp["start"],dateTracker.stamp["end"],dateTracker.gapSet)
                #how long the sensor needs to operate to be considered connected (def in const file)
                eodMargin=const["eodMargin"]
                critDCtime=const["critDCtime"]
                for i in range(len(conn)):
                    entry=conn[i]
                    #Connection start,end, duration, and number of lines:
                    (cStart,cEnd,cDur,cLines)=\
                                    (entry["start"],entry["end"],entry["dur"],entry["lines"])
                    #Check that the device has been posting for a significant amount of time: 
                    (connect,stamp)=valTracker.report.conn.connect(conn,i,fStart,fEnd,
                                                                        gSet,const,DER)
                    if connect:
                        if i==0: outReport+="\t" #First 'Connected' tabbed over once
                        else: outReport+="\t\t" #Subsequent are tabbed over twice
                        if stamp: outReport+="Connected at %s" %str(cStart)
                        else: outReport+="Connected" 
                    if valTracker.report.conn.disconnect(conn,i,fEnd,gSet,const,DER):
                            outReport+="\tDisconnected at %s" %str(cEnd)
                            #Add a new line, unless looking at final stamp:
                            if i!=len(conn)-1: outReport+="\n"
                if outReport=="": outReport+="\tIntermittent Connection"
                return outReport

            @staticmethod
            def entry(entry,const,DER):
                #Determines whether the entry is to be displayed or ignored
                return ((entry["dur"]>const["critOpTime"] and 
                        entry["lines"]>const["critOpLines"]) or DER)

            @staticmethod
            def connect(conn,i,fStart,fEnd,gSet,const,DER):
                #Check if it is appropriate to put a "Connect" flag and whether to 
                #report a time stamp with it
                (dispFlag,dispStamp)=(None,None)
                eodMargin=const["eodMargin"]
                cStart=conn[i]["start"]
                if valTracker.report.conn.entry(conn[i],const,DER):
                    #If it is the first post of the device:
                    if i==0 and abs(fStart-cStart)<eodMargin: return (True,False)
                    #If the device was connected not when a time gap ended:
                    elif cStart not in gSet: return (True,True)
                    #Display if a device was D/C before a time gap started, but reconnected
                    #When a time gap ended:
                    elif (i!=0 and 
                        valTracker.report.conn.disconnect(conn,i-1,fEnd,gSet,const,DER)):
                        return (True,True)
                return (False,False)


            @staticmethod
            def disconnect(conn,i,fEnd,gSet,const,DER):
                #Check if it is appropriate to put a "Disconnect" flag 
                eodMargin=const["eodMargin"]
                cEnd=conn[i]["end"]
                if abs(fEnd-cEnd)<eodMargin: return False #Don't display D/C at the end of day (eod)
                #Otherwise, do not display D/C for short entries unless DER is on:
                elif not valTracker.report.conn.entry(conn[i],const,DER): return False
                #Otherwise don't display if occurs at time gap, but not if it is a final entry
                elif cEnd in gSet and i!=len(conn)-1: return False  
                else: return True

    @staticmethod
    def dtList2Intervals(error,dtList,DER,const):
        if len(dtList)==0: return list()
        #Converts a list of time stamps into a list of intervals (start,end)
        tCrit=const["postLen"] #Length of one post @ 15s/line ratio=30
        tIso=const["tIso"] #Instantaneous errors this far apart from the next one get thrown out (if DER is)
        minLen=const["minErrLen"] #Throw away instances that do not recur within this interval
        #tCrit corresponds to minimum amount of error-free operation required to end an error flag
        intervals=list() #Stores time stamps as 
        start=dtList[0]
        end=dtList[0]
        if type(start)!=datetime.datetime: return intervals #reject any non-datetime array
        if len(dtList)==1:
            if DER: return [(start,end,end-start,error)] #Display singular error if DER is on
            else:   return list()
        elif len(dtList)==2:
            dt=dtList[1]-dtList[0]
            if not DER:
                if dt>minLen and dt<tCrit: return [(start,dtList[1],dt,error)]
                else: return list()
            elif dt>=tCrit: 
                return [(start,end,end-start,error),(dtList[1],dtList[1],datetime.timedelta(0),error)]
            else: return [(start,dtList[1],dt,error)]
        for i in range(1,len(dtList)-1):
            (stampL,stampM,stampH)=(dtList[i-1],dtList[i],dtList[i+1]) #Three adjacent time stamps
            (dt1,dt2)=(stampM-stampL,stampH-stampM) #delta-t between adj time stamps
            sdError=(type(error)==str and (error.startswith("SD") or error=="NO SD")) #Check of whether error is sd-related
            if (not DER) and (not sdError) and dt1>tIso and dt2>tIso: 
                (start,end)=(stampH,stampH)
                continue #Skip time stamp if isolated and not SD-related (those tend to cause skipping)
            if dt1<tCrit:
                end=stampM #extends end time if tCrit has not yet passed 
            else: #if tCrit has passed since last documented error, start new error stamp
                dur=end-start
                if DER or ((not DER) and (dur>minLen)):
                    intervals.append((start,end,dur,error))
                (start,end)=(stampM,stampM)
        dur=end-start
        if DER or ((not DER) and (dur>minLen)):
            intervals.append((start,end,dur,error)) #Loop always misses last interval
        return intervals

    @staticmethod
    def flatList2Intervals(error,flatList): 
        #Parses the list of flatlines and converts to format [(start1,end1),(start2,end2),etc.]
        intervals=list()
        for entry in flatList:
            intervals.append((entry.start,entry.end,entry.duration,error))
        return intervals

    @staticmethod
    def printIntervals(ivals,indent,DER):
        #Converts list of intervals into a report string
        #Format: start Time to end Time : error Code
        outStr=""
        indent="\n"+indent*'\t'
        ivals=sorted(ivals)
        for entry in ivals:
            (start,end,dur,errID)=(entry[0],entry[1],entry[2],entry[3])
            if dur>datetime.timedelta(0): #Only records if errors have a finite duration
                outStr+="%s%s to %s (%s) : %s" %(indent,start.time(),end.time(),dur,errID)
            #Converts start and end datetimes to times for easier reading
        return outStr

    @staticmethod
    def getDecode(name):
        #Gets output format of the sensor(s) in the cal file
        #which is the same as the list format of the input to the push() method
        (params,order)=calFile.options()
        return params[name]

    @staticmethod
    def noErrSubcat(subcat):
        #Takes a subcategory and inspects it for presense of errors
        #e.g. checks if there are flatlines, spikes, etc. in RH sensor of CO2
        if type(subcat)==dict:
            for err in subcat:
                if len(subcat[err])!=0: return False
        else:
            if len(subcat)!=0: return False
        return True

class tGapTracker(object):
    #Looks for time gaps
    def __init__(self,tGapDur,fSize,const):
        self.minGapDur=tGapDur
        self.fSize=fSize
        self.totalGapDur=datetime.timedelta(0)
        self.stamp={"last": None, "current": None,"change": None,
                    "start" : None,"end" : None}
        self.gapList=[]
        self.gapSet=set()
        self.flags=[]
        self.badStamps=0
        self.const=const

    def asessGap(self):
        #Decided whether adjacent time stamps are further apart than the time gap criterion
        lastStamp=self.stamp["last"]
        curStamp=self.stamp["current"]
        if self.minGapDur and lastStamp and curStamp:
            dt=curStamp-lastStamp
            if (dt>=self.minGapDur):
                gap=dict() 
                #stores the time gap in the form: {start:stamp1, end:stamp2, duration: stamp3}
                gap["start"]=lastStamp
                gap["end"]=curStamp
                gap["duration"]=dt
                self.gapList.append(gap)
                self.gapSet.add(lastStamp)
                self.gapSet.add(curStamp)
                self.totalGapDur+=dt #keeps track of total gap length
            self.stamp["change"]=dt

    def push(self,dtIn=None,*args):
        #Updates object based on last time stamp
        if dtIn==None: 
            self.badStamps+=1
            return None
        try: dt=dtIn['DATETIME']
        except: return None
        if self.stamp["start"]==None and dt!=None: self.stamp["start"]=dt #Set start stamp if one not set
        elif dt==None: 
            self.badStamps+=1
            return None
        self.stamp["last"]=self.stamp["current"]
        if dt:
            self.stamp["current"]=dt
            self.stamp["end"]=dt
            self.asessGap()
        return dtIn

    def publish(self,file,*args):
        #writes out the gap report
        if file:
            file.write("\tTime Gaps:")
            file.write(self.publishFlags())
            file.write("\n\t\tFile Start:\t%s" %str(self.stamp["start"]))
            file.write("\n\t\tFile End:\t%s" %str(self.stamp["end"]))
            file.write("\n\t\tGap Length:\t%s" %str(self.totalGapDur))
            file.write("\n\t\tGaps:")
            if self.gapList==list(): file.write("\t\tNone")
            else:
                for gap in self.gapList:
                    file.write(tGapTracker.publishGap(gap))

    def publishFlags(self):
        self.checkFlags()
        outStr="\n\t\tFlags:\t\t"
        if len(self.flags)==0: outStr+="None"
        else: outStr+=", ".join(self.flags)
        return outStr

    def checkFlags(self):
        bStampCrit=self.const["badStamps"] #Number of bad date stamps at which BADSTAMPS flag is thrown
        if self.stamp["start"]==None or self.stamp["end"]==None:
            self.flags.append("BADSTAMPS")
        else:
            if self.stamp["end"]-self.stamp["start"]<self.const["eodCrit"]: 
                self.flags.append("EOD")
            if self.smallFileSize():
                self.flags.append("FSIZE")
        if self.badStamps>=bStampCrit: #If large number of date stamps
            self.flags.append("BADSTAMPS")
        if self.totalGapDur>=datetime.timedelta(hours=6):
            self.flags.append("GAPS6")
        elif self.totalGapDur>=datetime.timedelta(hours=1):
            self.flags.append("GAPS1")

    def smallFileSize(self):
        typicalSize=self.const["typicalSize"] #Expected size of full day in bytes
        typicalDur=self.const["typicalDur"] #Typical duration of a day
        fileDur=(self.stamp["end"]-self.stamp["start"])-self.totalGapDur #Duration of file
        durFrac=fileDur/typicalDur #Fraction of day that the RAMP was reporting
        modSize=typicalSize*durFrac  #Expected file size factoring in the duration and time gaps
        errMargin=modSize*self.const["errMargin"]
        return self.fSize<(typicalSize-errMargin) #If file is smaller than expected within a margin

    @staticmethod
    def publishGap(gap):
        gapLine='\n\t\t'+str(gap["start"])+' to '+str(gap["end"])
        gapLine+='  -->  '+str(gap["duration"])
        return gapLine

class eChemTracker(valTracker):
    def __init__(self,runInfo,name,crit,const,echem):
        super().__init__(runInfo,name,crit,const)
        self.echem=eChemTracker.setEchem(echem)
        self.decode=echem
        self.encode=reverseDict(self.echem)
        self.setupddt()

    def push(self,val,time,dt):
        corrVal=self.decodeEchem(val)
        pushReturn=super().push(corrVal,time,dt)
        if type(pushReturn)==dict: return eChemTracker.encode(pushReturn,self.encode)
        else: return pushReturn

    @staticmethod
    def setEchem(echem):
        #Encodes a dictionary that maps sensor place to sensor type
        #e.g. S1:CO, S2:SO2, S3:NO2, etc.
        echemDict=dict()
        nSens=4 #Number of sensors
        for i in range(nSens):
            sensNum="S"+str(i+1) #Converts place 0 to S1, place 1 to S2, etc.
            echemDict[sensNum]=echem[i] #S1:echem[0]
        return echemDict

    def decodeEchem(self,val):
        #Converst S1:60, S2: 30, etc. to e.g. CO:60, SO2: 30, etc.
        newDict=dict()
        for key in val:
            corrKey=self.echem[key] #Reads map of e.g. S1:CO, S2:SO2
            newDict[corrKey]=val[key] #Applies the map
        return newDict

    @staticmethod
    def encode(D,enc):
        nD=dict()
        for key in enc:
            nD[enc[key]]=D[key]
        return nD

    def setupddt(self):
        self.ddtOn=True
        for gas in self.decode:
            self.ddt[gas]=ddtTracker(critT=self.crit[gas]["spikeT"])

class co2Tracker(valTracker):
    def __init__(self,runInfo,name,crit,const):
        super().__init__(runInfo,name,crit,const)
        super().setupddt()

class metTracker(valTracker):
    def __init__(self,runInfo,name,crit,const):
        super().__init__(runInfo,name,crit,const)
        self.setupddt()
        self.flagName="METFLAG"
        self.normFlag=0

    def setupddt(self):
        self.ddtOn=True
        self.ddt["MET"]=ddtTracker(critT=self.crit["MET"]["spikeT"])

    def push(self,val,time,dt):
        if val:
            super().push(val,time,dt)
            super().checkFlag(time,self.flagName,self.normFlag)
            return self.vals["output"]
        return None

class tsiTracker(valTracker):
    def __init__(self,runInfo,name,crit,const):
        super().__init__(runInfo,name,crit,const)
        self.setupddt()
        self.flagName="CPCFLAG"
        self.normFlag="C08"
        self.doNotAutoRemove={
                            "CPC",
                            "CPCFLOW",
                            "CPC_T",
                            "CPCPULSE"
                            }

    def setupddt(self):
        self.ddtOn=True
        self.ddt["CPC"]=ddtTracker(critT=self.crit["CPC"]["spikeT"])

    def push(self,val,time,dt):
        if val:
            super().push(val,time,dt)
            super().checkFlag(time,self.flagName,self.normFlag)
            return self.vals["output"]
        return None

class adiTracker(valTracker):
    def __init__(self,runInfo,name,crit,const):
        super().__init__(runInfo,name,crit,const)

class ppaTracker(valTracker):
    def __init__(self,runInfo,name,crit,const):
        super().__init__(runInfo,name,crit,const)
        self.setupDisagErr()
        self.setupddt()
        self.badLines=0
        self.bLCrit=const["badLineCrit"]

    def setupDisagErr(self):
        self.disagCounter=dict() #Keeps track of multiple consecutive disagreements
        self.noDisagCounter=dict() #keeps track of multiple consecutive agreements
        self.disagStamps=dict()
        self.lines2pushDisag=self.const["lines2pushDisag"]
        self.lines2stopDisag=self.const["lines2stopDisag"]
        self.PMtags={"PM010","PM025","PM100"}
        for tag in self.PMtags:
            self.eFlags[tag]={"DISAG" : list()} #Set up error flag dictionary
            self.disagCounter[tag]=0 #Start the disagreement counter for each PM reading
            self.noDisagCounter[tag]=0 #Start the end disagreement counter for each PM reading
            self.disagStamps[tag]=list() #Time stamps of disagreement errors

    def setupddt(self):
        self.ddtOn=True
        self.ddt=dict()
        for tag in self.PMtags: #ddt trackers for PM010A, PM010B, etc.
            for channel in ["A","B"]:
                param=tag+channel
                self.ddt[param]=ddtTracker(critT=self.const["ddtTrackLen"],
                                            critLen=self.const["ddtNumPts"])
        for param in self.crit: #T, RH, etc.
            if param not in (self.PMtags):
                self.ddt[param]=ddtTracker(critT=self.crit[param]["spikeT"],
                                        critLen=self.const["ddtNumPts"])

    def push(self,val,time,dt):
        pushOut=super().push(val,time,dt)
        return self.checkDisag(pushOut,time)

    def checkDisag(self,out,time):
        disagCrit=self.const["disagCrit"] #Agreement criterion between sensors
        minDisag=self.const["minDisag"] #Cutoff for disagreement
        for tag in self.PMtags:
            (Atag,Btag)=(tag+"A",tag+"B")
            (A,B)=(out[Atag],out[Btag]) #Channel readings
            #(medA,medB)=(self.ddt[Atag].mVal,self.ddt[Btag].mVal) #Running medians for channels
            if (A!=None and B!=None): #If readings are defined
                err=max((disagCrit*max(A,B)),minDisag) 
                #Choose rither % criterion if readings are large, or absolute if readings are small
                if abs(A-B)>err: #If difference between channels exceeds error criterion
                    self.noDisagCounter[tag]=0 
                    self.disagCounter[tag]+=1
                    if self.disagCounter[tag]>self.lines2pushDisag: 
                        self.eFlags[tag]["DISAG"].append(time)
                    else: self.disagStamps[tag].append(time)
                    (out[Atag],out[Btag])=(None,None)
                else: self.noDisagCounter[tag]+=1
                self.assessDisag(tag)
        return out

    def assessDisag(self,tag):
        if self.disagCounter[tag]>=self.lines2pushDisag and self.disagStamps[tag]!=list(): 
            #If enough disagreements detected and the stamp list is not empty
            self.eFlags[tag]["DISAG"]+=self.disagStamps[tag]
            self.disagStamps[tag]=list() #clear to free up RAM
        elif self.noDisagCounter[tag]>=self.lines2stopDisag:
            #If operating error-free for sufficient amount of time
            self.disagCounter[tag]=0
            self.disagStamps[tag]=list()

class battTracker(valTracker):
    def __init__(self,runInfo,name,crit,const):
        super().__init__(runInfo,name,crit,const)
        self.flagNames={"STAT"}
        self.setupddt()
        #(avg. derivative criterion for DRAIN FLAG, min. volt. crit. for DRAIN FLAG, 
        # min. volt for LOW FLAG)
        self.drainFlag="DRAIN"

    def push(self,val,time,dt):
        if val:
            super().push(val,time,dt)
            self.checkFlag(time)
            self.checkPowerLoss(time)
            return self.vals["output"]
        return None

    def checkFlag(self,time):
        #Special function to verify battery error flags 
        okFlags={"A/C","OK","BATTPWR"} #Flags which do not require an error to be dispayed
        if "STAT" not in self.eFlags: self.eFlags["STAT"]=dict() #Declare stat error dict if absent
        flag=self.vals["current"]["STAT"]
        if flag!=None and flag not in okFlags:
            if flag in self.eFlags["STAT"]: self.eFlags["STAT"][flag].append(time)
            else: self.eFlags["STAT"][flag]=[time]

    def checkPowerLoss(self,time):
        #Checks for power loss by monitoring battery voltage and its average change
        #To avoid missing power losses after long charge cycles, average change resets every so often
        if "BATT" in self.ddt:
            battDdt=self.ddt["BATT"] #Object that tracks the mean value and derivative
            if not battDdt.enoughData(): return #Skips the check if there is not enough data

            (mVolt,mdVdt)=(battDdt.mVal,battDdt.dVal) #Mean voltage and change in voltage over 20 lines
            (dvdtDrain,vDrain)=(self.const["draindVdt"],self.const["drainMinV"])
            #renames variables for less typing

            if mVolt<=self.const["lowCrit"]: #If battery is low
                if "LOW" not in self.eFlags["STAT"]: self.eFlags["STAT"]["LOW"]=list()
                self.eFlags["STAT"]["LOW"].append(time)
            elif mVolt<=vDrain and mdVdt<=dvdtDrain: #If battery is draining
                if self.drainFlag not in self.eFlags["STAT"]: 
                    self.eFlags["STAT"][self.drainFlag]=list()
                self.eFlags["STAT"][self.drainFlag].append(time)         

    def setupddt(self):
        self.ddtOn=True
        self.ddt={"BATT":ddtTracker(critT=self.const["ddtTrackLen"],
                                    critLen=self.const["ddtNumPts"])}

    @staticmethod
    def makeBattSubDict():
        #Constructs the battery error dictionary based on the facts that:
        #reading is in the form XYZ where
        #X: Temp Status – 0 if temp below -20°C, which disables charging
        #Y: Charge Status – 1 when battery voltage below 3.6V
        #Z: Fault Status – 1 when battery good; 0 if battery disconnected or fault
        outDict=dict()
        numVals=2
        for ones in range(numVals):
            for tens in range(numVals):
                for hundreds in range(numVals):
                    code=100*hundreds+10*tens+ones
                    if ones==0: outDict[code]="FAULT"
                    elif hundreds==0: outDict[code]="COLD"
                    elif tens==1: outDict[code]=None   
        return outDict

class statTracker(valTracker):
    def __init__(self,runInfo,name,crit,const):
        super().__init__(runInfo,name,crit,const)
        self.initializeFlags()

    def push(self,val,time,dt):
        if val!=None:
            if val=="XCON": 
                self.addFlagEntry("AUXstat","XCON",time)
                return None
            self.vals["current"]=val
            self.checkConn(time,dt)
            self.checkFlag(time,"recharge",0,{1 : "SIM DEPLETED"})
            self.checkSD(time)
            self.checkSignal(time)
        return None

    def checkSD(self,time):
        sdFlag=self.vals["current"]["SDstat"]
        if sdFlag==None: return
        elif len(sdFlag)==2: #i.e. flag is from old firmware
            if sdFlag[0]=="1": self.eFlags["SDstat"]["NO SD"].append(time)
            elif sdFlag[1]=="1": self.eFlags["SDstat"]["SD ERROR"].append(time)
        elif len(sdFlag)==3: #i.e. if new firmware
            if sdFlag[0]=="0":  self.eFlags["SDstat"]["NO SD"].append(time)
            elif sdFlag[1]=="0":self.eFlags["SDstat"]["SD INIT ERROR"].append(time)
            elif sdFlag[2]=="1":self.eFlags["SDstat"]["SD ERROR"].append(time)

    def checkSignal(self,time):
        if type(self.vals["current"]["signal"])==int and  self.vals["current"]["signal"]<10: 
                self.eFlags["signal"]["LOW"].append(time) 

    def initializeFlags(self):
        self.eFlags["SDstat"]={"NO SD": list(), "SD ERROR": list(), "SD INIT ERROR": list()}
        self.eFlags["signal"]={"LOW": list()}

class ddtTracker(object):
    #Keeps a small list of values, derivatives, and times on hand for average derivatives,
    #noise detection,etc.
    def __init__(self,sInterval=None,critLen=None,critT=None):
        self.val=list()
        self.advdt=list() #stores the absolute value of the derivative
        self.dt=list()
        self.tList=list() #Stores the time stamps considered
        self.aVal=None      #Average of values
        self.mVal=None      #Median of values
        self.mdVdt=None     #Mean of derivatives
        self.totalTime=None #Total time in the subset
        self.sInterval=sInterval #Sampling interval (if modified)
        self.lastSample=None
        self.critLen=critLen     #Number of data points held in RAM
        if critLen==None: 
            self.optCritLen=5
            self.critLen=self.optCritLen
        else: self.optCritLen=critLen      #Desired number of points (tradeoff between sample size and performance)
        if critT!=None: self.getSival(critT)

    def getSival(self,critT):
        #Determines whether downsampling is necessary given a desired critical time
        #And the desired maximum size of the sample
        postLen=datetime.timedelta(seconds=15)
        if critT<=(postLen*self.optCritLen):
            self.sInterval=None
            self.critLen=round(critT/postLen)
        else:
            seconds=critT.seconds/self.optCritLen
            self.sInterval=datetime.timedelta(seconds=seconds)

    def push(self,time,val,dvdt,dt):
        if val!=None and dvdt!=None and dt!=None and dt>datetime.timedelta(0):
            dt=self.sampleTime(dt)
            if dt==None: return #i.e. if tracker needs to wait before sampling again
            
            if len(self.dt)>0 and len(self.dt)>self.critLen: self.removePoints(dt)
            self.val.append(val)
            self.advdt.append(abs(dvdt))
            self.dt.append(dt)
            self.tList.append(time)

            self.aVal=mean(self.val)
            self.mVal=median(self.val)
            self.totalTime=genSum(self.dt)
            self.madVdt=mean(self.advdt)/mean(self.dt).seconds #Mean of absolute derivatives
            self.dVal=(self.val[-1]-self.val[0])/self.totalTime.seconds #Average derivative

    def removePoints(self,dt):
        #Removes old entries before updating the list
        tSum=datetime.timedelta(0)
        while (tSum<dt and len(self.dt)>0):
            #remove the first element of the list until either the list ends or the sum or
            #an equivalent amount of time is removed from the ddt tracker
            self.dt.pop(0)
            self.val.pop(0)
            self.advdt.pop(0)
            self.tList.pop(0)
            tSum+=dt

    def sampleTime(self,dt):
        #Determines if downsampling is enabled,
        #Returns None if it isn't sampling time
        #Otherwise returns the time since the last sample
        if self.sInterval==None: return dt
        elif self.lastSample==None: 
            self.lastSample=dt
            return None

        self.lastSample+=dt
        if self.lastSample>=self.sInterval: 
            lastSample=self.lastSample
            self.lastSample=datetime.timedelta(0)
            return lastSample
        else: return None

    def enoughData(self): 
        return (len(self.dt)>=self.critLen)

class flat(object):
    #keeps track of a flatline 
    def __init__(self,medianValue,start,end,duration,critT,critS):
        postLen=datetime.timedelta(minutes=7.5)
        self.medianValue=medianValue
        self.start=start
        self.end=end
        self.duration=duration
        self.maxOffDur=duration/4 #Amount of time the sensor is off before a new flatline is started
        self.stamps=0 #Number of time stamps in flatline
        self.sNumCrit=round(duration/postLen) #Number of time stamps necessary to declare a flatline
        self.critT=critT
        self.critS=critS
        self.minVal=medianValue-critS
        self.maxVal=medianValue+critS

    def __str__(self):
        outStr=""
        outStr+="Median: %s, Magnitude: %s" %(self.medianValue, self.critS) 
        outStr+="\nStart: %s, End: %s, Duration: %s, Min.Dur.: %s" %(self.start,self.end,
                                                                    self.duration,self.critT)
        return outStr

    def update(self,stamp,dt):
        self.end=stamp
        self.duration+=dt
        self.stamps+=1

    def largeEnough(self):
        return (self.duration>self.critT and self.stamps>self.sNumCrit)

    def continues(self,value,dt):
        return (value>=self.minVal and value<=self.maxVal and dt>=self.maxOffDur)

class dataYield(object):
    #Object that keeps track of how many points were pushed during a given period of time
    #Returns None when updating unless a NO DATA flag needs to be thrown
    def __init__(self,startTime,const):
        self.start=startTime
        fracIso=0.25 #fraction of isolation time has to be <0.5 to not be caught in tIso filter
        self.sampleTime=fracIso*const["tIso"] #Ensure that the flag is not isolated when report is compled
        self.resetTime=const["minErrLen"] #If more time than this has elapsed, just reset
        self.timeLeft=self.sampleTime #Countown to deciding whether to push a flag
        #How many missed lines to throw a no data flag:
        self.expNumLines=self.sampleTime/const["logLen"]
        self.ndCrit=self.expNumLines*const["NDcrit"] 
        #How many missed lines to throw an intermittent data flag:
        self.idCrit=self.expNumLines*const["IDcrit"]
        self.dataPoints=0
        self.totalPoints=0

    def __str__(self):
        sOut=("Start:\t%s\n"
            "Points: \t%s\n"
            "tLeft: \t%s\n" %(self.start,self.dataPoints,self.timeLeft))
        return sOut

    def update(self,dt,time,data=True):
        #Updates the timer, and counters of valid data points and calls to the tracker
        #Returns None until flag criteria are met
        if dt>self.resetTime: #If last stamp seen was a while ago, start anew
            self.reset(time)
            return None
        if data==True: #Increase counters, decrease timer
            self.dataPoints+=1
        self.totalPoints+=1
        self.timeLeft-=dt
        if self.timeLeft<=datetime.timedelta(0): 
            #When sample time elapses, check if a flag needs to be returned
            return self.returnFlag(time)
        else: return None

    def returnFlag(self,time):
        #Check if there is cause for concern:
        flag=None
        if self.totalPoints<self.idCrit: flag="LOW LOG FREQ" #Fewer than expected calls
        elif self.dataPoints<self.ndCrit: flag="NO DATA" #Little data pushed
        elif self.dataPoints<self.idCrit: flag="INTERMITTENT DATA" #Data pushed only occasionally
        start=self.start
        self.reset(time)
        if flag!=None: return(flag,start)
        else: return None

    def reset(self,time):
        #Annulates counters and timer
        self.start=time
        self.timeLeft=self.sampleTime
        self.dataPoints=0
        self.totalPoints=0

#_____________________SCRIPT MAIN AND MAIN HELPERS________________#

def init():
    print("\n%s %s (%s) now running...."%(NAME,("v."+VERSION),REVISION))
    runInfo=runParams()
    runInfo.loadParams()
    class Struct(object): pass
    files=Struct()
    listFiles(runInfo,files)
    process(runInfo,files)

##________________Searching for files__________________________##
def listFiles(runInfo,files):
    sTime=time.time()
    getFilesTime=0
    files.raw=dict()
    files.cal=dict()
    if runInfo.get("Auto Checks"): files.err=dict()
    concatFilesDir=runInfo.get("Concatenated Files Directory")
    #Creates error file list, if the option is enabled
    if runInfo.get("Print Output"): print("Files Found:")
    else: print("Locating files...")
    for rampNum in runInfo.rampDict: #Go ramp by ramp through the ramp selection
        ramp=runInfo.rampDict[rampNum]
        concatDir=os.path.join(concatFilesDir,"s%s" %str(rampNum))
        #Precompile dicts of dates mapped to sets of paths for which these dates are present
        serverDict=precompilePathSet(ramp.dirs["Server"])
        sdDict=precompilePathSet(ramp.dirs["SD"])
        for date in runInfo.get("Date Range"): #Go date by date
            (sdFiles,serverFiles)=(set(),set())
            if date in sdDict: #Get SD files corresponding to the date
                paths=sdDict[date]
                sdFiles=getDateFiles(ramp,date,paths,SD=True) 
            if date in serverDict: #Get server files corresponding to the date
                paths=serverDict[date]
                serverFiles=getDateFiles(ramp,date,paths,SD=False)
            rawFileSD=rawFile.get.bestFile(sdFiles,concatDir) 
            rawFileServ=rawFile.get.bestFile(serverFiles,concatDir)
            readFile=rawFile.get.bestFile({rawFileServ,rawFileSD},concatDir) #THERE CAN BE ONLY ONE
            if readFile!=None: 
                addFile(files,readFile,runInfo)

def precompilePathSet(dirList):
    #outputs a set of all date files in a directory and returns it
    dateDict=dict()
    for folder in dirList:
        fDateDict=config.pull.dates.fromDir(folder,pathDict=True)
        config.mergeDicts(dateDict,fDateDict)
    return dateDict

def getDateFiles(ramp,date,pathList,SD=False):
    #Converts a set of paths for a given ramp, date and SD status into a rawFile object
    dateFiles=set()
    for path in pathList:
        file=rawFile(ramp,date,path=path,SD=SD)
        dateFiles.add(file)
    return dateFiles

def addFile(files,readFile,runInfo):
    ramp=readFile.ramp
    date=readFile.date
    writeFile=calFile.create(readFile,runInfo)
    if ramp in files.raw:
        files.raw[ramp].append(readFile)
        files.cal[ramp].append(writeFile)
    else: #In case the lists of files have not been created yet
        files.raw[ramp]=[readFile]
        files.cal[ramp]=[writeFile]
        if runInfo.get("Auto Checks"): #Create an error file if auto checks are enabled
            files.err[ramp]=errorFile(ramp,
                runInfo.get("Error Reports Directory"))
    if runInfo.get("Print Output"): print(readFile) #Print the file if requested

##______________Processing____________________________##

def process(runInfo,files):
    (lenFiles,sFiles)=getRawLen(files.raw)
    sFiles=sFiles/(1024**2) #Change size from bytes to Mb
    rTime=getEstRunTime(lenFiles,runInfo)/60.0
    print("%d files found (%d MB)" %(lenFiles,sFiles))
    print("Estimated Time to Completion: %.2f minute(s)" %rTime)
    print("\nBeginning Processing:\n")
    start=time.time()
    if runInfo.get("Multiprocess"): parallelProcess(runInfo,files)
    else: serialProcess(runInfo,files)
    print("\nProcessing Complete")
    end=time.time()
    print("%d files cleaned in %.1f seconds" %(lenFiles,end-start))
    if runInfo.get("Log Performance"):
        logPerformance(runInfo,lenFiles,sFiles,end-start)

def getEstRunTime(nFiles,runInfo):
    clockSpeed=4e9 #Assumed clock speed in Hz
    parallel=runInfo.get("Multiprocess")
    cCount=runInfo.get("Num. Process")
    czechs=runInfo.get("Auto Checks")
    if parallel:
        if czechs: fCycles=3.4e10
        else: fCycles=1.1e10
        estTime=fCycles*nFiles/(cCount*clockSpeed)
    else:
        if czechs: fCycles=6.3e9
        else: fCycles=1.96e9
        estTime=fCycles*nFiles/clockSpeed
    return estTime

def serialProcess(runInfo,files):
    err=runInfo.get("Auto Checks")
    for ramp in files.raw:
        if err: chk=files.err[ramp]
        else: chk=None
        rampWorker([runInfo,files.raw[ramp],files.cal[ramp],chk])

def parallelProcess(runInfo,files):
    err=runInfo.get("Auto Checks")
    (raw,cal)=(files.raw,files.cal)
    if err: chk=files.err
    else: chk=None

    byFile=runInfo.get("Process by File")
    numProc=runInfo.get("Num. Process")
    if not numProc: numProc=2

    if byFile:
        workerInput=organizeByFile(runInfo,raw,cal,chk)
        worker=fileWorker
    else:
        workerInput=organizeByRamp(runInfo,raw,cal,chk)
        worker=rampWorker

    print("Allocating %d processes to the parralel pool" %numProc)
    print("Please wait, this may take some time...")
    with Pool(numProc) as p:
        p.map(worker,workerInput)

def organizeByFile(runInfo,raw,cal,chk=None):
    out=[]
    for ramp in raw:
        for i in range(len(raw[ramp])):
            if chk: ap=[runInfo,raw[ramp][i],cal[ramp][i],chk[ramp]]
            else: ap=[runInfo,raw[ramp][i],cal[ramp][i],None]
            out.append(ap) 
    return tuple(out)

def organizeByRamp(runInfo,raw,cal,chk=None):
    out=[]
    for ramp in raw:
        if chk: ap=[runInfo,raw[ramp],cal[ramp],chk[ramp]]
        else: ap=[runInfo,raw[ramp],cal[ramp],None]
        out.append(ap)
    return tuple(out)

def rampWorker(input):
    (runInfo,rawL,calL,chk)=(input[0],input[1],input[2],input[3])
    for i in range(len(rawL)):
        (raw,cal)=(rawL[i],calL[i])
        fileWorker([runInfo,raw,cal,chk])

def fileWorker(input):
    #I know opening and closing inside a worker seems stupid, but I
    #think the multiprocessing library has a built-in mutex lock
    (runInfo,raw,cal,chk)=(input[0],input[1],input[2],input[3])
    printOut=runInfo.get("Print Output")
    openIO(raw,cal,printOut,chk)
    writeStartLines(raw,cal,chk)
    readWrite(runInfo,raw,cal,chk)
    closeIO(raw,cal,chk)

def openIO(raw,cal,printOut,chk=None): 
    raw.open()
    if chk:
        if not os.path.isdir(chk.dir): 
            os.makedirs(chk.dir)
            if printOut:print("\nCreated Directory %s" %chk.dir)
        chk.open('a+')
    if not os.path.isdir(cal.dir):
        os.makedirs(cal.dir)
        if printOut:print("\nCreated Directory %s" %cal.dir)
    cal.open('w')

def closeIO(raw,cal,chk=None):
    raw.close()
    cal.close()
    if chk: chk.close()

def writeStartLines(raw,cal,chk=None):
    cal.writeStartLine()
    if chk: chk.writeStartLine(raw)

def readWrite(runInfo,raw,cal,chk=None):
    printOut=runInfo.get("Print Output")
    if printOut: print("Processing file:\n%s" %str(raw)) #Print to terminal if option is enabled
    if chk: tracker=errorTracker(runInfo,cal,raw.getSize(),chk) #Initialize error tracker if needed
    elif runInfo.get("Auto Remove"): tracker=errorTracker(runInfo,cal,raw.getSize())
    else: tracker=None
    line=raw.readline() #Get first line of raw file
    while line!="":
        pDict=parseLine(line,cal,tracker) #Turns raw string into value dictionary
        wLine=config4Writing(pDict,cal) #Rewrites dictionary as an output string
        if wLine!=None: cal.write(wLine)  #If valid string, write to processed file
        #elif tracker: tracker.badLine(line)
        line=raw.readline() #Continue reading lines
    if printOut: print("Processed and published to:\n%s" %str(cal)) 
    #Lets the user know that a file has been processed successfully if option is enabled
    if chk: #If auto checks are on
        tracker.publishReport()
        if printOut: print("Error report published to:\n%s" %str(chk))
        #let user know that error reports were written successfully (if enabled)
    if printOut: print('\n') #Blank line between reports of processing completion

def parseLineOld(line,cal,tracker=None): 
    parsedDict=dict()
    line=line.split("X")
    for elem in line:
        if elem.startswith("DATE"):
            dateTime=read.timeStamp(elem,cal.date)
            if tracker: dateTime=tracker.push('DATE',dateTime)
            if dateTime!=None: 
                parsedDict['DATE']=dateTime
                break
    try: #If date could not be established, do not read or track line
        if dateTime==None: return None
    except: return None
    for elem in line:
        (eType,eParsed)=inspectElem(elem,tracker)
        if eParsed: parsedDict[eType]=eParsed
    return parsedDict

def parseLine(line,cal,tracker=None):
    parsedDict=dict() #Map to store parsed values
    line=line.split("X") #'X' separates the RAMP number from the data string
    line=line[1] #Everything after the "X" should constitute valid data
    line=line.split(",") #Values are comma-delimited
    #Attempt to locate and parse a valid time stamp:
    for i in range(len(line)):
        elem=line[i]
        if elem.startswith("DATE") and i!=len(line)-1:
        #Locate a string that says "DATE", assume the next element is the time stamp
        #Ensure that the "DATE" string is not the last element
            pass2Parser=','.join(["DATETIME",line[i+1]])
            dateTime=read.timeStamp(pass2Parser,cal.date)
            if tracker:  dateTime=tracker.push('DATE',dateTime)
            if dateTime!=None: 
                parsedDict['DATE']=dateTime
                tStampID=i+1 #Store index of time stamp if parsed successfully
                break
    try: #If date could not be established, do not read or track line
        if dateTime==None: return None
        #Otherwise, attempt to parse everything after the datetime string:
        else: parseSubstrings(parsedDict,line[i+1:]) 
    except: return None
   
def parseSubstrings(parsedDict,line):
    #Parse the data string after the time stamp
    #Does not return, populated the parsedDict
    pDict=read.options() #Get map of readable headers
    eLenDict=read.expectedLengths() #Get map of readable headers:expected number of outputs
    readableSet=pDict.keys()
    i=0 #Start immediately after the time stamp
    while i<len(line)-1:
        elem=line[i]
        if elem in readableSet: #if header is known by the reader, attempt to parse
            #If header is in the map of expected lengths, reterieve that value:
            if elem in eLenDict:
                expLen=eLenDict[elem]
            else: #Otherwise, assume there is only one
                expLen=1
            expDatLst=line[i:i+expLen+1] #Isolate data thought to be pertinent to the header
            if len(readableSet & set(expDatLst))>1: #i.e. if more than one header in isolated list
                pass #TO DO: Try to scavange data from corrupted substring
                raise RuntimeError('Feature not implemented:\nscavenging data from corrupted substring')
            else: #Otherwise, pass header and readings to appropriate parser
                pass2Parser=','.join(expDatLst) #prepare string to be parsed
                readings=pDict[elem](pass2Parser) #Get output
                if tracker: readings=tracker.push(elem,readings)
                if readings: #If extracted values were valid, add to output dictionary
                    parsedDict[elem]=readings
                    i+=expLen+1
        else: i+=1

def inspectElem(elem,tracker):
    pDict=read.options() #Map of parameter name to read method
    for key in pDict.keys():
        if elem.startswith(key):
            readings=pDict[key](elem)
            if tracker: readings=tracker.push(key,readings)
            if readings: return (key,readings)
            else: return (None,None)
        elif tracker!=None and elem.startswith("CON"): 
            tracker.push("STAT","XCON")
            return(None,None)
    return (None,None)

def config4Writing(pDict,cal):
    if pDict==None: return None #If whole line couldn't be read (e.g. bad date stamp)
    params=cal.output['params']
    order=cal.order #dictionary pre-compiled in the calFile object
    nLine=copy.copy(cal.blankLine)
    for key in pDict:
        if key in params and key in order:
            place=order[key]
            subList=copy.copy(params[key])
            for i in range(len(subList)):
                item=pDict[key][subList[i]]
                if checkASCII(str(item)):
                    subList[i]=item
            nLine[place]=subList
    nLine=flatten(nLine)
    nLine=stringify(nLine)
    dlm=","
    nLine=dlm.join(nLine)+"\n"
    return nLine

def logPerformance(runInfo,lFiles,sFiles,runTime):
    #Records completion speed and other statistics
    speed=4e9 #Processor clock speed
    perfPath=PERFPATH #Full path to performance file
    performance=open(perfPath,'a+')
    #Format: nFiles,Size,Checks,R/F,Print,nProc,Time,FPS,FPS/core,pCycles,Throughput,pCycles/file
    if runInfo.get("Auto Checks") or runInfo.get("Auto Remove"): checks="Y"
    else: checks="N" #Shows that checks are enabled when Auto Checks or Auto Remove are on
    if runInfo.get("Print Output"): printOut="Y"
    else: printOut="N"
    if runInfo.get("Multiprocess")==False:
        byRamp="R" #If not multiprocessing, counts as 1 process by RAMP
        nProc=1
    else:
        if runInfo.get("Process by File"): byRamp="F"
        else: byRamp="R"
        nProc=runInfo.get("Num. Process")
    fps=runTime/lFiles #Files per second
    fpsC=fps/nProc #Files per second per core
    pCycles=speed*runTime*nProc #pseudoCycles to complete program
    thruPut=sFiles/runTime #Amount of data processed per second
    pCyclesF=pCycles/lFiles #pseudoCycles per file
    pCyclesS=pCycles/sFiles #pseduoCycles per MB
    out=[lFiles,sFiles,checks,byRamp,printOut,nProc,runTime,fps,fpsC,pCycles,thruPut,pCyclesF,pCyclesS]
    out=stringify(out)
    nLine="\n"+",".join(out)
    performance.write(nLine)
    performance.close()
    print("\nPerformance recorded in: %s" %perfPath)

#________________OTHER HELPER FUNCTIONS____________________________#

def dateRangeFormatChecker(s):
#Format:yyyy-mm-dd/yyyy-mm-dd
    try:
        s0=s.split("/")
        str2Date(s0[0])
        str2Date(s0[1])
        return True
    except: return False

def range2Dates(L): #transforms a date range into a list of dates
    dateList=[]
    rng=L.split("/")
    try:
        sDate=str2Date(rng[0])
        eDate=str2Date(rng[1])
        if sDate>eDate: (sDate,eDate)=(eDate,sDate) #In case the range is backwards
    except ValueError:
        print("\nERROR\nInvalid Date Range\n\
            Please make sure it is entered in the format: yyyy-mm-dd/yyyy-mm-dd")
        return []
    cDate=sDate
    day=datetime.timedelta(days=1)
    while cDate<=eDate:
        dateList+=[cDate] #Appends a date
        cDate+=day
    return(dateList)

def str2Date(s):
#Input format: y-m-d-> datetime object
    (y,m,d)=s.split("-")
    (y,m,d)=(int(y),int(m),int(d))
    return datetime.date(y,m,d)

def str2TimeDelta(s):
    #Takes a string in the format h:m:s or m:s and converts to time delta
    if type(s)!=str: raise TypeError("Input needs to be a string")
    try: 
        s=s.split(":")
        if len(s)==3: #i.e. if h:m:s
            (hr,mn,sc)=(int(s[0]),int(s[1]),int(s[2]))
            return datetime.timedelta(hours=hr,minutes=mn,seconds=sc)
        elif len(s)==2: #i.e. if m:s
            (mn,sc)=(int(s[0]),int(s[1]))
            return datetime.timedelta(minutes=mn,seconds=sc)
    except: raise ValueError("Could not parse the string: %s" %s)

def stringify(L):
#Turns lists/sets of integers,floats, etc. into list of strings
    newL=[]
    for item in L:
        if item==None: newL.append("")
        else: newL.append(str(item))
    return newL

def removeChars(s,toRemove):
#Given a string and a set of characters/substrings,
#Removes the characters/substrings from the input string and returns
    s_modified=""
    for i in range(len(s)):
        if (s[i] in toRemove)==True:
            i+=1 #skips over undesired character
        else:
            s_modified+=s[i]
    return s_modified

def flatten(L):
#Collapses a list of lists into a single list of base elements
    if L==[]: return L #Base case that Catches empty lists
    elif type(L)!=list: return L #base case that catches nonlist elements
    elif type(L[0])==list: #recursively flattens a sublist
        flatList=flatten(L[0]) 
        return flatList+flatten(L[1:])
    else: return [L[0]]+flatten(L[1:]) 

def getListVals(L,Ind):
#returns the values of a list at corresponding indexes
    Ind=sorted(list(Ind))
    rL=[None]*len(Ind)
    for i in range(len(Ind)):
        rL[i]=L[Ind[i]]
    return Ind

def FtoC(F):
    if type(F)!=float: #In case the input is non-numeric
        try: F=float(F)
        except: return None
    return round((F-32)*5/9,1)

def checkASCII(s):
#Returns true only if all characters are ASCII
    try: return len(s.encode())==len(s)
    except: return False

def concatenatePath(L):
#Takes a path as a list or tuple, concatenates it into a single string
    cPath=L[0]
    L=L[1:]
    for elem in L:
        cPath=os.path.join(cPath,elem)
    return cPath

def getRawLen(D):
    counter=0 #Keeps track of total file length
    sCounter=0 #Keeps track of total file size
    for ramp in D:
        counter+=len(D[ramp])
        for file in D[ramp]:
            sCounter+=file.size
    return (counter,sCounter)

def transferDictVals(pDict,tgtDict,app=None):
    #Takes two dictionaries with shared keys, 
    #transfers information over from pDict to tgtDict for shared keys
    #Optionally appends the keys in pDict
    #function takes advantage of aliasing and does not return
    for key in pDict:
        if (type(key)==type(app)) : keyTgt=key+app
        else: keyTgt=key
        if keyTgt in tgtDict: tgtDict[keyTgt]=pDict[key]

def noneDict(D):
    #Checks that all of the non-key entries in a dictionary are not None
    #Recursively checks subdicts
    if type(D)!=dict: raise TypeError('Input is not a dictionary') #Check for non-dict entries
    for elem in D:
        entry=D[elem]
        if type(entry)==dict and noneDict(entry)==False: return False #Check subdicts 
        elif entry!=None: return False #Check actual entries
    return True        

def blankIterable(I):
    return len(I)==0

def mean(L):
    if len(L)==0: raise IndexError('List has zero length')
    try:
        try: lSum=genSum(L)
        except: raise TypeError('Could not sum the list') 
        return lSum/len(L)
    except: raise TypeError('Cannot divide sum of list (%s) by integer' %type(lSum).__name__)

def median(LIn):
    L=sorted(LIn)
    length=len(L)
    if length==0: raise IndexError('List has zero length')
    if length%2==0:
        try:
            (mid0,mid1)=(float(L[length//2-1]),float(L[length//2]))
            return (mid0+mid1)/2
        except: raise TypeError('Could not convert entries to floats')
    else:
        try: return float(L[length//2])
        except: raise TypeError('Could not convert entry to float')

def genSum(L):
    #Infers data type of list from first element and sums accordingly
    if len(L)==0: raise IndexError('Cannot sum over an empty list')
    elif len(L)==1: return L[0]
    total=L[0]
    for elem in L[1:]:
        try: total+=elem
        except: raise TypeError('Cannot add %s and %s' %(type(total).__name__,type(elem).__name__))
    return total
    
def reverseDict(D):
    if type(D)!=dict: raise TypeError('Input is not a dictionary')
    else:
        nD=dict()
        for key in D:
            val=D[key]
            nD[val]=key
    return nD

def closestDateRange(ranges,date=None):
    #Given a list of date range strings chooses:
    # one with the latest expiration date if no date is given
    # one closest to date if it is given
    latestDate=None
    for dateStr in ranges:
        dates=dateStr.split("/")
        d1=str2Date(dates[0])
        d2=str2Date(dates[1])
        if d2<d1: (d1,d2)=(d2,d1) #Swaps in case the order is backwards
        if latestDate==None:
            latestDate=(dateStr,d1,d2)
        elif date and closerDate((d1,d2),latestDate,date): 
            latestDate=(dateStr,d1,d2)
        elif not(date) and laterDate((d1,d2),latestDate): 
            latestDate=(dateStr,d1,d2)
    return latestDate[0]

def laterDate(dates,lastDate):
    #Returns true if either the start of the range is later
    #or the end is later if the start dates are equal
    d1=dates[0]
    d2=dates[1]

    ld1=lastDate[1]
    ld2=lastDate[2]

    if d1>ld1: return True #i.e. if newer
    elif ld1==d1 and d2>ld2: return True 
    #If start at same time, one that expires later
    else: return False

def closerDate(dates,lastDate,tgt):
    #Returns true if the start date of the range is closer to the target date
    zdt=datetime.timedelta(0)

    d1=dates[0]
    d2=dates[1]

    diffD1=tgt-d1
    diffD2=tgt-d2

    ld1=lastDate[1]
    ld2=lastDate[2]

    diffLd1=tgt-ld1
    diffLd2=tgt-ld2

    #i.e. if start date is closer and either:
    #start date is before target date
    #both dates are after the target date
    if abs(diffD1)<=abs(diffLd1) and (diffD1>=zdt or diffD2<zdt): return True

#if __name__ == '__main__':
    #multiprocessing.freeze_support()
    #init()
