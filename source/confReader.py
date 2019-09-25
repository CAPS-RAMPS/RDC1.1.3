#Revisions:
    #2019-??-??
        #-Fix slowdown when user input 'beginning of time' or 'end of time'
        #-Fix bug when using 'yesterday' as a keyword
        #-Fix mixed list bug:  e.g. 2018-2-20/2018-2-22, 2018-2-24/2018-2-25
        #-Pre-fill Output fields
        #-Fix callback when saving empty window
    #2018-12-21
        #Fixed bug where '.TXT' files were not recognized as '.txt'
    #2018-8-24
        #Fixed output verification. Now parameters marked as "none" that are not in the order
            #will not generate an error
    #2018-8-17: 
        # Added optional delimit and dlm arguments to importDict method.
            # when delimit=True and dlm is a string, importDict will split entries by the
            # dlm character. delimit is True by default and dlm is a comma by default
        # Fixed bug where verifier crashed on empty Order field, but nonempty output fields

# TO DO #
    #Incorporate keyword detection into date lists and ranges (low priority)
    #Expand output format file verification

import os
import string
import datetime
import copy

class config(object):
    def __init__(self,template,directory=None):
        #Stores the forward and the reverse dictionary 
        if directory==None: directory=os.getcwd()
        self.dir=directory

        self.fDict=copy.deepcopy(template) #Forward dictionary
        self.template=template #Save the template for verification purposes
        self.rDict=config.writeReverseDict(self.fDict) #Creates a reverse dictionary

        self.wDict=None #Stores parameters as written to file

        self.errors=None #Store error/warning information if checks enabled
        self.warnings=None

    def __str__(self):
        return "Loader Object w/ Parameters:\n\n%s" %config.write.dict2str(self.fDict)

    def load(self,fileName,check=False,dependencies=None):
        #Parses the config file line by line, pulls values.
        #config.wipeDict(self.fDict) #Sets all values to None before loading other values
        filePtr=open(fileName,'r',encoding='ascii',errors='surrogateescape')
        line=filePtr.readline()
        while line!="":
            line=line.strip()
            while config.skipLine(line): #Skip over lines not containing configuration info
                line=filePtr.readline()
                line=line.strip() 
            try: 
                line=line.split("=")
                #Removes leading and trailing whitespace for both parameter and value:
                (param,value)=(line[0].strip(),line[1].strip())
                if param in self.rDict: #Only tries to load parameter if it is in the dictionary
                    self.loadValue(param,value)
            except: pass #Skip line if could not be delimited
            line=filePtr.readline()
        filePtr.close()
        if check: #If verification is turned on, carry it out
            (self.wDict,self.errors,self.warnings)=config.verify.complete(self.fDict,self.template,
                                                                dependencies,self.dir)
            return self.fDict #Return list of issues and the populated dictionary
        else: #If no checks requested 
            return self.fDict

    def loadValue(self,param,line):
        #Calls the appropriate loading function based on the parameter name
        miscDict=config.pull.miscDict() #Map of reading functions to parameter names
        #Handle parameters that specify paths:
        if self.rDict[param]=="Paths": 
            self.fDict["Paths"][param]=config.pull.path(param,line)
        #Handle parameters that specify things that are on or off:
        elif self.rDict[param]=="Toggles": 
            self.fDict["Toggles"][param]=config.pull.toggle(line)
        #Handle parameters that specify processed file output:
        elif self.rDict[param]=="Output": 
            self.fDict["Output"][param]=config.pull.output(param,line)
        #Calls the function specified in dictionary:
        elif param in miscDict: 
            self.fDict["Misc"][param]=miscDict[param](line)

    @staticmethod
    def importDict(fileName,delimit=True,dlm=","):
        outDict=dict()
        filePtr=open(fileName,'r',encoding='ascii',errors='surrogateescape')
        line=filePtr.readline()
        cCat=None #Current category of parameter
        while line!="":
            line=line.strip() #Remove whitespace at beginning and end
            while config.noInfoInLine(line): #Remove lines that contain no information
                line=filePtr.readline()
                line=line.strip()
            while config.categoryLabel(line): #i.e. if a line is a category label
                line=line[1:-1] #Remove first and last elements of a line [brackets]
                outDict[line]=dict()
                cCat=line #Set the current category label to the string in the file
                line=filePtr.readline() #Continue reading
                line=line.strip()
            try:
                line=line.split("=")
                (param,value)=(line[0].strip(),line[1].strip())
                #Set value to none if detects that a null entry was made 
                if config.pull.nullVal(value): value=None 
                #if there are delimiters in the value string:
                elif delimit and type(dlm)==str and value.find(dlm)!=-1: 
                    value=value.split(dlm) #Delimit by characters into a list
                if cCat!=None: #Writes value in the dictionary under a category label if it exists
                    outDict[cCat][param]=value
            except: pass #Don't bother with line if not formatted correctly
            line=filePtr.readline()
        filePtr.close()
        return outDict

    def save(self,fileName,path=False):
        if path==True:
            config.write.toPath(fileName,self.wDict)
        else:
            config.write.toFile(fileName,self.wDict)

    def noErrors(self):
        #Parses the self.errors dictionary. Returns false is there is an error.
        #If all errors are "None", however, return true
        if self.errors==None: return True
        for category in self.errors:
            for param in self.errors[category]:
                if self.errors[category][param]!=None:
                    return False
        return True

    @staticmethod
    def wipeDict(d):
        #Sets all values in a dictionary to None, but keeps the keys
        #Does not return, uses aliasing
        for key in d:
            if type(d[key])==dict: config.wipeDict(d[key]) #Recursive call on subdicts
            else: d[key]=None

    @staticmethod
    def skipLine(line):
        #Decided whether the line is a comment, category label, or a blank line
        return (config.noInfoInLine(line) or line.startswith("["))

    @staticmethod
    def noInfoInLine(line):
        #Skips over comments and whitespace only lines
        return (line.startswith("#") or line=="")

    @staticmethod
    def categoryLabel(line):
        #Determines whether the line satisfies the criteria for a category label
        return (line.startswith("[") and line.endswith("]"))

    @staticmethod
    def removeComment(line):
        #Disregards everything after "#" if the file needs to be commented
        cIndx=line.find('#')
        if cIndx!=0: return line[:cIndx]
        else: return line

    @staticmethod
    def mergeDicts(dTgt,dMrg):
        #Adds keys and appends values from dMrg to dTgt
        #Returns dTgt
        for key in dMrg:
            if key not in dTgt: #If key is not in dTgt, add it in, alias values
                dTgt[key]=dMrg[key]
            else: 
                if dMrg[key]==None: dTgt[key]=dMrg[key] #Overwrite NoneType entries
                else:
                    tgtType=type(dTgt[key])
                    mrgType=type(dMrg[key])
                    if tgtType==mrgType:
                        if tgtType==set: dTgt[key]=dTgt[key]|dMrg[key] #Merge sets
                        elif tgtType==list: dTgt[key]+=dMrg[key] #Merge lists
                        #Recursive call for dict of dicts:
                        elif tgtType==dict: config.mergeDicts(dTgt[key],dMrg[key])
                        else: #If type not specified, raise error
                            raise TypeError("Function can only handle, sets, lists, and dicts",
                                            "Input is of type: %s" %str(tgtType))
                    elif tgtType==set and mrgType==list:
                        dTgt[key]=dTgt[key]|set(dMrg[key])
                    elif tgtType==list and mrgType==set:
                        dTgt[key]+=list(dMrg[key])
                    else: #If cannot combine raise error
                        raise TypeError("Cannot combine %s and %s" %(str(tgtType),str(mrgType)))
        return dTgt

    class pull(object):
        #Organizes functions used to convert values to dictionary entries
        @staticmethod
        def miscDict():
            #Returns a parameter-function map
            miscDict={ #Maps reading functions to the subcategories labeled "Misc."
                "Time Gap": config.pull.tGap,
                "Ramp Nums": config.pull.ramps.nums,
                "Date Range": config.pull.dates.fromLine,
                "Num. Process": config.pull.numProc
                }
            return miscDict

        @staticmethod
        def nullVal(line):
            #Checks if the value passed to it can be interpreted as a "no input"
            if type(line)==str:
                return  ( #Looks for no input keywords
                        line.lower()=="none" or 
                        line.lower()=="n/a" or 
                        line==""
                        )
            elif (type(line)==list or type(line)==set or type(line)==dict):
                #Looks for empty iterables
                return  len(line)==0
            elif line==None: return True #NoneType works too
            else: return False #If can't read, assume there is input

        #Static methods that read parameters into the forward dictionary
        @staticmethod
        def path(pathName,line):
            if config.pull.nullVal(line): #If entry explicitly defined as empty, log and return
                return None
            multPaths={"Raw Directory"} #List of parameters that can accept multiple paths
            if pathName in multPaths: #If the parameter takes multiple paths, create a list
                pathList=line.split(",") #Delimit list by commas
                dirs=list() #Declare the entry as an empty list
                for path in pathList:
                    dirs.append(path.strip()) #Populate map entry
                return dirs
            else: return line
        
        @staticmethod
        def toggle(line):
            #If the toggle is specified as "y", it enables the toggle
            #Otherwise toggle set to false
            if line.lower()=="y": return True
            else: return False

        @staticmethod
        def output(pName,line):
            #Deals with the parameter set specifying processed file output
            if config.pull.nullVal(line): #If entry explicitly defined as empty, log and return
                return None
            singleEntry={"Output File Name"} #Parameters that don't take list values
            if pName not in singleEntry:
                valueList=list() #Initialize output as a list
                entries=line.split(",") #Delimit by commas
                for entry in entries:
                    entry=entry.strip() #Remove whitespace at beginning and end of entry
                    valueList.append(entry)
                return valueList
            else: return line

        @staticmethod
        def tGap(line):
            #Tries to parse the time gap entry in as a timedelta object
            if config.pull.nullVal(line): #If entry explicitly defined as empty, log and return
                return None
            try:
                line=line.split(':')
                (h,m,s)=(int(line[0]),int(line[1]),int(line[2]))
                return(datetime.timedelta(hours=h,minutes=m,seconds=s))
            except: return("Error Parsing '%s'" %line)
                #For unexpected entried, log as 

        @staticmethod
        def numProc(line):
            #Pulls out the number of processes as an integer
            if config.pull.nullVal(line): #If entry explicitly defined as empty, log and return
                return None
            else:
                try: return(int(line))
                except: return("Error Parsing '%s'" %line)

        @staticmethod
        def masterDict(rawDir):
            #Gets together a master dictionary of the raw directory selected mapping:
            #ramp No. -> dates present ->files with that date.
            masterDict=dict()
            allRamps=config.pull.ramps.all(rawDir,returnPathDict=True)
            for ramp in allRamps:
                allDates=dict()
                for directory in allRamps[ramp]:
                    dates=config.pull.dates.fromDir(directory,pathDict=True)
                    config.mergeDicts(allDates,dates)
                masterDict[ramp]=allDates
            return masterDict

        class ramps(object):
            #Organizes ramp number extraction methods
            @staticmethod
            def nums(line):
                value=line.lower() #Convert to lowercase
                if config.pull.nullVal(line): #If entry explicitly defined as empty, log and return
                    return None
                elif value.startswith("all"): return "all"
                else:
                    try: return config.pull.ramps.mixedList(value)
                    except: return "Error Parsing '%s'" %line

            @staticmethod
            def range(value):
                #Splits a range in the format 100-150 into a list of ramps
                rampSet=set()
                value=value.split('-')
                [rMin,rMax]=sorted([int(value[0]),int(value[1])+1])
                for i in range(rMin,rMax):
                    rampSet.add(i)
                return rampSet

            @staticmethod
            def mixedList(value):
                rampSet=set()
                elements=value.split(',')
                for elem in elements:
                    if config.verify.ramp.range(elem): #checks if the element contains a range e.g. (101-130)
                        subrange=config.pull.ramps.range(elem)
                        rampSet=rampSet|subrange #Add ramps from subrange into set
                    else:
                        try: #Try to parse in element as an integer ramp number
                            ramp=int(elem)
                            rampSet.add(ramp)
                        except: pass
                return rampSet

            @staticmethod
            def numFromPathElem(element):
                #Get the ramp number from a string that is either s104 or 104
                num=None
                if (element.startswith("s")):
                    try: num=int(element[1:])
                    except: pass
                else: 
                    try: num=int(element)
                    except: pass
                return num

            @staticmethod
            def all(rawDirs,returnPathDict=False):
                #Pulls all ramp numbers from a list of directories 
                allRampDict=dict()
                for rawDir in rawDirs: #iterate through list of directories
                    contents=os.listdir(rawDir)
                    for element in contents:
                        path=os.path.join(rawDir,element) #Get path to every element
                        num=None
                        try: num=config.pull.ramps.numFromPathElem(element)
                        except: pass #Verify that the element is a valid ramp number
                        validRampDir=num!=None and config.verify.ramp.number(num)
                        if validRampDir:
                            #Add a path to the ramp dictionary if if exists
                            if num in allRampDict: allRampDict[num].append(path)
                            #otherwise create a dictionary entry and a list with the path
                            else: allRampDict[num]=[path]
                #Return the dictionary if requested
                if returnPathDict: return allRampDict
                #Otherwise, jsut return the numbers
                else: return set(allRampDict.keys())

        class dates(object):
            @staticmethod
            def fromLine(line):
                #Pull a date range from a string. Recognize daily and all as special inputs
                if config.verify.date.keywords(line): return line.lower()
                else:
                    try: return config.pull.dates.mixedList(line)
                    except: return "Error Parsing '%s'" %line

            @staticmethod
            def fromStr(s):
                #Converts string in format "yyyy-m-d" to datetime object
                (y,m,d)=s.split("-")
                (y,m,d)=(int(y),int(m),int(d))
                return datetime.date(y,m,d)

            @staticmethod
            def fromFile(s):
                #Given a directory element, attempts to pull a datetime object from the name
                #Crashes otherwise
                fileExt=".txt" #File extensions
                servExt="-raw" #Suffix of server files
                chopOff=-1*len(fileExt) #Number of elements to chop off at the end
                s=s[0:chopOff] #Get rid of file extension
                if s.endswith(servExt):
                    chopOff=-1*len(servExt) 
                    s=s[0:chopOff]#get rid of server extension
                    return config.pull.dates.fromStr(s) 
                else: #In case the file is an SD file
                    #Delimit format YYMMDD into yyyy-mm-dd, check the string
                    y=int(s[0:2])
                    m=int(s[2:4])
                    d=int(s[4:6])
                    y+=2000 #Bring into current millenium
                    dateStr="%d-%d-%d" %(y,m,d) #Format as a string yyyy-m-d
                    return config.pull.dates.fromStr(dateStr) 

            @staticmethod
            def mixedList(line):
                #Takes a list in format d0,d1/d2,d3,d4,d5/d6, etc. 
                #where d1,...,dn are stings in format yyyy-m-d
                #Converts to a list of datetime objects
                dateList=list()
                elements=line.split(",")
                for elem in elements:
                    #checks if the element contains a range e.g. (2018-6-1/2018-7-1):
                    if config.verify.date.range(elem): 
                        subrange=config.pull.dates.range(elem)
                        dateList+=subrange #Add ramps from subrange into set
                    else:
                        try: #Try to parse in element as an integer ramp number
                            date=config.pull.dates.fromStr(elem)
                            dateList.append(date)
                        except: pass
                return sorted(dateList)

            @staticmethod
            def range(elem):
                #Takes date range in format yyyy-m-d/yyyy-m-d, converts to a list of datetime objects
                dateList=list()
                [d0,d1]=elem.split("/") 
                (d0,d1)=(config.pull.dates.fromStr(d0),config.pull.dates.fromStr(d1)) #Convert to datetime objects
                if d0>d1: (d0,d1)=(d1,d0) #Swaps elements if they're backwards
                cDate=d0 #keeps track of current date within the loop
                day=datetime.timedelta(days=1) #length of one day
                while cDate<=d1:
                    dateList+=[cDate] #populates date list
                    cDate+=day
                return dateList 

            @staticmethod
            def fromDir(directory,pathDict=False):
                #Compiles a set of dates in a given directory
                #Recursively scanes subdirectories with names in SDdataDirs set
                #Compiles a list of directories mapped to dates if specified
                if pathDict==True: dateDict=dict()
                else: dateSet=set()
                SDdataDirs={"DATA","USB"}
                dirCont=os.listdir(directory)
                for element in dirCont:
                    elemPath=os.path.join(directory,element)
                    if os.path.isdir(elemPath) and (element in SDdataDirs):
                        #Add a dateSet from the subdir if exists:
                        if pathDict==True:
                            subDirDateDict=config.pull.dates.fromDir(elemPath,pathDict)
                            dateDict=config.mergeDicts(dateDict,subDirDateDict)
                        else:
                            dateSet=dateSet|config.pull.dates.fromDir(elemPath,pathDict) 
                    else:
                        #Try to pull date from the file name:
                        date=config.verify.date.file(element,elemPath,returnDate=True)
                        if date!=None:
                            if pathDict==True:
                                #Create entry if it does not exist:
                                if date not in dateDict: dateDict[date]={elemPath} 
                                else: dateDict[date].add(elemPath)
                            else:
                                dateSet.add(date)
                if pathDict: return dateDict
                else: return dateSet

            @staticmethod
            def all(rawDir,rampNums=None):
                #Creates a set of all data files in a given directory and with a given set of ramps
                #Uses the config.pull.ramps.all command to get a list of ramp directories
                #Calls config.pull.dates.fromDir on those directories to populate a set
                dates=set()
                #Get a dictionary of all ramps where ramp->list of paths to ramp folder
                allRampsDict=config.pull.ramps.all(rawDir,returnPathDict=True)
                #If a ramp Set not given, make it all ramps
                if rampNums==None: rampNums=set(allRampsDict.keys()) 
                #Iterate through the ramp numbers:
                for ramp in rampNums:
                    #Iterate through directories found for those ramp numbers:
                    for rampDir in allRampsDict[ramp]: 
                        #Add the dates of date files found in those folders to the dates set:
                        dates=dates|config.pull.dates.fromDir(rampDir)
                return dates

    @staticmethod
    def writeReverseDict(fDict):
        #Takes a dictionary and switches the keys and values places
        rDict=dict() #Stores the reverse dictionary
        for key in fDict:
            for subkey in fDict[key]: #Map each entry in dictionary to its key 
                rDict[subkey]=key #(as opposed key ->entry)
        return rDict

    class verify(object):
        @staticmethod
        def complete(fDict,template,dependencies,directory):
            #Conducts a full examination of the forward dictionary in question. 
            warnings=copy.deepcopy(template) #Warnings dictionary, same format as template
            errors=copy.deepcopy(template) #Errors dictionary, same format as template
            wDict=copy.deepcopy(fDict) #Dictionary that does not auto-expand keywords (e.g. "all")
            rDict=config.writeReverseDict(fDict)
            config.verify.toggles(warnings,errors,fDict,wDict)
            config.verify.path.block(warnings,errors,fDict,wDict)        
            config.verify.misc.block(warnings,errors,fDict,wDict)
            config.verify.output.block(warnings,errors,fDict,wDict,template,directory)
            if dependencies!=None:
                config.verify.dependencies(warnings,errors,fDict,rDict,wDict,dependencies)
            return (wDict,errors,warnings)

        @staticmethod
        def toggles(warnings,errors,fDict,wDict): pass

        @staticmethod
        def dependencies(warnings,errors,fDict,rDict,wDict,dependencies):
            for param in dependencies:
                paramCat=rDict[param] #Get the category name from the reverse dictionary
                sameSetting=(config.pull.toggle(dependencies[param]["setting"])==
                            fDict[paramCat][param])
                for dependency in dependencies[param]: #For each dependent parameter
                    if dependency!="setting":
                        depCat=rDict[dependency]
                        if sameSetting:
                            #Overwrite the setting with the one in the dependencies dictionary
                            #and clear errors
                            #if the settings are specified in the dependencied dict: 
                            if dependencies[param][dependency]!="valid":
                                #Set the value to that specified by the dependencies file
                                fDict[depCat][dependency]=dependencies[param][dependency]
                                #Duplicate in writing dictionary:
                                wDict[depCat][dependency]=dependencies[param][dependency]
                                #Clear warnings and errors, as things are fille automatically
                                errors[depCat][dependency]=None
                                warnings[depCat][dependency]=None
                            #If the entry should be valid, transfer appropriate warnings to the
                            #errors dictionary
                            elif (config.verify.warn2err(depCat,dependency,
                                                warnings[depCat][dependency])
                                and errors[depCat][dependency]==None):
                                #Transfer the warning into errors:
                                errors[depCat][dependency]=warnings[depCat][dependency]
                                #Reset the warning:
                                warnings[depCat][dependency]=None
                        else: #i.e. if setting the user made is different to the one in dependcies
                            if (dependencies[param][dependency]==None and
                                config.verify.warn2err(depCat,dependency,
                                                        warnings[depCat][dependency]) and
                                errors[depCat][dependency]==None):
                                errors[depCat][dependency]=warnings[depCat][dependency]
                                warnings[depCat][dependency]=None
                            elif dependencies[param][dependency]=="valid":
                                #Set the value to None, as it is no longer required
                                fDict[depCat][dependency]=None
                                #Duplicate in writing dictionary:
                                wDict[depCat][dependency]=None
                                #Clear warnings and errors, as things are fille automatically
                                errors[depCat][dependency]=None
                                warnings[depCat][dependency]=None                                

        @staticmethod
        def warn2err(category,paramName,warning):
            #Decides whether to convert warnings for a given parameter into an error
            warn2Err={"No entry"} #Warnings that automatically get converted to errors
            nonOptCat={"Paths"} #Categories where warnings are converted
            nonOptParam={"Ramp Nums","Date Range"} #Parameters where warnings are converted
            #Warnings starting with these strings get converted:
            startWarn2Err={"Error Parsing","Time Gap"} 
            if warning==None: return False #Don't bother writing the warning if there is none
            elif category in nonOptCat: return True #For categories in the set, convert all warnings
            elif paramName in nonOptParam: return False #Do not convert all non-optional parameters
            elif warning in warn2Err: return True #Convert if the error is in the warn2Err set
            else:
                #If a warning starts with the keywords in startWarn2Err set, convert
                for error in startWarn2Err:
                    if warning.startswith(error): return True
            return False

        class misc(object):
            @staticmethod
            def block(warnings,errors,fDict,wDict):
                #Goes through the miscellaneous parameters and checks for errors using helpers
                order=["Ramp Nums","Date Range"]
                ordSet=set(order)
                miscParams=set(fDict["Misc"].keys())
                #First look at elements that have to be verified in order:
                for param in order:
                    if param not in miscParams: #Raise an error if a parameter not present when expected
                        raise KeyError("Expected the parameter '%s' to be present" %param) 
                    config.verify.misc.param(warnings,errors,param,fDict,wDict)
                for param in miscParams:
                    if param not in ordSet:
                        config.verify.misc.param(warnings,errors,param,fDict,wDict)

            @staticmethod
            def param(warnings,errors,param,fDict,wDict):
                #Writes errors and warnings to individual misc. param
                #Entries only required when some parameters are enabled:
                optional={"Time Gap","Num. Process"}
                miscDict={ #Maps reading functions to the subcategories labeled "Misc."
                    "Time Gap": config.verify.misc.tGap,
                    "Ramp Nums": config.verify.ramp.nums,
                    "Date Range": config.verify.date.list,
                    "Num. Process": config.verify.misc.numProc
                    }
                entry=fDict["Misc"][param]
                if entry==None: #Catch blank entries
                    if param in optional: warnings["Misc"][param]="No entry"
                    else: errors["Misc"][param]="No entry"
                elif type(entry)==str and entry.startswith("Error Parsing"):
                    #Catch parsing errors, erase entries
                    fDict["Misc"][param]=None
                    wDict["Misc"][param]=None
                    errors["Misc"][param]=entry
                    warnings["Misc"][param]=None
                elif param in miscDict: 
                    (valid,warning,error)=miscDict[param](fDict["Misc"][param],fDict,wDict,errors)
                    errors["Misc"][param]=error
                    warnings["Misc"][param]=warning
                else: warnings["Misc"][param]="No verification function found for %s" %param

            @staticmethod
            def numProc(n,fDict,wDict,*args):
                from multiprocessing import cpu_count
                nCpu=cpu_count()
                (valid,warning,error)=(None,None,None)
                if n>nCpu:
                    warning="Too many processes: %d\nThis computer only has %d cores" %(n,nCpu)
                    (valid,error)=(True,None)
                elif n<=1:
                    if n<1: #Auto-set num.Proc. to 1 if fewer than 1
                        fDict["Misc"]["Num Process"]=1
                        wDict["Misc"]["Num Process"]=1 
                    warning=("Only one process specified.\n"
                            "Consider disabling multiprocessing for faster execution")
                    (valid,error)=(True,None)
                elif n==nCpu:
                    warning=("All %d cores allocated.\n"
                            "Ensure that the selection size is not too large\n"
                            "so as not to overheat the CPU"   %n)
                    (valid,error)=(True,None)
                else: (valid,warning,error)=(True,None,None)
                return(valid,warning,error)

            @staticmethod
            def tGap(t,*args):
                #Verifies that the time gap min. length is not too short or long
                tMin=datetime.timedelta(seconds=15) #Minimum timedelta between two logged lines
                tMax=datetime.timedelta(hours=24) #Length of one data file
                (valid,warning,error)=(None,None,None)
                if t<=tMin:
                    error="Time Gap too small: %s\nMust be at least %s" %(t,tMin)
                    (valid,warning)=(False,None)
                elif t>=tMax:
                    error="Time Gap too large: %s\nMust be at most %s" %(t,tMax)
                    (valid,warning)=(False,None)
                else: 
                    (valid,warning,error)=(True,None,None)
                return (valid,warning,error)

        class path(object):
            #Verification of the Paths subdicionary and its individual entries
            @staticmethod
            def block(warnings,errors,fDict,wDict):
                paths=fDict["Paths"]
                optional={"Error Reports Directory"}
                #Verify the whole subdictionary
                for entry in paths:
                    #Notify the programmer if a path was left blank
                    if paths[entry]==None: 
                        if entry in optional: warnings["Paths"][entry]="No entry"
                        else: errors["Paths"][entry]="No entry"
                    elif type(paths[entry])==list: #i.e. list of paths
                        validPaths=False
                        for path in paths[entry]:
                            if path==None: continue #If an empty list entry, skip
                            else: 
                                (validPath,error)=config.verify.path.isValid(entry,path)
                                if not validPath: warnings["Paths"][entry]=error
                                else: validPaths=True
                        if not validPaths:#If zero paths in the list were valid
                            errors["Paths"][entry]="No valid paths found: %s" %paths[entry]
                        else: 
                            errors["Paths"][entry]=None
                    else: #Having eliminated other possibilities, the entry must be a single path
                        (validPath,error)=config.verify.path.isValid(entry,paths[entry])
                        if entry in optional: warnings["Paths"][entry]=error
                        else: errors["Paths"][entry]=error

            @staticmethod
            def isValid(pathName,path):
                #Determines whether a given path exists and does not
                #Specify an incorrect location (e.g. a directory when a file was requested)
                #Returns whether the path is true and what the nature of the error is (None if valid)
                if not os.path.exists(path): 
                    error="Path not found on disk: %s" %path
                    return (False,error)
                elif os.path.isdir(path) and not pathName.endswith("Directory"):
                    error="Name specifies file, directory given: %s" %path
                    return (False,error)
                elif not os.path.isdir(path) and pathName.endswith("Directory"):
                    error="Name specifies directory, file given: %s" %path
                    return (False,error)
                elif pathName=="Raw Directory":#Verify raw directory separately
                    return config.verify.path.rawDir(path) 
                else: return(True,None)

            @staticmethod
            def rawDir(path):
                SDdataDirs={"DATA","USB"}
                contents=os.listdir(path)
                for element in contents: #Go through each item in a folder
                    error="No RAMP folders found in %s" %path #Default error assumption
                    rampPath=os.path.join(path,element) #path to the item
                    #Try to pull a ramp Number from the element
                    rampNum=config.pull.ramps.numFromPathElem(element) 
                    validRampNum=config.verify.ramp.number(rampNum)
                    if os.path.isdir(rampPath) and validRampNum:
                        error="No data files found in %s" %path #For the case of no data files in dir
                        foldContents=os.listdir(rampPath)
                        for file in foldContents: #check if there are valid date files in subdir
                            filePath=os.path.join(rampPath,file)
                            #Return true if at least one valid date file found:
                            if config.verify.date.file(file,filePath): return (True,None)
                            #Check if an element is and SD directory and parse that:
                            elif os.path.isdir(filePath) and (file in SDdataDirs):
                                sdContents=os.listdir(filePath)
                                for sdFile in sdContents:
                                    sdFilePath=os.path.join(filePath,sdFile)
                                    if config.verify.date.file(sdFile,sdFilePath): return(True,None)
                return (False,error)

        class ramp(object):
            @staticmethod
            def number(n):
                rMin=90
                rMax=500
                return (type(n)==int and n>=rMin and n<=rMax)

            @staticmethod
            def nums(nums,fDict,wDict,errors,*args):
                warning=None
                error=None
                valid=None
                if errors["Paths"]["Raw Directory"]!=None: #Entry invalid if the Raw Directory is
                    valid=False
                    error=errors["Paths"]["Raw Directory"]
                    warning=None 
                else:
                    #get a set of all ramps:
                    allRamps=config.pull.ramps.all(fDict["Paths"]["Raw Directory"])
                    if nums=="all": #If all ramps were specified, auto-complete
                        fDict["Misc"]["Ramp Nums"]=allRamps
                        (valid,error,warning)=(True,None,None)
                    #If the ramp numbers are a subset of all ramps:
                    elif nums<=set(allRamps): (valid,error,warning)=(True,None,None)
                    else:
                        #If there are no ramps in directory that were specified in config
                        if (nums-allRamps)==nums:
                            valid=False
                            error=("None of the entered RAMPs"
                                 "were found in Raw Directory:\n%s" %sorted(list(nums)))
                            warning=None
                        else: #If some portion of RAMPs were in the directory
                            rejects=nums-allRamps
                            fDict["Misc"]["Ramp Nums"]=nums-rejects #Get rid of rejects in input
                            warning=("The following RAMP numbers were not found in directory\n"
                                    "%s" %sorted(list(rejects)))
                            error=None
                            valid=True
                return (valid,warning,error)      

            @staticmethod
            def range(line):
                try:
                    line=line.split('-')
                    if len(line)!=2 or line[0]==line[1] : return False
                    r0=int(line[0])
                    r1=int(line[1])
                    if config.verify.ramp.number(r0) and config.verify.ramp.number(r1):
                        return True
                    else: return False
                except: return False

        class date(object):
            @staticmethod
            def keywords(s):
                #takes a string argument, returns true if it is a valid date keyword
                keywords={"daily","yesterday","today","all"}
                if s.lower() in keywords: return True
                else: return False

            @staticmethod
            def range(s):
                #Format:yyyy-mm-dd/yyyy-mm-dd
                try:
                    s0=s.split("/")
                    config.pull.dates.fromStr(s0[0])
                    config.pull.dates.fromStr(s0[1])
                    return True
                except: return False

            @staticmethod
            def list(dateList,fDict,wDict,errors,*args):
                #Converts date input into a list of dates
                warning=None
                error=None
                valid=None
                if errors["Misc"]["Ramp Nums"]!=None: #Entry invalid if the RAMP input is
                    valid=False
                    error=errors["Misc"]["Ramp Nums"]
                    warning=None 
                else: #Pull a set of all dates to cross-reference with input:
                    allDates=config.pull.dates.all(fDict["Paths"]["Raw Directory"],
                                                        fDict["Misc"]["Ramp Nums"])
                    if type(dateList)==str: #Means that input is either "all","daily",or "today"
                        if dateList=="all": 
                            #Raw Directory was previously verified, so can assume is valid
                            valid=True
                            error=None
                            warning=("This may be quite a lot of information.\n"
                                    "Make sure you have allocated the necessary computer resources")
                            #Put all dates in a sorted list for chronological auto checks writing
                            fDict["Misc"]["Date Range"]=sorted(list(allDates))
                        elif dateList=="today":
                            today=datetime.date.today()
                            if today in allDates:
                                fDict["Misc"]["Date Range"]=[today]
                                (valid,error,warning)=(True,None,None)
                            else:
                                (valid,warning)=(False,None)
                                error="No files for today in the specified director(y/ies)"
                        elif dateList=="daily" or dateList=="yesterday":
                            yday=datetime.date.today()-datetime.timedelta(days=1)
                            if yday in allDates:
                                fDict["Misc"]["Date Range"]=[yday]
                                (valid,error,warning)=(True,None,None)
                            else:
                                (valid,warning)=(False,None)
                                error="No files for yesterday in the specified director(y/ies)"
                    else: #Means that the input will be a sorted mixed list
                        dateSet=set(dateList)
                        datesAbsent=dateSet-allDates
                        #If there are no dates in the date list that are also not in allDates:
                        if datesAbsent==set(): 
                            (valid,error,warning)=(True,None,None)
                        #If none of the dates in the list are in the specified directories
                        elif datesAbsent==dateSet:
                            (valid,warning)=(False,None)
                            error=("None of the dates input are in the specified director(y/ies)\n"
                                    "%s" %dateList)
                            fDict["Misc"]["Date Range"]=list()
                        #If some of the dates in the list are not in the specified directories
                        else:
                            (valid,error)=(True,None)
                            warning=("The following dates are not in the specified director(y/ies):"
                                    "\n%s" %sorted(list(datesAbsent)))
                            fDict["Misc"]["Date Range"]=sorted(list(dateSet-datesAbsent))
                return (valid,warning,error)

            @staticmethod
            def file(s,sPath,returnDate=False):
                #Checks if the file and its path are consistent with a RAMP data file
                fileExt=".txt"
                errMargin=datetime.timedelta(days=7)
                #SDext={"(DATA)","(USB)"}
                if not os.path.exists(sPath) and returnDate==False: return False
                elif os.path.isdir(sPath) and returnDate==False: return False
                elif not s.lower().endswith(fileExt) and returnDate==False: return False
                else:
                    try: #If could not pull the date from the string, invald date file
                        date=config.pull.dates.fromFile(s)
                        if date>(datetime.date.today()+errMargin): #Eliminate entries from the future
                            if returnDate: return None
                            else: return False
                        if returnDate==True: return date
                        else: return True
                    except: 
                        if returnDate: return None
                        else: return False

        class output(object):
            @staticmethod
            def block(warnings,errors,fDict,wDict,template,directory):
                #Verifies the whole output block entry by entry
                outDict=fDict["Output"]
                if "Output File Name" in outDict:
                    entry="Output File Name"
                    (valid,warning,error)=config.verify.output.file(outDict[entry],directory)
                    errors["Output"]["Order"]=error
                    warnings["Output"]["Order"]=warning
                if "Order" in outDict:
                    (valid,warning,error)=config.verify.output.order(outDict,template)
                    errors["Output"]["Order"]=error
                    warnings["Output"]["Order"]=warning
                doneSet={"Output File Name","Order"}
                for entry in outDict:
                    if entry not in doneSet:
                        (valid,warning,error)=config.verify.output.comp2Template(entry,
                                                                            outDict,template)
                        errors["Output"][entry]=error
                        warnings["Output"][entry]=warning

            @staticmethod
            def file(fileName,directory):
                #Check that a valid output file was entered
                outFold="Output"
                (valid,warning,error)=(None,None,None)
                if fileName==None:
                    (valid,warning)=(False,None)
                    error="No entry"
                    return (valid,warning,error)                    
                #Get directory with settings/constants/output/etc. folders:
                outFilePath=os.path.join(directory,outFold,fileName) #Path to the output file
                #Check that the path is valid:
                (valid,error)=config.verify.path.isValid("Output File Name",outFilePath) 
                return (valid,warning,error)

            @staticmethod
            def order(outDict,template):
                (valid,warning,error)=(None,None,None)
                order=outDict["Order"]
                if order==None:
                    (valid,warning)=(False,None)
                    error="No entry"
                    return (valid,warning,error)
                ordSet=set(order) #Repackaged the order list into a set for comparison w/template
                validOrdSet=set(template["Output"]["Order"])
                invalidHeaders=ordSet-validOrdSet
                if invalidHeaders==set():
                    for header in ordSet: #Check that the headers also have entries
                        if header not in outDict:
                            (valid,warning)=(False,None)
                            error="Header '%s' does not have an entry in the output dictionary"
                        else: #Check that the corresponding header entries are also valid
                            (valid,warning,
                                error)=config.verify.output.comp2Template(header,outDict,template)
                else: #Catch the cases when one or more headers are not in the template
                    (valid,warning)=(False,None)
                    if invalidHeaders==ordSet: #If none are in the template
                        error="None of the entered headers are recognized:\n%s" %order
                    else:
                        error="The following headers are not recognized: \n%s" %list(ordSet)
                return (valid,warning,error)

            @staticmethod
            def comp2Template(entry,outDict,template):
                #Ensures that all the parameters listed in the config. file are also in the template
                (valid,warning,error)=(None,None,None)
                validHeaderSet=set(template["Output"]["Order"]) #Headers in the template file
                headerSet=outDict["Order"]#headers in the Order string
                if headerSet==None: headerSet=set()
                else: headerSet=set(headerSet)
                if entry not in validHeaderSet:
                    if entry in headerSet: #Throw an error if a header is used
                        (valid,warning)=(False,None)
                        error="Header '%s' is not recognized" %entry
                    else: #Only give a warning if a header is not used. Do not check it
                        (valid,error)=(True,None)
                        warning="Header '%s' is not recognized" %entry
                elif entry not in headerSet and outDict[entry]!=None: 
                    #Give a warning if the order string does not contain the header
                    (valid,error)=(True,None)
                    warning="Header '%s' is not listed in the output order" %entry
                elif outDict[entry]==None:
                    if entry in headerSet:
                        (valid,warning)=(False,None)
                        error="No entry"
                    else:
                        (valid,error,warning)=(True,None,None)
                else:
                    line=outDict[entry]
                    paramSet=set(line) #Repack line elements into set for comparison
                    validEntries=template["Output"][entry]
                    #Configure valid set based on whether there are one entires or multiple:
                    if type(validEntries)==list: validSet=set(validEntries)
                    else: validSet={validEntries}
                    elemNotInTemplate=paramSet-validSet #See which elements are not in the template
                    if elemNotInTemplate==set(): (valid,warning,error)=(True,None,None)
                    else: #See if the whole entry is invalid or just a part of it
                        #Give appropriate error:
                        (valid,warning)=(False,None)
                        if elemNotInTemplate==paramSet:
                            error=("None of the listed parameters are recognized:\n"
                                    "%s" %line)
                        else:
                            error=("The following parameters are not in the template:\n"
                                  "%s" %sorted(list(elemNotInTemplate)))
                return (valid,warning,error)

    class write(object):
        @staticmethod
        def toFile(name,configDict):
            #Setting Paths
            ext=".ini"
            if not name.endswith(ext): name+=ext #Add appropriate extension if not present
            setFold="Settings"
            #Get directory with settings/constants/output/etc. folders:
            workDir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            setPath=os.path.join(workDir,setFold)
            setFilePath=os.path.join(workDir,setFold,name) #Path to the settings file
            if not os.path.exists(setPath):
                raise FileNotFoundError("%s Directory does not exist in:\n%s" %(setPath,workDir))

            wStr=config.write.convert2Str(configDict)

            setFile=open(setFilePath,'w')
            setFile.write(wStr)
            setFile.close()

        @staticmethod
        def toPath(path,configDict):
            wStr=config.write.convert2Str(configDict)
            setFile=open(path,'w')
            setFile.write(wStr)
            setFile.close()

        @staticmethod
        def convert2Str(configDict):
            wDict=copy.deepcopy(configDict)
            #Compress date and ramp lists for more compact storage:
            wDict["Toggles"]=config.write.toggles2YN(wDict["Toggles"])
            wDict["Misc"]["Ramp Nums"]=config.write.ramp2MixedList(wDict["Misc"]["Ramp Nums"])
            wDict["Misc"]["Date Range"]=config.write.date2MixedList(wDict["Misc"]["Date Range"])
            return config.write.dict2str(wDict) #Convert dictionary into a string

        @staticmethod
        def toggles2YN(togDict):
            #Returns a dictionary copy where all true values are converted to "Y" and the rest to "N"
            newTogDict=copy.deepcopy(togDict) #Output dictionary
            for toggle in newTogDict:
                if (newTogDict[toggle]==True or (type(newTogDict[toggle])==str and 
                    newTogDict[toggle].lower()=="y")):
                    newTogDict[toggle]="Y"
                else: newTogDict[toggle]="N" #Consent rule: the only Y is True
            return newTogDict

        @staticmethod
        def ramp2MixedList(rampList):
            #Compresses a numerical list into a mixed list
            keywords={"all"}
            try:
                if rampList in keywords: return rampList
            except: pass
            newList=list()
            if len(rampList)<=1: 
                return config.write.stringify(rampList)
            else:
                rampList=sorted(rampList)
                (i0,i1)=(0,0)
                subRange=list()
                for i in range(len(rampList)):
                    #Try to get the difference between adjacent entries. 
                    #Skip iteration if not possible:
                    i1=i+1 #Set i1 as the higher index
                    if i1<len(rampList): diff=rampList[i1]-rampList[i] 
                    if diff>1 or i1==len(rampList):
                        if i1-i0>2: #If a contraction can be made by eliminating >=1 No. 
                            subRange=["%d-%d"%(rampList[i0],rampList[i1-1])] 
                            #Convert subrange into a string "n0-n1"
                        #Otherwise, just stringify the list:
                        else: subRange=config.write.stringify(rampList[i0:i1])
                        newList+=subRange
                        (i0,i1)=(i+1,i+1)
            return newList

        @staticmethod
        def date2MixedList(dateList):
            #Compresses a date list into a mixed list 
            keywords={"daily","today","all"}
            try:
                if dateList in keywords: return dateList
            except: pass
            delta=datetime.timedelta(days=1)
            newList=list()
            if len(dateList)<=1: return config.write.stringify(dateList)
            else:
                dateList=sorted(dateList)
                (i0,i1)=(0,0)
                subRange=list()
                for i in range(len(dateList)):
                    #Try to get the difference between adjacent entries. 
                    #Skip iteration if not possible:
                    i1=i+1 #Set i1 as the higher index
                    if i1<len(dateList): diff=dateList[i1]-dateList[i] 
                    if diff>delta or i1==len(dateList):
                        if i1-i0>2: #If a contraction can be made by eliminating >=1 No. 
                            subRange=["%s/%s"%(str(dateList[i0]),str(dateList[i1-1]))] 
                            #Convert subrange into a string "n0-n1"
                        #Otherwise, just stringify the list:
                        else: subRange=config.write.stringify(dateList[i0:i1])
                        newList+=subRange
                        (i0,i1)=(i+1,i+1)
            return newList

        @staticmethod
        def dict2str(D):
            #Converts a configuration dictionary into a string
            sOut="" #Stores the output string
            for key in D:
                sOut+="\n[%s]\n" %key #Introduce category labels
                for subkey in D[key]:
                    sOut+=subkey+'=' #Adds parameter name to output string
                    member=D[key][subkey] #i.e. value of parameter
                    if (type(member)==list or type(member)==set):
                        strList=config.write.stringify(member) #Converts lists/sets/etc. into a string
                        strList=",".join(strList)
                        sOut+=(str(strList).strip())+"\n" #Cleans up line before adding it
                    else: sOut+=str(D[key][subkey])+'\n'
            return sOut

        @staticmethod
        def stringify(L):
        #Turns lists/sets of integers,floats, etc. into list of strings
            newL=[]
            for item in L:
                if item==None: newL.append("")
                else: newL.append(str(item))
            return newL
