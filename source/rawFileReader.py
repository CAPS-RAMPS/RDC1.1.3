#__CHANGELOG__#
    #2019-04-01: Fixed compatibility issue when a new PPA is paired w/RAMP-PPA box w/new firmware
    #2018-08-27: Fixed string-reading bug (checkASCII method was absent)
    #2018-08-17: Fixed PPA reading bugs
    #2018-08-17: Added BCM reading functionality
    #2018-08-16: Created

import string
import datetime

class read(object):
    def __init__(self): pass

    @staticmethod
    def expectedLengths():
        #Functions which returns how many values are expected
        #After each header in the map below:
        opt={
            "RAW"   : 8,
            "STAT"  : 3,
            "MET"   : 2
            }

        return opt

    @staticmethod
    def options():
        #Maps the headers below to functions which read substrings
        # with those headers
        opt={
            "RAW"   : read.echem.raw,
            "CO"    : read.echem.cal,
            "NO2"   : read.echem.cal,
            "SO2"   : read.echem.cal,
            "NO"    : read.echem.cal,
            "O3"    : read.echem.cal,

            "CO2"   : read.singleVal,
            "T"     : read.singleVal,
            "RH"    : read.singleVal,

            "BATT"  : read.singleVal,
            "CHRG"  : read.singleVal,
            "RUN"   : read.singleVal,
            "SD"    : read.singleVal,

            "PM1.0" : read.ptr.new,
            "PM2.5" : read.ptr.new,
            "PM10"  : read.ptr.new,

            "WD"    : read.singleVal,
            "WS"    : read.singleVal,

            "MET"   : read.met,
            "TSI"   : read.tsi,
            "ADI"   : read.adi,
            "PPA"   : read.ppa.line,
            "PTR"   : read.ptr.old,
            "STAT"  : read.stat,
            "BCM"   : read.bcm
            }
        return opt

    @staticmethod
    def timeStamp(s,fileDate=None):
        try:
            maxDateError=datetime.timedelta(days=2) 
            #Date stamps in file may be off from file date by this amount
            s=s.split(',')[1] #After splitting, s=['DATETIME',datetime]
            (date,time)=s.split(' ')

            (m,d,y)=date.split("/")
            (m,d,y)=(int(m),int(d),int(y)+2000)
            #Assuming RAMP data was gathered between years 2000 and 2100

            (hr,mn,sc)=time.split(":")
            (hr,mn,sc)=(int(hr),int(mn),int(sc))

            dt=datetime.datetime(year=y, month=m, day=d, 
                                hour=hr, minute=mn, second=sc)
            date=datetime.date(year=y,month=m,day=d)

            if ((fileDate==None) or (abs(date-fileDate)<maxDateError)): 
                return {'DATETIME': dt}
            elif ((fileDate!=None) and abs(date-fileDate)>maxDateError):
                #Attempt to correct the year, in case it is set incorrectly
                try:
                    yFile=fileDate.year
                    date=datetime.date(year=yFile,month=m,day=d)
                    dt=datetime.datetime(year=yFile, month=m, day=d, 
                                        hour=hr, minute=mn, second=sc)
                except: return None
                if abs(date-fileDate)<maxDateError: 
                    return {'DATETIME': dt} #Return corrected stamp if successful
                else:
                    #Try correcting the month and the year
                    try:
                        yFile=fileDate.year
                        mFile=fileDate.month
                        date=datetime.date(year=yFile,month=mFile,day=d)
                        dt=datetime.datetime(year=yFile, month=mFile, day=d, 
                                            hour=hr, minute=mn, second=sc)
                    except: return None
                    if abs(date-fileDate)<maxDateError: 
                        return {'DATETIME': dt} #Return corrected stamp if successful
                    else:
                        return None
            else: return None
        except: return None

    @staticmethod
    def singleVal(s):
        try:
            sTemp=s.split(',')
            header=sTemp[0]
            out={header : (float,1,None)}
            try: return read.vals(s,out,2)
            except: return out #Return empty dictionary if could not be parsed but connected
        except: return None

    class echem(object):
        @staticmethod
        def raw(s):
            dOut=   {"S1": None #Stores values of echem sensors
                    ,"S2": None
                    ,"S3": None
                    ,"S4": None
                    }
            try:
                s=s.split(",")
                #print(s)
                #if (len(s)!=2*len(dOut)+2) and (len(s)!=2*len(dOut)+1): 
                #    return dOut #Rejects unexpected line lengths
                nEnt=2 #Number of eChem entries per sensor
                nSens=len(dOut) #Number of sensors
                for i in range(1,nSens*nEnt+1,nEnt):
                    #Iterates over the number of entries expected in the ECHEM line,
                    #skipping every other value (2 readings for each ECHEM signal)
                    #Goal of loop, get the net signal from the two readings for each sensor
                    sNum="S"+str(i//nEnt+1) #i.e. S1, S2, S3, S4
                    if sNum in dOut:
                        try:
                            (S_s,S_r)=(int(s[i]),int(s[i+1]))
                            #Gets the two signals for the particular sensor
                            dOut[sNum]=S_s-S_r #Stores the net value in the dictionary
                        except: pass
                #print(dOut)
                return dOut
            except: return dOut

        @staticmethod
        def cal(s):
            try:
                sTemp=s.split(',')
                gasID=sTemp[0] #The header is the gas name
                calSuf="CAL" #Suffix added to 'calibrated' gas headers
                gasCalName=gasID+calSuf
                out={gasCalName : (float,1,None)}
                try: return read.vals(s,out,2)
                except: return out #Return empty dictionary if could not be parsed but connected
            except: return None

    class batt(object):
        @staticmethod
        def line(s):
            out={
                "BATT"  : (int,1,1/100), #Battery voltage
                "STAT"  : (str,2,None)   #Battery status code
                }
            try: 
                outVals=read.vals(s,out,len(out)+1) #Try to parse batt line from regular ramps
                outVals["STAT"]=read.batt.stat(outVals["STAT"])
                return outVals
            except: 
                try: #Try to parse batt line from prototype ramp
                    out={
                    "BATT"  : (int,1,1/100), #Battery voltage
                    "STAT"  : (str,2,None),  #Battery status string
                    "CCHRG" : (int,3,None), #Charging current
                    "CSYS"  : (int,4,None), #System current
                    }
                    outVals=read.vals(s,out,len(out)+1) #Try to parse batt line from new ramp
                    outVals["STAT"]=read.batt.stat(outVals["STAT"])
                    return outVals
                except: return None

        @staticmethod
        def stat(stat):
            #Convert battery status 
            if stat==None: return None
            new=len(stat)==2 #How to tell whether the BATT stat line is new
            try: stat=int(stat)
            except: return None
            if new:
                if stat<10: return "COLD" #0x of new stat line means cold battery
                elif stat==11: return "A/C" #x1 iof new stat line indicates charging
                else: return "BATTPWR"
            else:
                if stat%2==0: return "FAULT" #xx0 means battery fault
                elif stat<100: return "COLD" #0xx means cold battery
                else: return "OK"

    @staticmethod
    def met(s):
        out={
            "MET"       : (int,1,None),
            "METFLAG"   : (int,2,None)
            }
        try: return read.vals(s,out,len(out)+1)
        except: return out

    class ppa(object): 
        @staticmethod
        def line(elem):
            #Attempts to retreive and reformat PurpleAir portion of the data string
            outDict={
                    "T_PPA" : None,
                    "H_PPA" : None,
                    "P"     : None,
                    #"DP"    : None,
                    #"Alt"   : None,
                    'PM010A': None,
                    'PM010B': None,
                    'PM025A': None,
                    'PM025B': None,
                    'PM100A': None,
                    'PM100B': None
                    }
            elemStr=elem #Store a copy of string for newLine
            elem=elem.split(',')
            if elem[0]=='PPA':
                elem=elem[1:] #Removes the 'PPA'
            nElemNewCln=13 #Max n(elements) in new RAMP-PPA firm, but old PPA firm
            nElemOld=4 #Max. number of elements in old string
            if len(elem)<=nElemOld:
                return read.ppa.oldLine(elem,outDict)
            elif len(elem)<=nElemNewCln:
                return read.ppa.newLineCln(elemStr,outDict)
            else:
                return read.ppa.newLineX(elemStr,outDict)

        @staticmethod
        def newLineCln(elemStr,outDict):
            #Reads line output by new RAMP-PPA boxes and OLD
            #PPA firmware (max 13 elem)
            lFormat={ #Order of parameters in new firmware line
                    "T_PPA" :   (int,1,None),
                    "H_PPA" :   (int,2,None),
                    "DP"    :   (float,3,None),
                    "P"     :   (float,4,None),
                    "Alt"   :   (float,5,None),
                    "PM010A":   (float,6,None),
                    "PM025A":   (float,7,None),
                    "PM100A":   (float,8,None),
                    "PM010B":   (float,9,None),
                    "PM025B":   (float,10,None),
                    "PM100B":   (float,11,None),
                    "PPAPWR":   (int,12,None)
                    }
            try:
                parsedVals=read.vals(elemStr,lFormat) #Attempt to parse line
                #Convert values in Fahrenheit to real units:

                parsedVals['T_PPA']=read.ppa.FtoC(parsedVals['T_PPA'])
                parsedVals['DP']=read.ppa.FtoC(parsedVals['DP'])

                #Get rid of values not requested by outDict:
                read.transferDictVals(parsedVals,outDict)
            except: pass
            return outDict

        def newLineX(elemStr,outDict):
            #Reads line output by new RAMP-PPA boxes and NEW
            #PPA firmware (max. 19 elem)
            lFormat={ #Order of parameters in new firmware line
                    "T_PPA" :   (int,1,None),
                    "H_PPA" :   (int,2,None),
                    "DP"    :   (float,3,None),
                    "P"     :   (float,4,None),
                    "Alt"   :   (float,5,None),
                    "PM010A":   (float,7,None),
                    "PM025A":   (float,9,None),
                    "PM100A":   (float,11,None),
                    "PM010B":   (float,13,None),
                    "PM025B":   (float,15,None),
                    "PM100B":   (float,17,None),
                    "PPAPWR":   (int,18,None)
                    }
            try:
                parsedVals=read.vals(elemStr,lFormat) #Attempt to parse line
                #Convert values in Fahrenheit to real units:

                parsedVals['T_PPA']=read.ppa.FtoC(parsedVals['T_PPA'])
                parsedVals['DP']=read.ppa.FtoC(parsedVals['DP'])

                #Get rid of values not requested by outDict:
                read.transferDictVals(parsedVals,outDict)
            except: pass
            return outDict

        @staticmethod
        def oldLine(elem,outDict):
            #Attempts to parse PurpleAir data from the old firmware 
            #i.e. when(PPA directly uploads string to RAMP)
            try:
                #Attempt to separate string into A channel, B channel, and stats(T,RH,DP,Alt):
                (A,B,stats)=read.ppa.substrings(elem)
                #Attempt to parse data from each channel: 
                try: PMA=read.ppa.findPM(A)
                except: PMA=dict()
                try: PMB=read.ppa.findPM(B)
                except: PMB=dict()
                try: ambCond=read.ppa.stats(stats)
                except: ambCond=dict()

                #Consolidate parsed values into outDict:
                read.transferDictVals(PMA,outDict,'A')
                read.transferDictVals(PMB,outDict,'B')
                read.transferDictVals(ambCond,outDict)

                if not read.noneDict(outDict): #if all values in a dictionary are None
                    return outDict
                else:
                    try: return(read.ppa.bySpaces(elem))#Try reading by spaces as a last-ditch effort
                    except: return outDict
            except:
                try: return(read.ppa.bySpaces(elem)) #Try reading by spaces as a last-ditch effort
                except: return outDict #Returns empty columns if there is deviation from format

        @staticmethod
        def substrings(elem):
            Astart='A'
            Bstart='B'
            statsStart={'T','emp'}
            (A,B,stats)=(None,None,None)
            for subElem in elem:
                if subElem.startswith(Astart):
                    A=subElem
                elif subElem.startswith(Bstart):
                    B=subElem
                else:
                    for s in statsStart:
                        if subElem.startswith(s):
                            stats=subElem
                            break
            return (A,B,stats)

        @staticmethod
        def bySpaces(elem):
            #Should the theoretically more robust readPPA() fail
            #This function will attempt to use spaces as references for where data ought to be
                #Dependencies:
                    #FtoC
                    #stringify
            outDict={ #Stores the theoretical locations of elements in a space-delimited string
                    "T_PPA" : 1,
                    "H_PPA" : 4,
                    "P"     : 9,
                    #"DP"    : 13,
                    #"Alt"   : 16,
                    'PM010A': 2,
                    'PM010B': 2,
                    'PM025A': 5,
                    'PM025B': 5,
                    'PM100A': -2,
                    'PM100B': -2
                    }       
            (A,B,stats)=read.ppa.substrings(elem)
            A=A.split(' ') #splits each string by spaces
            B=B.split(' ')
            stats=stats.split(' ')
            read.ppa.correlateSpacing({'PM010A','PM025A','PM100A'},A,outDict)
            read.ppa.correlateSpacing({'PM010B','PM025B','PM100B'},B,outDict)
            read.ppa.correlateSpacing({'T_PPA','DP'},stats,outDict,FtoC)
            read.ppa.correlateSpacing({'H_PPA','Alt'},stats,outDict,int)
            read.ppa.correlateSpacing({'P'},stats,outDict)
            return outDict

        @staticmethod
        def correlateSpacing(paramSet,pList,outDict,cFun=float):
            #takes a set of parameters (paramSet), space-delimited list (pList),
            #dictionary with theoretical places of the parameters in the list (outDict),
            #and a processing function (cFun)
            #Tries to find each of the parameters in the pList and populate the outDict
            #tries to apply the cFun to the parameters if found in the list
            for elem in paramSet:
                place=outDict[elem]
                try: val=cFun(pList[place])
                except: 
                    try: val=cFun(pList[place+1])
                    except: val=None
                if type(val)==float: #Selectively rounds values
                    if abs(val)<10: val=round(val,2)
                    else: val=round(val,1)
                outDict[elem]=val

        @staticmethod
        def findPM(elem):
        #Finds the pm values in the string and outputs after converting to float
            #Dependencies:
                #findInd
                #findPPAPM
            PM= {
                "PM010" : None,
                "PM025" : None,
                "PM100" : None
                }
            if elem==None: return PM #Catches the case 'PPA,,,0'
            pm1='PM1.0:'
            pm25='PM2.5:'
            pm10='PM10:'
            eStr='('#character(s) after the PM values wanted
            locPM1=read.ppa.indeces(elem,pm1,eStr)
            locPM25=read.ppa.indeces(elem,pm25,eStr)
            locPM10=read.ppa.indeces(elem,pm10,eStr)
            if locPM1!=None:    PM["PM010"]=read.ppa.pm(elem[locPM1[0]:locPM1[1]]) 
            if locPM25!=None:   PM["PM025"]=read.ppa.pm(elem[locPM25[0]:locPM25[1]])
            if locPM10!=None:   PM["PM100"]=read.ppa.pm(elem[locPM10[0]:locPM10[1]])
            return PM

        @staticmethod
        def stats(elem):
        #nearly identical to findPM function, but for other stats
            #Dependencies:
                #findInd
                #FtoC
            stats=  {
                    "T_PPA" : None,
                    "H_PPA" : None,
                    "P"     : None,
                    "DP"    : None,
                    "Alt"   : None
                    }
            if elem==None: return stats #Catches the case 'PPA,,,0'
            T='Temp:'
            H='Hum:'
            DP='Dew Point:'
            P='Pressure:'
            Alt='Altitude:'
            locT=read.ppa.indeces(elem,T,'*') #Finds the locations of the strings starting and ending
            locH=read.ppa.indeces(elem,H,'%') #with the variables defined above
            locDP=read.ppa.indeces(elem,DP,'*')
            locP=read.ppa.indeces(elem,P,'h')
            locAlt=read.ppa.indeces(elem,Alt,'m')
            if locT!=None:  stats["T_PPA"]=read.ppa.FtoC(int(elem[locT[0]:locT[1]])) #Writes to dictionary if foudn
            if locH!=None:  stats["H_PPA"]=int(elem[locH[0]:locH[1]])
            if locDP!=None: stats["DP"]=read.ppa.FtoC(float(elem[locDP[0]:locDP[1]]))
            if locP!=None:  stats["P"]=float(elem[locP[0]:locP[1]])
            if locAlt!=None:stats["Alt"]=int(elem[locAlt[0]:locAlt[1]])
            return stats

        @staticmethod
        def indeces(elem,tgt,eStr):
        #Given a string and a target:
        #1.Finds the next element after the target string
        #2.Finds the next eStr after the tgt e.g. next '(' for PM values
        #Returns None if either one is not found
            try:
                i=elem.index(tgt)+len(tgt)
                e=elem[i:].index(eStr)+i
                return (i,e)
            except: return None

        @staticmethod
        def pm(s):
            #Attempts to parse a string to extract a PM value
            try:
                s=s.split(" ")
                if len(s)==4:
                    try: return float(s[2]) #return where the value is expected
                    except:
                        for elem in s:
                        #If the value is in a place other than where expected
                            if elem=="": continue
                            elif len(elem)>1: #catches check digits (or whatever the extraneous numbers are)
                                try: return float(elem)
                                except: continue
                        return None
                else:
                    try: return float(s[1]) #return where the value is expected
                    except:
                        for elem in s:
                        #If the value is in a place other than where expected
                            if elem=="": continue
                            elif len(elem)>1: #catches check digits (or whatever the extraneous numbers are)
                                try: return float(elem)
                                except: continue
                        return None
            except: return None

        @staticmethod
        def FtoC(F):
            if type(F)!=float: #In case the input is non-numeric
                try: F=float(F)
                except: return None
            return round((F-32)*5/9,1)

    class ptr(object):
        @staticmethod
        def old(s):
            out={
                "PTR010"    : (float,1,None),
                "PTR010A"   : (float,2,None),
                "PTR025"    : (float,3,None),
                "PTR025A"   : (float,4,None),
                "PTR100"    : (float,5,None),
                "PTR100A"   : (float,6,None)
                }
            try: return read.vals(s,out,len(out)+1)
            except: return None

        def new(s):
            headerChange=   {
                            "PM1.0" : "PM010",
                            "PM2.5" : "PM025",
                            "PM10"  : "PM100"
                            }
            try: 
                sTemp=s.split(',')
                header=sTemp[0]
                header=headerChange[header] #Change header as seen in map above
                out={header : (float,1,None)}
                try: return read.vals(s,out,2)
                except: return out #Return empty dictionary if could not be parsed but connected
            except: return None

    @staticmethod
    def tsi(s):
        out={
            "CPCFLAG"   : (str,4,None),
            "CPC"       : (float,5,None),
            "CPCPULSE"  : (int,11,None),
            "CPC_T"     : (float,-2,None)
            }
        try: return read.vals(s,out,22)
        except: return None

    @staticmethod
    def adi(s):
        out={
            "CPCFLOW"   : (int,-2,None)
            }
        try: return read.vals(s,out,10)
        except: return None

    @staticmethod
    def co2(s): 
        out={
            "CO2" : (int,1,None),      #Stores the data type
            "T"   : (int,2,1/10),   #The place in string
            "RH"  : (int,3,1/10)    #And the multiplier of each parameter
            }
        try: return read.vals(s,out,len(out)+1)
        except: return None

    @staticmethod
    def bcm(s):
        out={
            "BCSZ"          : (float,2,None),
            "BCRZ"          : (float,3,None),
            "BCSB1"         : (float,4,None),
            "BCRB1"         : (float,5,None),
            "BCATN1"        : (float,5,None),
            "BCUVPM"        : (float,6,None),
            "BCSB2"         : (float,7,None),
            "BCRB2"         : (float,8,None),
            "BCATN2"        : (float,9,None),
            "BC"            : (float,10,None),
            "BCFLOW"        : (float,11,None),
            "BCWS"          : (float,12,None),
            "BCWM"          : (int,13,None),
            "BCAT"          : (float,14,None),
            "BCRH"          : (float,15,None),
            "BCBP"          : (float,16,None),
            "BCLEDT"        : (float,17,None),
            "BCDETT"        : (float,19,None)
        }
        try: return read.vals(s,out)
        except: return None

    @staticmethod
    def stat(s):

        outOld={
            "recharge"  : (int,1,None),
            "signal"    : (int,2,None),
            #"ratio"     : (int,3,None),
            #"interval"  : (int,4,None),
            #"pump"      : (int,5,None),
            #"ADC"       : (int,6,None),
            "SDstat"    : (str,7,None),
            #"AUXstat"   : (int,8,None)
            }
        outNew= {
                "recharge"  : (int,1,None),
                "signal"    : (int,2,None),
                #"ratio"     : (int,3,None),
                #"interval"  : (int,4,None),
                #"filter"    : (int,5,None),
                #"pump"      : (int,6,None),
                #"ADC"       : (int,7,None),
                "SDstat"    : (str,8,None),
                #"AUXstat"   : (int,9,None)
                }
        try: return read.vals(s,outNew)
        except:
            try: return read.vals(s,outOld)
            except: return None

    @staticmethod
    def vals(s,param,l=None,dlm=","):
        #Takes a dictionary in the format: {key: (data type(D), place in string(P),multiplier(M))}
        #As well as a string of values
        #Delimits the string, and attempts to populate the dictionary with:
        #Element from the string in place P, converted to data type D, and multiplied by M
        s=s.split(dlm)
        if l!=None and len(s)!=l: return None #Does not parse if the list is of unexpected length
        for key in param:
            (dType,place,mult)=(param[key][0],param[key][1],param[key][2])
            #(data Type to convert to, place in list, multiplier)
            try: 
                val=dType(s[place])
                if mult!=None: val*=mult #In case data type is a string, or such
                if type(val)==float: #Rounds values to 2 or 3 dec. places, depending on magnitude
                    if abs(val)<10: val=round(val,2)
                    else: val=round(val,1)
                if dType==str and not read.checkASCII(val): param[key]=None #Rejects non-ASCII entries
                param[key]=val
            except: param[key]=None #If value wasn't found in the list, do not write
        return param

    @staticmethod
    def transferDictVals(pDict,tgtDict,app=None):
        #Takes two dictionaries with shared keys, 
        #transfers information over from pDict to tgtDict for shared keys
        #Optionally appends the keys in pDict
        #function takes advantage of aliasing and does not return
        for key in pDict:
            if (type(key)==type(app)) : keyTgt=key+app
            else: keyTgt=key
            if keyTgt in tgtDict: tgtDict[keyTgt]=pDict[key]

    @staticmethod
    def noneDict(D):
        #Checks that all of the non-key entries in a dictionary are not None
        #Recursively checks subdicts
        if type(D)!=dict: raise TypeError('Input is not a dictionary') #Check for non-dict entries
        for elem in D:
            entry=D[elem]
            if type(entry)==dict and noneDict(entry)==False: return False #Check subdicts 
            elif entry!=None: return False #Check actual entries
        return True        

    @staticmethod
    def checkASCII(s):
    #Returns true only if all characters are ASCII
        try: return len(s.encode())==len(s)
        except: return False