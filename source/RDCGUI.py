#_____CHANGE LOG_____#
    # 2019-05-08: Switched to RDC 1.1.2
    # 2019-04-01
        #Switched to RDC 1.1.1
    # 2018-8-24
        #Code runs from source dir when .py, but from RAMPDIR when .exe/.app
        #Switched to RDC version 0.1.1 

from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
from confReader import config
import platform
import copy
import os
import subprocess
import sys

if getattr(sys,'frozen',False):
    SCRIPTPATH=sys.executable
    SOURCE=os.path.dirname(SCRIPTPATH)
    RAMPDIR = SOURCE
else:
    SCRIPTPATH=os.path.abspath(__file__)
    SOURCE = os.path.dirname(SCRIPTPATH)
    RAMPDIR = os.path.dirname(SOURCE)
# Gives the path of this python script
HOME = os.path.expanduser('~')
if SOURCE.endswith('MacOS'):
    SOURCE=os.path.dirname(os.path.dirname(os.path.dirname(SOURCE)))
SETTINGS = os.path.join(RAMPDIR, 'Settings')
TEMPLATES = os.path.join(SETTINGS, 'templates')
OUTPUT = os.path.join(RAMPDIR,'Output')
TEMPLPATH = os.path.join(TEMPLATES, 'template.ini')  # Get the path of template file
DEFAULT = os.path.join(SETTINGS, 'DefaultsRD.ini')  # Get the path of default settings file
DEPENDPATH = os.path.join(TEMPLATES, 'dependencies.ini')
TOOLTIPPATH = os.path.join(TEMPLATES,'tooltips.ini')
RUNFILE = os.path.join(SETTINGS,'run.ini')
TEMPLATE= config.importDict(TEMPLPATH)
DEPENDENCIES = config.importDict(DEPENDPATH)
TOOLTIPS = config.importDict(TOOLTIPPATH,delimit=False)
DCWNAME = 'RDCauto1.1.2'
for ext in ['.exe','.py']:
    if os.path.exists(os.path.join(SOURCE,(DCWNAME+ext))):
        DCWEXT=ext
        break
    else: continue
DCW=DCWNAME+DCWEXT
DCWPATH = os.path.join(SOURCE,DCW)
MACRUNDCW = ['python3',DCWPATH]
WINRUNDCW = ['python',DCWPATH]


class GUI(Frame):
    def __init__(self, master):
        Frame.__init__(self, master)
        self.master=master
        self.pack()
        self.runDict=None
        self.varDict = copy.deepcopy(TEMPLATE)  # variable dictionary that links parameters to variables and widgets
        self.paramDict = copy.deepcopy(TEMPLATE)  # an empty template dictionary to store all values

        ############# INITIAL UI #######################################################################################
        for section in self.varDict:
            for key,value in self.varDict[section].items():
                self.varDict[section][key]=[StringVar()]

        Frame1=Frame(master)  # A child frame in the main frame that contains all toggle checkbuttons and option buttons
        Frame1.pack(side=LEFT,fill=Y)
        Frame2=Frame(master)  # contains paths,misc and output options
        Frame2.pack(side=RIGHT,fill=Y)

        row = 1
        Frame1.columnconfigure(0,minsize=250)
        fontStyle=('Arial',10)
        # Checkbuttons for toggles. Check dependencies every time a checkbutton is clicked
        Label(Frame1, text='---Toggles---', font=fontStyle).grid(row=row, column=0, sticky=W+E);row += 1
        for param, var in self.varDict['Toggles'].items():
            self.varDict['Toggles'][param].append(Checkbutton(Frame1, text=param, height=1, var=var[0], onvalue='Y', offvalue='N', font=fontStyle, command=self.check))
            var[0].set('N')
            self.varDict['Toggles'][param][1].grid(row=row, column=0, sticky=W);row += 1

        #  Create operation buttons
        row+=1; height=1; width=20; bg1='grey';bg='light grey';bgRun='yellow'
        Frame1.rowconfigure(row-1,minsize=70)
        loadDefBtn=Button(Frame1, text='Load default', font=fontStyle,bg=bg1, command=self.loadDef, height=height,width=width)
        loadDefBtn.grid(row=row, column=0, sticky=W);row += 1
        loadAutorunBtn = Button(Frame1, text='Load Autorun', font=fontStyle,bg=bg, command=self.loadAutorun,
                                height=height, width=width)
        loadAutorunBtn.grid(row=row, column=0, sticky=W); row += 1
        loadOtherBtn=Button(Frame1, text='Load a file', font=fontStyle,bg=bg1, command=self.loadOther, height=height,width=width)
        loadOtherBtn.grid(row=row, column=0, sticky=W);row += 1
        clearBtn=Button(Frame1, text='Clear all', font=fontStyle,bg=bg, command=self.clearAll, height=height,width=width)
        clearBtn.grid(row=row, column=0, sticky=W);row += 1
        saveDefBtn=Button(Frame1, text='Save as default', font=fontStyle,bg=bg1,command=self.saveAsDef, height=height,width=width)
        saveDefBtn.grid(row=row, column=0, sticky=W);row += 1
        saveAutorunBtn = Button(Frame1,text='Save as Autorun', font=fontStyle,bg=bg,command=self.saveAutorun,height=height,width=width)
        saveAutorunBtn.grid(row=row,column=0,sticky=W);row+=1
        saveOtherBtn=Button(Frame1, text='Save to other file', font=fontStyle,bg=bg1, command=self.saveFile, height=height,width=width)
        saveOtherBtn.grid(row=row, column=0, sticky=W);row += 1
        runBtn=Button(Frame1, text='Run', font=('Arial',10,'bold'), bg=bgRun, command=self.runParam, height=height,width=width)
        runBtn.grid(row=row, column=0, sticky=W)

        row = 1; column = 0; columnspan=1
        Frame2.columnconfigure(1,minsize=300)  # Widen the Entry boxes

        # Create label,entry box and browse button for each path parameter
        Label(Frame2, text='---Paths---', font=fontStyle).grid(row=row, column=column+1, sticky=W+E);row += 1
        for param, var in self.varDict['Paths'].items():
            Label(Frame2, text=param, font=fontStyle).grid(row=row, column=column, sticky=E)
            self.varDict['Paths'][param].append(Entry(Frame2, textvariable=var[0], font=fontStyle))
            self.varDict['Paths'][param][1].grid(row=row, column=column + 1, columnspan=columnspan, sticky=W + E)
            if param == 'Raw Directory':
                self.varDict['Paths'][param].append(Button(Frame2, text='Browse', font=fontStyle, command=lambda var=var[0]: self.setMultiDir(var)))
                self.varDict['Paths'][param][2].grid(row=row,column=column + 2,sticky=W)
            else:
                self.varDict['Paths'][param].append(Button(Frame2, text='Browse', font=fontStyle, command=lambda var=var[0]: self.setSingleDir(var)))
                self.varDict['Paths'][param][2].grid(row=row,column=column + 2,sticky=W)
            row += 1

        # Create label and entry box for each Misc parameter
        Label(Frame2, text='---Misc---', font=fontStyle).grid(row=row, column=column+1, sticky=W+E);row += 1
        for param, var in self.varDict['Misc'].items():
            Label(Frame2, text=param, font=fontStyle).grid(row=row, column=column, sticky=E)
            self.varDict['Misc'][param].append(Entry(Frame2, textvariable=var[0], font=fontStyle))
            self.varDict['Misc'][param][1].grid(row=row, column=column + 1, columnspan=columnspan, sticky=W+E)
            row += 1

        # Create label and entry box for each output parameter. Output File Name has an entra browse button
        Label(Frame2, text='---Output---', font=fontStyle).grid(row=row, column=column+1, sticky=W+E);row += 1
        for param, var in self.varDict['Output'].items():
            Label(Frame2, text=param, font=fontStyle).grid(row=row, column=column, sticky=E)
            self.varDict['Output'][param].append(Entry(Frame2, textvariable=var[0], font=fontStyle))
            self.varDict['Output'][param][1].grid(row=row, column=column + 1,columnspan=columnspan, sticky=W+E)
            if param == 'Output File Name':
                self.varDict['Output'][param].append(Button(Frame2, text='Browse', font=fontStyle, command=lambda var=var[0]: self.getFileName(var)))
                self.varDict['Output'][param][2].grid(row=row,column=column + 2,sticky=W)
            row += 1

        for section in self.varDict:
            for key in self.varDict[section]:
                if key in TOOLTIPS[section] and TOOLTIPS[section][key]:
                    ToolTip(self.varDict[section][key][1],TOOLTIPS[section][key])
        ################################################################################################################

    def loadOther(self):  # load an .ini file with configReader into the entry boxes
        file = filedialog.askopenfilename(initialdir=SETTINGS,
                                          filetypes=(("ini files", "*.ini"), ("All files", "*")))
        # a filedialog that asks user to choose a '.ini' file
        if file != '':  # if selection is not empty(i.e. the user did not click on the 'cancel' button)
            loadPopup = LoadPopup(self.master, 'Loading')
            loadPopup.update()
            try:
                outDict = self.loader(file)  # import the selected file to a dictionary
                self.editFrame(outDict)  # modify the UI to display loaded parameters
                self.check()
            except:
                messagebox.showerror('Error', 'Unable to load %s' % file)
            loadPopup.destroy()

    def loadDef(self):
        # load Defaults.ini with configReader.
        loadPopup = LoadPopup(self.master, 'Loading')
        loadPopup.update()
        try:
            outDict = self.loader(DEFAULT)
            self.editFrame(outDict)
            self.check()
        except:
            messagebox.showerror('Error', 'Unable to load default configuration')
        loadPopup.destroy()

    def loadAutorun(self):
        loadPopup = LoadPopup(self.master, 'Loading')
        loadPopup.update()
        try:
            outDict = self.loader(RUNFILE)
            self.editFrame(outDict)
            self.check()
        except:
            messagebox.showerror('Error', 'Unable to load Autorun parameter')
        loadPopup.destroy()

    def saveFile(self):
        # run the validation first. If all inputs are valid, popup a saving window to save the configuration
        (validate, param) = self.completeValidate()
        if validate is True:
            saveFileName=filedialog.asksaveasfilename(initialdir=SETTINGS,defaultextension='.ini')
            if saveFileName:
                if not saveFileName.endswith('.ini'): saveFileName+='.ini'
                param.save(saveFileName, path=True)
                messagebox.showinfo('Confirm', 'Configuration has been successfully saved!')
        else: pass  # if validation fails, save window will not show until validated

    def saveAsDef(self):
        # run the validation first. If all inputs are valid, save to 'Defaults.ini' automatically
        (validate, param) = self.completeValidate()
        if validate is True:
            try:
                param.save(DEFAULT, path=True)
                messagebox.showinfo('Confirm', 'Configuration has been successfully saved!')
            except:
                messagebox.showerror('Error', 'File can not be saved, please try again')
        else: pass

    def saveAutorun(self):
        (validate, param) = self.completeValidate()
        if validate is True:
            try:
                param.save(RUNFILE, path=True)
                self.runDict=param.wDict
                messagebox.showinfo('Confirm', 'Configuration has been successfully saved!')
                return self.runDict
            except:
                messagebox.showerror('Error', 'File can not be saved, please try again')
        else: pass

    def runParam(self):
        # After validation of input, run 'RAMP Data Cleaner WIP.py' with current configuration.
        (validate, param) = self.completeValidate()
        self.runDict=self.loader(RUNFILE)
        if validate is True:
            loadPopup = LoadPopup(self.master, 'Running')
            loadPopup.update()
            param.save(RUNFILE, path=True)
            if os.path.exists(DCWPATH):
                if DCWEXT != '.py':
                    subprocess.call(DCWPATH)
                else:
                    if platform.system() == 'Darwin':
                        os.system(MACRUNDCW)  # run the main data cleaner script
                    elif platform.system() == 'Windows':
                        subprocess.call(WINRUNDCW)
                config.write.toPath(RUNFILE,self.runDict)
                loadPopup.destroy()
                messagebox.showinfo('Complete', 'Data Cleaner Processing Complete')
            else:
                loadPopup.destroy()
                messagebox.showerror('Error','Fail to run RAMP Data Cleaner, please check if %s is under source file' % DCW)
        else:
            pass

    def editFrame(self, outDict):  # Display loaded parameters when clicking 'Load' buttons
        for section in self.varDict:
            for key, var in self.varDict[section].items():
                if key in outDict[section]:
                      # Set entry variables to match the imported file dictionary
                    if type(outDict[section][key])==list:  # if the value is a list, separate them by commas
                        var[0].set(','.join(outDict[section][key]))
                    elif outDict[section][key] is None: var[0].set('')
                    else: var[0].set(outDict[section][key])
                else: var[0].set('')  # if UI parameters not in imported dictionary, set to empty

    def clearAll(self):  # Clear all inputs, set all variables to ''
        for section in self.varDict:
            if section == 'Toggles':
                for key, var in self.varDict[section].items():
                    var[0].set('N')
            else:
                for key, var in self.varDict[section].items():
                    var[0].set('')

    def getInput(self):  # pull user inputs to the empty dictionary
        for section in self.paramDict:
            for key, value in self.paramDict[section].items():
                if key in self.varDict[section]:  # first get all the GUI inputs and load into param dictionary
                    self.paramDict[section][key] = self.varDict[section][key][0].get()
        for key,value in self.paramDict['Toggles'].items():
            self.paramDict['Toggles'][key] = config.pull.toggle(value)
        # then convert values in param dictionary to desired format
        for key,value in self.paramDict['Paths'].items():
            self.paramDict['Paths'][key] = config.pull.path(key,value)
        miscDict = config.pull.miscDict()
        for key, value in self.paramDict['Misc'].items():
            self.paramDict['Misc'][key] = miscDict[key](value)
        for key, value in self.paramDict['Output'].items():
            if key == 'Output File Name':
                if os.path.isabs(value): self.paramDict['Output'][key] = os.path.basename(value)
                else: continue
            self.paramDict['Output'][key] = config.pull.output(key, value)
        return self.paramDict

    def completeValidate(self):
        # verify outputting dictionary using complete validation, generate a popup message box with all error messages
        paramDict = self.getInput()  # get all inputs into paramDict
        param = config(paramDict, RAMPDIR)  # define config object
        try:  # run the complete verify module and get writing, error and warning dictionaries
            (param.wDict, param.errorDict, param.warningDict) = param.verify.complete\
                (param.fDict, TEMPLATE, DEPENDENCIES,RAMPDIR)
        except:
            messagebox.showerror('Error','Verification failed, please check your input')
            return False, None
        newDict=self.restoreDict(copy.deepcopy(param.wDict))  # restore the param dictionary to writable values
        errorText = []  # append all error messages for different parameters to a list
        for section in param.errorDict:
            for key, value in param.errorDict[section].items():
                if value is None: continue
                else: errorText.append("'%s' Error: '%s'" % (key, value))
        warningText=[]  # append all warning messages for different parameters to a list
        for section in param.warningDict:
            for key, value in param.warningDict[section].items():
                if value is None: continue
                else: warningText.append("'%s' Warning: '%s'" % (key, value))
        self.editFrame(newDict)
        self.paramDict=copy.deepcopy(newDict)
        if errorText:  # popup a message box with all the error messages, verification fails
            messagebox.showerror('Error', '\n'.join(errorText))
            return False, None
        elif warningText:  # popup a message box with warnings, user can dismiss the warnings and continue
            warningPopup=WarningPopup(self.master,'\n'.join(warningText))
            warningPopup.wait_window()
            if warning is True: return True,param
            else: return False,None
        else:
            return True, param

    def check(self):  # check dependencies and modify corresponding configurations upon clicking a checkbutton
        for key,value in self.varDict['Toggles'].items():
            if key in DEPENDENCIES:  # if the parameter is targeted in dependencies
                if value[0].get() == DEPENDENCIES[key]['setting'].upper():
                        # if the input matches the dependencies prerequisite,
                        # set all related widgets to preset values in dependencies then disable the widgets
                    self.disable(key)
                else:  # if the input does not match the dependencies prerequisite, enable all related widgets
                    self.enable(key)
            if self.varDict['Toggles']['Output Format File'][0].get() == 'N':
                self.varDict['Output']['Output File Name'][0].set('')
                self.varDict['Output']['Output File Name'][1].config(state='disabled')
                self.varDict['Output']['Output File Name'][2].config(state='disabled')

    def disable(self,key):  # disable related widgets and set their values as specified in dependencies
        for param,value in DEPENDENCIES[key].items():
            if value is None:  # if values in dependencies are None, set to '' and disable
                for section in self.varDict:
                    if param in self.varDict[section]:
                        self.varDict[section][param][0].set('')
                        for widget in self.varDict[section][param][1:]: widget.config(state='disabled')
            elif value.lower() == 'valid':
                for section in self.varDict:
                    if param in self.varDict[section]:
                        for widget in self.varDict[section][param][1:]: widget.config(state='normal')
            else:  # if toggles in dependencies are 'N', set to '0'and disable checkbuttons
                for section in self.varDict:
                    if param in self.varDict[section]:
                        self.varDict[section][param][0].set(value)
                        for widget in self.varDict[section][param][1:]: widget.config(state='disabled')

    def enable(self,key):  # enable all related widgets in dependencies.
        for param,value in DEPENDENCIES[key].items():
            for section in self.varDict:
                if param in self.varDict[section]:
                    for widget in self.varDict[section][param][1:]: widget.config(state='normal')

    @staticmethod
    def loader(file):  # load file using smart loader
        param = config(TEMPLATE,RAMPDIR)
        param.load(file,check=True,dependencies=DEPENDENCIES)
        return GUI.restoreDict(param.wDict)

    @staticmethod
    def restoreDict(dictionary):  # restore to writable dictionary
        dictionary["Toggles"] = config.write.toggles2YN(dictionary["Toggles"])
        dictionary["Misc"]["Ramp Nums"] = config.write.ramp2MixedList(dictionary["Misc"]["Ramp Nums"])
        dictionary["Misc"]["Date Range"] = config.write.date2MixedList(dictionary["Misc"]["Date Range"])
        return dictionary

    @staticmethod
    def setMultiDir(var):  # Ask for multiple directories and display in the Entry box.
        FileDir = []
        for dir in var.get().split(','):  # retrieve the content in entry box
            if os.path.exists(dir) and dir not in FileDir: # if the existing path is valid, append the new directory to the directory list
                FileDir.append(dir)
            else: continue
        newDir = filedialog.askdirectory(initialdir=HOME)  # ask user to select a new directory
        if newDir != '' and newDir not in FileDir:  # append new dir to the list
            FileDir.append(newDir)
        FileDir = ','.join(FileDir)  # convert the list elements to a string
        var.set(FileDir)

    @staticmethod
    def setSingleDir(var):
        # Ask for a single directory and display in the Entry box.
        folder = filedialog.askdirectory(initialdir=HOME)
        if folder!='':
            var.set(folder)

    @staticmethod
    def getFileName(var):
        filePath = filedialog.askopenfilename(initialdir=OUTPUT, defaultextension='.ini',
                                                filetypes=(("ini files", "*.ini"), ("All files", "*")))
        if filePath!='':
            filename=os.path.basename(filePath)
            var.set(filename)

    @staticmethod
    def center(window):  # center popup windows on screen
        windowWidth=window.winfo_reqwidth()
        windowHeight=window.winfo_reqheight()
        positionRight=int(window.winfo_screenwidth()/2-windowWidth/2)
        positionDown=int(window.winfo_screenheight()/2-windowHeight/2)
        window.geometry("+{}+{}".format(positionRight,positionDown))


class LoadPopup(Toplevel):
    def __init__(self, master, cmd):
        Toplevel.__init__(self, master)
        self.title(cmd)
        GUI.center(self)
        self.master = master
        Message(self,text='%s...Please wait...'% cmd,width=200,borderwidth=20).grid(row=1,sticky=W+E)


class WarningPopup(Toplevel):
    def __init__(self,master,warning):
        Toplevel.__init__(self,master)
        self.title('Warning')
        GUI.center(self)
        Message(self,text=warning,width=500).grid(row=0,columnspan=2,sticky=W+E)
        Button(self,text='Dismiss and continue',command=self.dismiss,width=20).grid(row=1,column=1,sticky=E)
        Button(self,text='Go back',command=self.goBack,width=20).grid(row=2,column=1,sticky=E)

    def dismiss(self):
        global warning
        warning = True
        self.destroy()

    def goBack(self):
        global warning
        warning = False
        self.destroy()


class ToolTip(object):
    def __init__(self,widget,text):
        self.waittime = 500  # miliseconds
        self.wraplength = 300  # pixels
        self.widget = widget
        self.text = text
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)
        self.widget.bind("<ButtonPress>", self.leave)
        self.id = None
        self.tw = None

    def enter(self,event=None):
        self.schedule()

    def leave(self,event=None):
        self.unschedule()
        self.hidetip()

    def schedule(self):
        self.unschedule()
        self.id=self.widget.after(self.waittime,self.showtip)

    def unschedule(self):
        id = self.id
        self.id = None
        if id:
            self.widget.after_cancel(id)

    def showtip(self, event=None):
        x = y = 0
        x, y, cx, cy = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 20
        # creates a toplevel window
        self.tw = Toplevel(self.widget)
        # Leaves only the label and removes the app window
        self.tw.wm_overrideredirect(True)
        self.tw.wm_geometry("+%d+%d" % (x, y))
        label = Label(self.tw, text=self.text, justify='left',
                         background="#ffffff", relief='solid', borderwidth=1,
                         wraplength=self.wraplength)
        label.pack(ipadx=1)

    def hidetip(self):
        tw = self.tw
        self.tw = None
        if tw:
            tw.destroy()


if __name__ == '__main__':
    root = Tk()
    root.title('Ramp Data Cleaner Configuration')
    gui = GUI(root)
    gui.mainloop()
