#! /usr/bin/env python
# emacs-mode: -*- python-*-
# -*- coding: utf-8 -*-

import os 
import sys 
import datetime 
import re 
from GmiAutomationConstants import GmiAutomationConstants 
class IoRoutines():
    __module__ = __name__

    def __init__(self):
        self.constants = GmiAutomationConstants()



    def __del__(self):
        pass


    def appendLinesToFileAndRename(self, fileName, linesToAppend, newFile):
        if ((len(fileName) == 0) or (len(newFile) == 0)):
            raise self.constants.ERROR

        if (not os.path.exists(fileName)):
            raise self.constants.NOSUCHFILE
        try:
            fileLines = self.readFile(fileName)
            if (fileLines == self.constants.READERROR):
                raise 
            newFileLines = (fileLines + linesToAppend)
            self.writeToFile(newFileLines, newFile)
            print newFileLines
            print newFile
        except:
            raise 


    def readFile(self, fileName):
        if (len(fileName) == 0):
            raise self.constants.ERROR
        if (not os.path.exists(fileName)):
            raise self.constants.NOSUCHFILE
        fileContents = []
        try:
            fileObject = open(fileName, 'r')
            fileContents = fileObject.read()
            fileObject.close()
        except:
            return GmiAutomationTools.constants.READERROR
        fileLines = fileContents.splitlines()
        return fileLines



    def writeToFile(self, fileContents, fileName):
        mode = 'a'
        longLine = ''
        for line in fileContents:
            longLine = ((longLine + line) + '\n')

        try:
            fileObject = open(fileName, mode)
            fileObject.write(longLine)
            fileObject.close()
        except:
            raise 



    def touchNewFile(self, fileName):
        try:
            fileObject = open(fileName, 'w')
            fileObject.close()
        except:
            raise self.constants.ERROR



    def errorAndQuit(self, mesg, subject, mailTo, lockFile):
        print mesg
        self.mailMessage(mesg, subject, mailTo)
        os.remove(lockFile)
        sys.exit(-1)



    def mailMessage(self, mesg, subject, mailTo):
        systemCommand = ((((((('echo "' + mesg) + '" | ') + self.constants.MAIL_PATH) + ' -s "') + subject) + '" ') + mailTo)
        systemReturnCode = os.system(systemCommand)



    def printMessage(self, whereTo, mesg):
        if (whereTo == self.constants.STD_ERR):
            sys.stderr.write((mesg + '\n'))
        else:
            sys.stdout.write((mesg + '\n'))


