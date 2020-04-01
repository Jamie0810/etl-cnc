from lib import *

def moveFilesToSuccDir(tempDir, currentDate):
    if (os.path.exists(currentDateDir) == False):
        os.mkdir(currentDateDir)
    succDir = currentDateDir + '/successful'
    if (os.path.exists(succDir) == False):
        os.mkdir(succDir)
    for fileName in os.listdir(tempDir):
        if (fileName.endswith('.log')):
            shutil.move(tempDir + fileName, succDir)

def moveFilesToFailDir():
    if (os.path.exists(currentDateDir) == False):
            os.mkdir(currentDateDir)
    failDir = currentDateDir + '/fail' 
    if (os.path.exists(failDir) == False):
        os.mkdir(failDir)
    for fileName in os.listdir(tempDir):
        if (fileName.endswith('.log')):
            shutil.move(tempDir + fileName, failDir)