import zlib
import random
import time
import gzip
import lzma
import hashlib
import os
import tempfile
import magic

def gztoxztemp(args):
    xzsize = 0
    (file_, logFile) = args
    Log("File " + file_ + " is being processed...\n", logFile)
    targetFileName = file_[:-3] + ".xz"
    time.sleep(0.2)
    return int(random.uniform(1, 3))

def gztoxz(args):
    xzsize = 0
    (file_, logFile) = args
    time.sleep(random.uniform(1, 3))
    Log("File " + file_ + " is being processed...\n", logFile)
    targetFileName = file_[:-3] + ".xz"
    tempFile = tempfile.NamedTemporaryFile()
    if os.path.isfile(targetFileName):
        Log("File " + targetFileName + " exists.\n", logFile)
        if md5sum(file_, logFile) == md5sum(targetFileName, logFile):
            Log("File " + file_ + " and " + targetFileName + " match.\n", logFile)
            os.remove(file_)
            xzsize = os.stat(targetFileName).st_size
    else:
        if uncompress(file_, tempFile.name, logFile):
            if compress('xz', tempFile.name, targetFileName, logFile):
                if md5sum(file_, logFile) == md5sum(targetFileName, logFile):
                    Log("File " + file_ + " and " + targetFileName + " match\n", logFile)
                    os.remove(file_)
                else:
                    Log("File " + file_ + " and " + targetFileName + " do NOT match\n", logFile)
                xzsize = os.stat(targetFileName).st_size
        else:
            Log("Error during uncompression of file " + file_ + "\n", logFile)
    tempFile.close()
    return xzsize

def compress(algo, file_, targetFileName, logFile):
    fmagic = filemagic(targetFileName)
    time.sleep(random.uniform(1, 3))
    Log("File " + file_ + " is being processed...\n", logFile)
    targetFolder = os.path.dirname(file_)
    if not os.path.exists(targetFolder):
        Log("Directory " + targetFolder + " is being created...\n", logFile)
        os.makedirs(targetFolder)

    try:
        if os.path.isfile(targetFileName) and targetFileName.endswith(".xz"):
            Log("Target file " + targetFileName + " exists.\n", logFile)
            if md5sum(targetFileName, logFile) == md5sum(file_, logFile):
                if 'xz' in fmagic and targetFileName.endswith(".gz"):
                    Log("File " + file_ + " is actually a gzip compressed file.\n", loFile)
                elif 'gzip' in fmagic and targetFileName.endswith(".xz"):
                    Log("File " + file_ + " is actually an XZ compressed file.\n", logFile)
                    Log("File " + file_ + " and " + targetFileName + " are the same.\n", logFile)
            else:
                Log("Target file " + targetFileName + " and source file " + file_ + " are NOT the same.\n", logFile)
        else:
            Log("File " + targetFileName + " is being written...\n", logFile)
            with open(file_, "rb") as sf:
                data = sf.read()
                if algo == 'xz':
                    Log("Compressing using XZ and writing to " + targetFileName + "\n", logFile)
                    try:
                        with lzma.open(targetFileName,'wb') as cf:
                            cf.write(data)
                    except IOError as err:
                        Log("IO Error in file " + targetFileName + ", Error - " + str(err) + "\n", logFile)
                        return False
                else:
                    with gzip.open(targetFileName,'wb') as cf:
                        cf.write(data)
    except IOError as err:
        Log("Compress - IO Error in file " + file_ + ", Error - " + str(err) + "\n", logFile)
        return False

    return True

def uncompress(file_, targetFileName, logFile):
    fmagic = filemagic(file_)
    time.sleep(random.uniform(1, 3))
    try:
        if os.path.isfile(file_):
            Log("File " + targetFileName + " is being written...\n", logFile)
            with open(targetFileName, 'wb') as tf:
                if 'plain' in fmagic:
                    Log("File " + file_ + " is actually an ASCII Text file.\n", logFile)
                    with open(file_, "rb") as f:
                        data = f.read()
                elif 'gzip' in fmagic or file_.endswith(".gz"):
                    Log("Uncompressing " + file_ + " using gzip and writing to " + targetFileName + " has magic - " + fmagic + " \n", logFile)
                    with gzip.open(file_, "rb") as f:
                        data = f.read()
                elif 'xz' in fmagic or file_.endswith(".xz"):
                    if not file_.endswith(".xz"):
                        Log("File " + file_ + " is actually a XZ compressed file.\n", logFile)
                    with lzma.open(file_, "rb") as f:
                        data = f.read()
                tf.write(data)
        return True
    except (zlib.error, EOFError, OSError) as err:
        print(err)
        Log("uncompress - IO Error in file " + file_ + ", Error - " + str(err) + "\n", logFile)
        return False

def md5sum(file_, logFile):
    m = hashlib.md5()
    fmagic = filemagic(file_)
    try:
        if 'plain' in fmagic:
            with open(file_, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    m.update(chunk)
        elif 'gzip' in fmagic or file_.endswith(".gz"):
            with gzip.open(file_, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    m.update(chunk)
        elif 'xz' in fmagic or file_.endswith(".xz"):
            with lzma.open(file_, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    m.update(chunk)
        else:
            with open(file_, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    m.update(chunk)
    except lzma.LZMAError as err:
        Log("LZMA Error in " + file_ + ", Error - " + str(err) + "\n", logFile)
        md5 = ""
    except IOError as err:
        Log("md5sum - IO Error in " + file_ + ", Error - " + str(err) + "\n", logFile)
        md5 = ""
    except EOFError as err:
        Log("md5sum - EOF Error in " + file_ + ", Error - " + str(err) + "\n", logFile)
        md5 = ""
    finally:
        md5 = m.hexdigest()
    return md5

def filemagic(file_):
    ms=magic.open(magic.MAGIC_MIME)
    ms.load()
    return ms.file(file_)

def Log(message, logFile):
#    print(message)
    with open(logFile, "a") as f:
        f.write(message)
