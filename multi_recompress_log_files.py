#!/usr/bin/env python3
import os
import sys
import time
from compress import gztoxz
from multiprocessing import cpu_count, Pool
from datetime import datetime
from progressbar import ProgressBar, SimpleProgress, Bar

def startProcessing(mydir):
    # Create datetime object with date older than date and calculate time
    olderThanDate = datetime(2016, 6 ,30)
    olderThanDays = datetime.now() - olderThanDate
    olderThan = time.time() - (olderThanDays.days * 86400)
    startTime = datetime.now()
    print(startTime)
    logFile = "/home/sganji/recompress.log"
    filelist = []
    try:
        gzFileSize = 0
        print("Starting Collecting log file information")
        # Find all the files in the dir that end with .gz
        for root, dirs, files in os.walk(mydir,topdown=True):
            for thefile in files:
                if thefile.endswith('.gz'):
                    fullpath = os.path.join(root, thefile)
                    try:
                        stat = os.stat(fullpath)
                        gzFileSize += stat.st_size
                        if stat.st_mtime <= olderThan:
                            filelist.append((fullpath, logFile))
                    except FileNotFoundError:
                        print("File {} not found. It may be a broken link.".format(fullpath))

        # Start file compression
        print("Starting file compression.")
        # Use only 50% of the available CPUs
        pool = Pool(int(cpu_count() / 1.2))
        # Use an iterable mapping function
        result = pool.imap(gztoxz, filelist)
        # Close the pool
        pool.close()
        numfiles = len(filelist)
        print("Number of files: {}".format(numfiles))
        # Init progressbar
        pbar = ProgressBar(widgets=[SimpleProgress(), Bar()], maxval=numfiles)
        pbar.start()
        # Keep track of completed files
        while (True):
            completed = result._index
            if (completed == numfiles): break
            pbar.update(completed)
            time.sleep(1)
        pbar.finish()

        # Find the file size of all XZ files returned from the gztoxz compress
        # function
        xzFileSize = 0
        for size in result:
            xzFileSize += size

        # Output the compression stats
        print("Total GZ File Size is - {}.".format(gzFileSize))
        print("Total XZ File Size is - {}.".format(xzFileSize))
        print("Saved {}% of space.".format(100*(xzFileSize/gzFileSize)))

        print("Finished file compression.")
    except KeyboardInterrupt:
        # Handle Ctrl + C or Keyboard Interrupt
        print("Number of files: {}".format(numfiles))
        print("Caught Break Signal")
        exit(0)
    finally:
        # Print final/closing stats
        print("Done collecting log file information")
        endTime = datetime.now()
        print(endTime - startTime)
        print(endTime)

if __name__ == "__main__":
    # Check if the arguments are correct
    if len(sys.argv) < 2:
        print("No Folders specified.")
        exit(1)
    else:
        # Skip the script name
        mydirs = sys.argv[1:]

    # Check if the user wants to continue with the process
    print("Start scanning directory {} and compress files?".format(mydirs))
    choice = input("Do you want to continue: [y/n]" )
    if choice in ["N","n"]:
        exit(0)

    # For all directories, process the files
    for mydir in mydirs:
        if not os.path.exists(mydir):
            print("{} is not a directory!".format(mydir))
            exit(1)
        else:
            startProcessing(mydir)
