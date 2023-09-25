#!/usr/bin/env python3

import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("-j","--jobs", type=int, default=32)
parser.add_argument("-r","--recursive", action='store_true')
parser.add_argument("-e", "--empty", action='store_true')
parser.add_argument("-s", "--server", type=str, default = "eoscms.cern.ch") # can try cms-xrd-global.cern.ch
parser.add_argument("--maxFiles", type=int, default=None)
parser.add_argument("--dryRun", action='store_true', help="Print command but don't copy")
parser.add_argument("source", type=str, help="Source path. Can specify an existing file containing a list of input files using 'file=list.ext'")
parser.add_argument("dest", type=str)

args = parser.parse_args();

lsfilenames = []

sourceIsInputList = False
if args.source.startswith("file="):
    sourceIsInputList = True
    # this is useful when dumping a list of files from DAS, with typical format as the following
    # /store/mc/RunIISummer20UL16NanoAODv9/QCD_Pt-20_MuEnrichedPt15_TuneCP5_13TeV-pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/2520000/9645FB1D-15D5-F145-945F-29BBB8A447DD.root
    # the check on the size done below in the other scope is not needed in this case
    with open(args.source.split("=")[1], "r") as f:
        lsfilenames = [line.rstrip() for line in f]
    if not args.dryRun:
        print("Using this list of files:")
        print(lsfilenames)

else:

    cmds = ["xrdfs", args.server, "ls", "-l"]

    if args.recursive:
        cmds += ["-R"]

    cmds.append(args.source)
    print("Command to find all files:", cmds)

    res = subprocess.run(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res.check_returncode()
    lsfiles = str(res.stdout, 'utf-8').splitlines()

    lsfilenames = []
    for f in lsfiles:
        fsplit = f.split(" ")
        filesize = fsplit[-2]
        filename = fsplit[-1]
        if args.empty or filesize != "0":
            lsfilenames.append(filename)

if args.maxFiles and args.maxFiles < len(lsfilenames):
    print(f"INFO: copying the first {args.maxFiles} of {len(lsfilenames)} valid files")
    lsfilenames = lsfilenames[:args.maxFiles]

if sourceIsInputList:
    # take one file as template, keep everything until file name with its last folder
    basedir = lsfilenames[0].split("/")[:-2]
else:
    basedir = args.source.rstrip("/").split("/")[:-1]

basedir = "/".join(basedir)
outfiles = [f.replace(basedir, args.dest) for f in lsfilenames]
infiles = [f"root://{args.server}/{f}" for f in lsfilenames]

if args.dryRun:
    print(f"Will run the following command on {args.jobs} threads (example for the first file):")
    print(f"--> xrdcp {infiles[0]} {outfiles[0]}")
    exit(0)

def xrdcp(files):
    subprocess.run(["xrdcp", files[0], files[1]])


with ThreadPoolExecutor(max_workers=args.jobs) as executor:
    executor.map(xrdcp, zip(infiles,outfiles))
    

