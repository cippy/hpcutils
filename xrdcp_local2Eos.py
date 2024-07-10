#!/usr/bin/env python3

import os
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("-j","--jobs", type=int, default=32)
parser.add_argument("--dryRun", action='store_true', help="Print command but don't copy")
parser.add_argument("source", type=str, help="Source path. Can specify an existing file containing a list of input files using 'file=list.ext'")
parser.add_argument("dest", type=str)

args = parser.parse_args();

infilesBase = []

for f in os.listdir(args.source):
    if not f.endswith(".root"): continue
    fullFile = os.path.join(args.source, f)
    if os.path.isfile(fullFile):
        infilesBase.append(f)
        
prefix = "root://eoscms.cern.ch//"
destPath = args.dest
if destPath.startswith("/eos/cms/store/"):
    destPath = prefix + "/".join(destPath.split("/")[3:])

outfiles = [os.path.join(destPath, f) for f in infilesBase]

infiles = [os.path.join(args.source, f) for f in infilesBase]

if args.dryRun:
    print(f"Will run the following command on {args.jobs} threads (example for the first file):")
    print(f"--> xrdcp {infiles[0]} {outfiles[0]}")
    exit(0)

def xrdcp(files):
    subprocess.run(["xrdcp", files[0], files[1]])


with ThreadPoolExecutor(max_workers=args.jobs) as executor:
    executor.map(xrdcp, zip(infiles,outfiles))
