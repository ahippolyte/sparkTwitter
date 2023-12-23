#!/usr/bin/env python3

import os
# import numpy as np
# import pandas as pd

# SIMULATION PARAMETERS #

# Relative path to executables
BINARY_PATH = "../"

# CSV output file
DATA_DIRECTORY = "data"
DATA_FILE = "simulation.csv"

# Number of repetitions for each experience
repetitions = 10

# Parameters for the simulation :
# as kernels, versions and sizes are tables,
# the simulation is going to make an experience
# with each combination of these parameters
parameters = {
    "kernels": ["top_20_hashtags", "hashtag_friends"],
    "num-executors" : [1, 2, 3],
    "executor-cores" : [1, 2, 3, 4],
}
#-----------------------#

f = open(DATA_DIRECTORY + "/" + DATA_FILE, "a")
f.write("kernel;num-executors;executor-cores;time\n")
f.close()

print("Starting simulation...")
for kernel in parameters["kernels"]:
    for numexecutors in parameters["num-executors"]:
        for executorcores in parameters["executor-cores"]:
            for i in range(repetitions):
                print("spark-submit --master yarn " + BINARY_PATH + kernel + ".py " + ' --num-executors ' + str(numexecutors) + ' --executor-cores ' + str(executorcores))
                output = os.popen("spark-submit --master yarn " + BINARY_PATH + kernel + ".py " + ' --num-executors ' + str(numexecutors) + ' --executor-cores ' + str(executorcores)).read()
                lines = output.splitlines()
                time = lines[len(lines)-1]
                f = open(DATA_DIRECTORY + "/" + DATA_FILE, "a")
                f.write(kernel + ';' + str(numexecutors) + ';' + str(executorcores) + ';' + time + '\n')
                f.close()
print("Simulation done.")

