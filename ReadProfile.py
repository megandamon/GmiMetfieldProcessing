#!/usr/bin/python

import cProfile
import pstats
from time import time, sleep

outputFile="profiles/task.MERRA300.prod.assim20050107.profile"

p = pstats.Stats(outputFile)
p.sort_stats('name')
p.print_stats()
