#!/usr/bin/python

import cProfile
import pstats
from time import time, sleep

def someFunction ():
    sleep (30)
    

print("Profiler")
cProfile.run('someFunction ()', 'profile.out')
p = pstats.Stats('profile.out')
p.sort_stats('name')
p.print_stats()
