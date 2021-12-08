import pandas as pd
import numpy as np
import random

with open('/Users/xueyuan/Desktop/dataset.txt', 'w') as f:
    for _ in range(1000):
        feature1 = random.random()
        feature2 = random.random()
        lb = 2*feature1 + feature2 + random.random()
        to_write = str(feature1)+' '+str(feature2)+','+str(lb)+'\n'
        f.write(to_write)
f.close()    