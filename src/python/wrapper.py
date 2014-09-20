#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyaccumulo import Accumulo, Mutation, Range
import settings
import sys
import random

QUEUED="queued"
INPROGRESS="inprogress"
COMPLETED="completed"
Q="wq"

def remove_and_update(cq,q,a,b):
  mut = Mutation(q)
  mut.put(cf=a,cq=cq,is_delete=True)
  mut.put(cf=b,cq=cq)
  return conn.write(table,mut)
  

def randtask(q,state,x):
  n=random.randint(0,x)
  entry=None
  for entry in conn.scan(table,scanrange=Range(srow=q,erow=q),cols=[[state]]):
    if n == 0:
      break
    else:
      n=n-1
  if entry is None:
    return None
  else:
    return entry.cq


conn = Accumulo(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD)
table = settings.TABLE

if sys.argv[1] == "-c":
  print "create"
  wr = conn.create_batch_writer(table)
  i=0
  q="%s:%s"%(Q,sys.argv[2])
  mut = Mutation(q)
  for entry in conn.batch_scan(table,cols=[["Genome","md5"]],numthreads=10):
     genome=entry.row
     if i%1000 == 0:
       print entry.row
     mut.put(cf=QUEUED,cq=genome)
     i=i+1
  wr.add_mutation(mut)
  wr.close()
  exit()

if sys.argv[1] == "-r":
  print "recover"
  q="%s:%s"%(Q,sys.argv[2])
  genome=randtask(q,INPROGRESS,10)
  while genome:
    print genome
    # Fork and run script
    rc=remove_and_update(genome,q,INPROGRESS,COMPLETED)
    genome=randtask(q,INPROGRESS,10)
    




func=sys.argv[1]
q="%s:%s"%(Q,func)
genome=randtask(q,QUEUED,10)
while genome:
  print genome
  rc=remove_and_update(genome,q, QUEUED, INPROGRESS)
  # Fork and run script
  subprocess.call([sys.argv[2],genome])
  rc=remove_and_update(genome,q,INPROGRESS,COMPLETED)
  genome=randtask(q,QUEUED,10)


conn.close()


