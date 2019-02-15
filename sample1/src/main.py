import sys
import os
from os.path import join, getsize
from os import stat
import json
import logging
import hashlib
#
# set up logging
#
##################################################################
#         Globals
##################################################################

logger = logging.getLogger("main")
logging.basicConfig(level=logging.DEBUG)
handler = logging.FileHandler('pythian.log')
handler.setLevel(logging.INFO)
logger.addHandler(handler)

DirectoryList1 = []
DirectoryList2 = []
DirectoryList3 = []
DirectoryList4 = []

##################################################################
#         Services
##################################################################

class FileInfo(object):
  def __init__(self,full_path,inode,hash="none"):
    self.full_path=full_path;
    self.inode=inode;
    self.hash = hash;
    self.group = "1"
    self.filecontent = "none"
    
def compare_files(file1, file2):
  return false;

def read_file(filename):
  with open(filename, "r") as filehandle:  
      filecontent = filehandle.read()
      return filecontent
 
def get_hash( data):
   hash_object = hashlib.md5(data)
   hash_object.hexdigest();
   return hash_object

def print_hash():      
    for key,value in DirectoryHashMap.items():
      print value.full_path
      print value.inode
      print value.hash

##################################################################
#   Functional Modules
##################################################################

def step1_read_fs( directory):
  logger.debug("step1: path:{}".format(directory))
  for root, dirs, files in os.walk(directory, topdown=True, onerror=None, followlinks = True):  
    for filename in files:
        #logger.debug("step1: processing:{}".format(filename))
        full_path = os.path.join(directory, filename)
        x = read_file(full_path)
        inode = stat(join(root, full_path));
        fileInfo = FileInfo(full_path,inode);
        DirectoryList1.append(fileInfo);
  logger.debug("step1: ending")

##################################################################
def step2_sort_by_size():

  def compare(x):
    return x.inode.st_size

  logger.debug("step2: starting")
  DirectoryList1.sort(key=compare)
  logger.debug("step2: ending")

##################################################################
def step3_assign_size_group():
  logger.debug("step3: starting")
  length = len(DirectoryList1)
  i = 0
  group=1
  for x in DirectoryList1:
    if (i > 0):
      last = DirectoryList1[i-1]
      last_size = last.inode.st_size
      current = DirectoryList1[i]
      current_size = current.inode.st_size
      if (last_size != current_size):
        group = group +1
      current.group = group 
    i=i+1
  logger.debug("step3: ending")

##################################################################
def step4_prune_group():
  logger.debug("step4: starting")
  length = len(DirectoryList1)
  i = 0
  size = len(DirectoryList1)
  while i < size:
    if (i > 0):
      last = DirectoryList1[i-1]
      last_group = last.group
      current = DirectoryList1[i]
      current_group = current.group
      if (last_group == current_group):
        print "equal",last,current
        DirectoryList2.append(last)
        DirectoryList2.append(current)
        i=i+1
    i=i+1

  logger.debug("step4: ending")
##################################################################
def step5_create_digest():
  logger.debug("step5: starting")
  i=0
  for x in DirectoryList2:
      print x
      data = read_file(x.full_path)
      x.filecontent=data
      x.hash = get_hash(data);
      DirectoryList2[i]=x
      i=i+1;
  logger.debug("step5: ending")
##################################################################
def step6_sort_by_hash():

  def compare(x):
    return str(x.hash.digest())

  logger.debug("step6: starting")
  DirectoryList2.sort(key=compare)
  logger.debug("step6: ending")

##################################################################
def step7_assign_digest_group():
  logger.debug("step7: starting")

  size = len(DirectoryList2)
  i=0
  while i < size:
    if (i > 0):
      last = DirectoryList2[i-1]
      last_hash = last.hash.digest()
      current = DirectoryList2[i]
      current_hash = current.hash.digest()
      print last,current,last_hash, current_hash
      if (last_hash == current_hash):
        print "equal",last,current,current_hash
        DirectoryList3.append(last)
        DirectoryList3.append(current)
        i=i+1
    i=i+1

  logger.debug("step7: ending")

##################################################################
def step8_compare_content():
  logger.debug("step8: starting")

  size = len(DirectoryList3)
  i=0
  while i < size:
    if (i > 0):
      last = DirectoryList3[i-1]
      last_content = last.filecontent
      current = DirectoryList3[i]
      current_content = current.filecontent
      ## print last,current,last_content, current_content
      if (last_content == current_content):
        print "equal",last,current
        DirectoryList4.append(last)
        DirectoryList4.append(current)
        i=i+1
    i=i+1

  logger.debug("step8: ending")

##################################################################
def step9_produce_output():
  logger.debug("step8: starting")
  print " *********************  Duplicate Files ***************"
  for x in DirectoryList4:
    print x.full_path
  print " *********************  Duplicate Files ***************"
  logger.debug("step8: ending")

##################################################################
#  Main Program
##################################################################
def main(path,threads):
  logger.info("** starting **")  
  step1_read_fs(path)
  step2_sort_by_size()
  step3_assign_size_group()
  step4_prune_group()
  if (len(DirectoryList2) > 0 ):
     step5_create_digest()
     step6_sort_by_hash()
     step7_assign_digest_group()
     step8_compare_content()  
     step9_produce_output()
  logger.info("** dedup: ending **")  

##################################################################
#   Process Command line arguements and invoke main
##################################################################
if __name__ == "__main__":
   #
   # Get Parameters
   #
   path = sys.argv[1];
   logger.info( "path:{}".format(path));
   threads = sys.argv[2] ;
   logger.info( "threads:{}".format(threads));
   # 
   # launch the processing
   #
   print "\n*\n* Starting Execution\n* root:{} threads:{}\n*".format(path,threads)
   cwd = os.getcwd();
   abs_path = join(cwd,path)
   main(abs_path,threads)
