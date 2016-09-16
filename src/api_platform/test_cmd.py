import subprocess
import shlex
import sys

def runProcess(exe):    
    p = subprocess.Popen(exe, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while(True):
      retcode = p.poll() #returns None while subprocess is running
      line = p.stdout.readline()
      yield line
      if(retcode is not None):
        break
    print "\nFinish process"

# Make it configable
client = "/Users/willwywang-NB/github/cell_signalling_analysis/tool/odps/bin/odpscmd"

file_name = sys.argv[1]

# use sql file
cmd = """
%s -f "%s";
""" % (client, file_name)

print "cmd: ",cmd
args =  shlex.split(cmd)
print "args: ",args

for line in runProcess(args):
    print line,
