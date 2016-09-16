import sys
file_name = sys.argv[1]
file = open(file_name)
one_line = file.readline()
print "The input is: ", one_line
print "Print the last 5 char of the first line:"
for c in one_line[-5:]:
  print ord(c),c
print "If last line is 10: \\n"
print "If last line is 13 10: \\r\\n"
