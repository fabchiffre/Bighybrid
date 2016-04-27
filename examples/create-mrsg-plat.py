#!/usr/bin/python

import sys
import string
import random

if len(sys.argv) < 8:
	print 'Usage:', sys.argv[0], 'platform_file.xml num_workers cores_per_node_min[:numCores_max] cpu_min[:cpu_max] latency_min[:latency_max] bw_min[:bw_max] diskSize:diskBwrite:diskBRead:diskBconnection'
	print 'Ex. Homogeneous :', sys.argv[0], 'plat.xml 5 2 1e9 1e-4 1.25e8 500GB:30MBps:100MBps:150MBps'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-cpu_var.xml 10 2 4e9:7e9 1e-4 1.25e8 500GB:30MBps:100MBps:150MBps'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-BW_var.xml 10 2 7e9 1e-4 1.25e6:1.25e8 500GB:30MBps:100MBps:150MBps'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-Lat_var.xml 10 2 7e9 1e-4:1e-2 1.25e8 500GB:30MBps:100MBps:150MBps'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-net_var.xml 10 2 7e9 1e-4:1e-2 1.25e6:1.25e8 500GB:30MBps:100MBps:150MBps'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-ALL_var.xml 10 2 4e9:7e9 1e-4:1e-2 1.25e6:1.25e8 500GB:30MBps:100MBps:150MBps'
#	print 'Distribution_name',, sys.argv[0], 'uniform, beta, expo, gamma, gauss, logn, weibull'
	sys.exit(1)

# Command line arguments.
outFileName = sys.argv[1]
numNodes = int(sys.argv[2]) + 1
numCores = string.split(sys.argv[3], ':')
for i in range(len(numCores)):
	numCores[i] = int(numCores[i])
cpu = string.split(sys.argv[4], ':')
for i in range(len(cpu)):
	cpu[i] = float(cpu[i])
latency = string.split(sys.argv[5], ':')
for i in range(len(latency)):
	latency[i] = float(latency[i])
bandwidth = string.split(sys.argv[6], ':')
for i in range(len(bandwidth)):
	bandwidth[i] = float(bandwidth[i])
diskProperties = string.split(sys.argv[7], ':')

# Header
output = open(outFileName, 'w')
output.write('<?xml version=\'1.0\'?>\n')
output.write('<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid.dtd">\n')
output.write('<platform version="3">\n')
output.write('  <AS id="AS1" routing="Full">\n')

random.seed()

#Storage type definition.
output.write('\n')
output.write('\t<storage_type id="MRSG_disk_type" model="linear" size="' + diskProperties[0] +'" content="content/storage_content.txt" >\n')
output.write('\t\t<model_prop id="Bwrite" value="' + diskProperties[1] +'" />\n')
output.write('\t\t<model_prop id="Bread" value="' + diskProperties[2] +'" />\n')
output.write('\t\t<model_prop id="Bconnection" value="' + diskProperties[3] + '" />\n')
output.write('\t</storage_type>\n')



#Disks definition.
output.write('\n')
for i in range(numNodes):
	output.write('\t<storage id="MRSG_Disk' + str(i) + '" typeId="MRSG_disk_type" attach="MRSG_Host' + str(i) + '" />\n')

# Nodes definition.
output.write('\n')
for i in range(numNodes):
	nbCpu = cpu[0] if (len(cpu) == 1) else random.uniform(cpu[0], cpu[1])
	nbCores = numCores[0] if (len(numCores) == 1) else  random.randrange(numCores[0], numCores[1],2)

	output.write('\t<host id="MRSG_Host' + str(i) + '" power="' + str(nbCpu) + '" core="' + str(nbCores) + '" >\n')
	output.write('\t\t<mount storageId="MRSG_Disk' + str(i) + '" name="/" />\n')
	output.write('\t</host>\n')

# Links definition.
output.write('\n')
if len(bandwidth) == 1:
	for i in range(1,numNodes):
		 output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(bandwidth[0]) + '" latency="' + str(latency[0]) + '" />\n')

elif len(latency) ==1:
	for i in range(1,numNodes):
		rBW = random.uniform (bandwidth[0], bandwidth[1])
		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(rBW) + '" latency="' + str(latency[0]) + '" />\n')
		

elif (len(bandwidth) == 1 and len(latency) ==1):
	for i in range(1,numNodes):
		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(bandwidth[0]) + '" latency="' + str(latency[0]) + '" />\n')

else:
	for i in range(1,numNodes):
		rBW = random.uniform (bandwidth[0], bandwidth[1])
		rLat = random.uniform (latency[0], latency[1])
		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(rBW) + '" latency="' + str(rLat) + '" />\n')

#if else:
#	for i in range(1,numNodes):
#		rBW = random.uniform (bandwidth[0], bandwidth[1])
#		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(rBW) + '" latency="' + latency + '" />\n')

# Topology (paths) definition.
output.write('\n')
for src in range(numNodes):
	for dst in range(numNodes):
		if src != dst:
			output.write('\t<route src="MRSG_Host' + str(src) + '" dst="MRSG_Host' + str(dst) + '">\n')
			if (src == 0):
				output.write('\t\t<link_ctn id="l' + str(dst) + '"/>\n')
			elif (dst == 0):
				output.write('\t\t<link_ctn id="l' + str(src) + '"/>\n')
			else:
				output.write('\t\t<link_ctn id="l' + str(src) + '"/>\n')
				output.write('\t\t<link_ctn id="l' + str(dst) + '"/>\n')
			output.write('\t</route>\n')

# Footer
output.write('\n  </AS>\n')
output.write('</platform>\n')
output.close()
