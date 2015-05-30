import sys, os, json
import time
lib_path = os.path.abspath(os.path.join('../libs/raco', '../libs/myria-python'))
sys.path.append(lib_path)
from raco.catalog import FromFileCatalog
import raco.myrial.parser as parser
import raco.myrial.interpreter as interpreter
import raco.algebra as alg
from raco.expression.expression import UnnamedAttributeRef
from myria import MyriaConnection
from myria import MyriaSchema
from myria import MyriaRelation

#CONFIGURE: information about the datasets in Myria
SNAPSHOT_LIST=['000045','000054','000072']
USER_NAME="jortiz"
PROGRAM_NAME="romulustest"

#parameters
DM_SOL_UNIT = 7.374469e13
NON_GRP_PARTICLES = '-1'

#queries
#implied: grp, time, mass * dm_sol_unit, totalParticles
EXTRA_ATTRIBUTES = []
#END CONFIGURE

connection = MyriaConnection(hostname = "rest.myria.cs.washington.edu", port=1776, ssl=True, execution_url="https://myria-web.appspot.com")

time_count = 1
union_string = None

relation_name_prefix = USER_NAME + ":" + PROGRAM_NAME + ":"
#Write the union
for i in SNAPSHOT_LIST:
	#dbscan query?
	current_relation_name =  relation_name_prefix + "cosmo" + i;
	#after totalParticles, would have extra attributes
	current_snapshot = "(select s1.grp as grpID," + str(time_count) + "as currentTime, sum(s1.mass) as mass, count(*) as totalParticles from \""+ current_relation_name + "\" as s1 where s1.grp >" + NON_GRP_PARTICLES + " group by s1.grp)"
	if(union_string):
		union_string = union_string + " UNION " + current_snapshot
	else:
		union_string = current_snapshot
	time_count = time_count + 1

#prepare local_nodes schema (if needed, add more attributes in this section)

#write the union at postgres layer (from template query) for local_nodes
with open('local_nodes_template.json', 'r+') as data_file:    
    local_nodes_json_query = json.load(data_file)
    data_file.close()

#make modifications
local_nodes_json_query['plan']['fragments'][0]['operators'][0]['sql'] = union_string
local_nodes_json_query['plan']['fragments'][0]['operators'][1]['relationKey']['userName'] = USER_NAME
local_nodes_json_query['plan']['fragments'][0]['operators'][1]['relationKey']['programName'] = PROGRAM_NAME
local_nodes_json_query['plan']['fragments'][0]['operators'][1]['relationKey']['relationName'] = 'nodesLocal'

#run local query
print "RUNNING LOCAL NODES"
query_status= connection.submit_query(local_nodes_json_query)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

#keep checking, sleep a little
while status!='SUCCESS':
	status = (connection.get_query_status(query_id))['status']
	time.sleep(2);
	if status=='ERROR':
		break;

if status=='SUCCESS':
	print 'QUERY SUCCESS'
else:
	print 'QUERY ERROR'

#global myrial query
print "RUNNING GLOBAL NODES"
global_nodes_query = "T1 = [from scan(" + relation_name_prefix + "nodesLocal) as n emit grpID, currentTime, sum(mass)*" + format(DM_SOL_UNIT, '.1f') + " as mass, sum(totalParticles) as totalParticles]; store(T1, " +  relation_name_prefix +  "nodesTable);"
query_status = connection.execute_program(program=global_nodes_query)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

#keep checking, sleep a little
while status!='SUCCESS':
	status = (connection.get_query_status(query_id))['status']
	time.sleep(5);
	if status=='ERROR':
		break;

if status=='SUCCESS':
	print 'QUERY SUCCESS'
else:
	print 'QUERY ERROR'
