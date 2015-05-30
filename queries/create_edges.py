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

NON_GRP_PARTICLES = '-1'
IORDER = 'iOrder'
#note, might need to change to iord or iOrder

#END CONFIGURE

connection = MyriaConnection(hostname = "rest.myria.cs.washington.edu", port=1776, ssl=True, execution_url="https://myria-web.appspot.com")

time_count = 1
union_string = None

relation_name_prefix = USER_NAME + ":" + PROGRAM_NAME + ":"
#Write the union
for i in range(len(SNAPSHOT_LIST)-1):
	current_relation_name =  relation_name_prefix + "cosmo" + SNAPSHOT_LIST[i];
	next_relation_name = relation_name_prefix + "cosmo" + SNAPSHOT_LIST[i+1];
	current_snapshot = "(select s1.grp as currentGroup," + str(time_count) + " as currrentTime, s2.grp as nextGroup, count(*) as sharedParticles from \"" + current_relation_name + "\" s1, \"" + next_relation_name + "\" s2 where s1.\"" +  IORDER  + "\" = s2.\"" +  IORDER  + "\" and s1.grp > " + NON_GRP_PARTICLES + " and s2.grp >" + NON_GRP_PARTICLES + " group by s1.grp, s2.grp)"
	if(union_string):
		union_string = union_string + " UNION " + current_snapshot
	else:
		union_string = current_snapshot
	time_count = time_count + 1

with open('local_edges_template.json', 'r+') as data_file:    
    local_nodes_json_query = json.load(data_file)
    data_file.close()

#make modifications
local_nodes_json_query['plan']['fragments'][0]['operators'][0]['sql'] = union_string
local_nodes_json_query['plan']['fragments'][0]['operators'][1]['relationKey']['userName'] = USER_NAME
local_nodes_json_query['plan']['fragments'][0]['operators'][1]['relationKey']['programName'] = PROGRAM_NAME
local_nodes_json_query['plan']['fragments'][0]['operators'][1]['relationKey']['relationName'] = 'edgesLocal'

#run local query
print "RUNNING LOCAL EDGES"
query_status= connection.submit_query(local_nodes_json_query)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

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
print "RUNNING GLOBAL EDGES"
global_edges_query = "T1 = [from scan(" + relation_name_prefix + "edgesLocal) as n emit currentGroup, currentTime, nextGroup, sum(sharedParticles) as sharedParticleCount]; store(T1, " +  relation_name_prefix +  "globalEdges);"
query_status = connection.execute_program(program=global_edges_query)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

while status!='SUCCESS':
	status = (connection.get_query_status(query_id))['status']
	time.sleep(2);
	if status=='ERROR':
		break;

if status=='SUCCESS':
	print 'QUERY SUCCESS'
else:
	print 'QUERY ERROR'


#running connectedEdges
print "RUNNING ITERATIVE EDGES QUERY"
nodes_query = "T1 = [from scan(" + relation_name_prefix + "globalEdges) as n where currentTime = 1 emit currentGroup as nowGroup, currentTime, currentGroup, nextGroup, sharedParticleCount];"
iterator = "I = [1 as i];"
maximumTime = "maxTime = [from " + relation_name_prefix + "globalEdges emit max(currentTime) as maxT];"
loop = "do delta = [from edges as e1," + relation_name_prefix + "globalEdges as e2, I where e1.nextGroup = e2.currentGroup and e1.currentTime+1 = e2.currentTime and e1.currentTime = I.i emit e1.nowGroup, e2.currentTime, e2.currentGroup, e2.nextGroup, e2.sharedParticleCount]; edges = distinct(delta + edges); I = [from I emit i+1 as i]; while [from I, maxTime where I.i < maxTime.maxT emit count(*) > 0];"
final_store = "store(edges, " +relation_name_prefix + "edgesTable);"
final_query = nodes_query + iterator + maximumTime + loop + final_store
print final_query
#query_status = connection.execute_program(program=nodes_query + iterator + maximumTime + loop + final_store)
#query_id = query_status['queryId']
#status = (connection.get_query_status(query_id))['status']


