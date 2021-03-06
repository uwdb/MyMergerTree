import sys, os, json
import time
from raco.catalog import FromFileCatalog
import raco.myrial.parser as parser
import raco.myrial.interpreter as interpreter
import raco.algebra as alg
from raco.expression.expression import UnnamedAttributeRef
from myria import MyriaConnection
from myria import MyriaSchema
from myria import MyriaRelation
from raco.language.myrialang import compile_to_json
from raco.scheme import Scheme
from raco.language.myrialang import MyriaQueryScan

#CONFIGURE: information about the datasets in Myria (first snapshot must be most "recent")
SNAPSHOT_LIST=['002560','002552','002432']
USER_NAME="jortiz"
PROGRAM_NAME="romulustest"

NON_GRP_PARTICLES = '0'
IORDER = 'iOrder'
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

catalog = FromFileCatalog.load_from_file("schema.py")
_parser = parser.Parser()

#Run the first query
current_query = "T1 = empty(x:int); store(T1, " + relation_name_prefix + "edgesLocal);"

statement_list = _parser.parse(current_query);
processor = interpreter.StatementProcessor(catalog, True)
processor.evaluate(statement_list)
p2 = processor.get_logical_plan()
p2 = processor.get_physical_plan()
p2.input = MyriaQueryScan(sql=union_string, scheme=Scheme([('currentGroup', 'LONG_TYPE'), ('currentTime', 'LONG_TYPE'), ('nextGroup', 'LONG_TYPE'), ('sharedParticles','LONG_TYPE')]))
finalplan = compile_to_json('create edges', p2, p2)

print "RUNNING LOCAL EDGES"
query_status= connection.submit_query(finalplan)
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
global_edges = "scan(" + relation_name_prefix + "globalEdges)"

edges = "edges = [from "+ global_edges+ " as e where currentTime = 1 emit currentGroup as nowGroup, currentTime, currentGroup, nextGroup, sharedParticleCount];"
I = "I = [1 as i];"
maxTime = "maxTime = [from "+ global_edges+ " as e emit max(currentTime) as maxT];"
loop = "do delta = [from edges as e1," + global_edges + " as e2, I where e1.nextGroup = e2.currentGroup and e1.currentTime+1 = e2.currentTime and e1.currentTime = I.i emit e1.nowGroup, e2.currentTime, e2.currentGroup, e2.nextGroup, e2.sharedParticleCount]; edges = distinct(delta + edges); I = [from I emit i+1 as i]; while [from I, maxTime where I.i < maxTime.maxT emit count(*) > 0];"
store = "store(edges, " +relation_name_prefix + "edgesTable);"
connecting_edges = edges + I + maxTime + loop + store
query_status = connection.execute_program(program=connecting_edges)
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