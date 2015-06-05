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
from raco.language.myrialang import compile_to_json
from raco.scheme import Scheme
from raco.language.myrialang import MyriaQueryScan

#CONFIGURE: information about the datasets in Myria
SNAPSHOT_LIST=['002560','002552','002432']
USER_NAME="jortiz"
PROGRAM_NAME="romulustest"

#parameters
DM_SOL_UNIT = 7.374469e13
NON_GRP_PARTICLES = '0'

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
	current_snapshot = "(select s1.grp as grpID," + str(time_count) + " as timeStep, sum(s1.mass) as mass, count(*) as totalParticles from \""+ current_relation_name + "\" as s1 where s1.grp >" + NON_GRP_PARTICLES + " group by s1.grp)"
	if(union_string):
		union_string = union_string + " UNION " + current_snapshot
	else:
		union_string = current_snapshot
	time_count = time_count + 1

catalog = FromFileCatalog.load_from_file("schema.py")
_parser = parser.Parser()

#Run the first query
current_query = "T1 = empty(x:int); store(T1, " + relation_name_prefix + "nodesLocal);"

statement_list = _parser.parse(current_query);
processor = interpreter.StatementProcessor(catalog, True)
processor.evaluate(statement_list)
p2 = processor.get_logical_plan()
p2 = processor.get_physical_plan()
p2.input = MyriaQueryScan(sql=union_string, scheme=Scheme([('grpID', 'LONG_TYPE'), ('timeStep', 'LONG_TYPE'), ('mass', 'FLOAT_TYPE'), ('totalParticles','LONG_TYPE')]))
finalplan = compile_to_json('create nodes', p2, p2)

print "RUNNING LOCAL NODES"
query_status= connection.submit_query(finalplan)
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
global_nodes_query = "T1 = [from scan(" + relation_name_prefix + "nodesLocal) as n emit grpID, timeStep, sum(mass)* " + format(DM_SOL_UNIT, '.1f') + " as mass, sum(totalParticles) as totalParticles]; store(T1, " +  relation_name_prefix +  "nodesTable);"
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
