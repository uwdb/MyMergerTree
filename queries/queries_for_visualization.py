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
USER_NAME="jortiz"
PROGRAM_NAME="romulustest"
NODES_RELATION="nodesTable"
EDGES_RELATION="edgesTable"
#END CONFIGURE

table_prefix = USER_NAME + ":" + PROGRAM_NAME + ":"
nodes_table = table_prefix + NODES_RELATION;
edges_table = table_prefix + EDGES_RELATION;


connection = MyriaConnection(hostname = "rest.myria.cs.washington.edu", port=1776, ssl=True)

#first get schemas, create the catalog
f = open('schema.py', 'w')
f.write("{" + '\n');
#--nodes
current_schema = (MyriaRelation(relation=nodes_table, connection=connection).schema.to_dict())
columnNames = [x.encode('utf-8') for x in current_schema['columnNames']]
columnTypes = [x.encode('utf-8') for x in current_schema['columnTypes']]
columns = zip(columnNames, columnTypes)
f.write("'" + nodes_table + "' : " +  str(columns) + ',\n');
#--edges
current_schema = (MyriaRelation(relation=edges_table, connection=connection).schema.to_dict())
columnNames = [x.encode('utf-8') for x in current_schema['columnNames']]
columnTypes = [x.encode('utf-8') for x in current_schema['columnTypes']]
columns = zip(columnNames, columnTypes)
f.write("'" + edges_table + "' : " +  str(columns) + ',\n');
f.write("}" + '\n');
f.close()

catalog = FromFileCatalog.load_from_file("schema.py")
_parser = parser.Parser()

#First Query: take care of the splits -- 
##(take edges and sort them)
current_query = "T1 = scan("+ edges_table + "); store(T1," + table_prefix + "edgesConnectedSplitSort);"

statement_list = _parser.parse(current_query);
processor = interpreter.StatementProcessor(catalog, True)
processor.evaluate(statement_list)
p = processor.get_logical_plan()

tail = p.args[0].input
p.args[0].input = alg.Shuffle(tail, [UnnamedAttributeRef(0)])
p.args[0].input = alg.OrderBy(p.args[0].input, [0,1,3,4], [True, True, True, False])
p = processor.get_physical_plan()
finalplan = processor.get_json()
print finalplan
#submit the query
print "/***** SPLIT QUERY*****/";
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

