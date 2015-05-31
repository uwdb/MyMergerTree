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


connection = MyriaConnection(hostname = "rest.myria.cs.washington.edu", port=1776, ssl=True, execution_url="https://myria-web.appspot.com")

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
#submit the query
print "/***** DELETING SPLITS QUERY*****/";
print "PART 1"
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
	print 'QUERY SUCCESS 1/2'
else:
	print 'QUERY ERROR'

print "PART 2"
ranking = "apply RunningRank(haloGrp) {  [0 as _rank, -1 as _grp]; [case when haloGrp = _grp then _rank + 1 else 1 end, case when haloGrp = _grp then _grp else haloGrp end]; _rank;};"
rankedEdges = "rankedEdges = [from scan(" + table_prefix + "edgesConnectedSplitSort) as e emit e.*,  RunningRank(e.nextGroup) as splitOrder];"
edgesTree = "edges = [from rankedEdges as e where splitOrder = 1 emit e.*];"
store = "store(edges," + table_prefix + "edgesFinal);"
query_status =  connection.execute_program(program=ranking + rankedEdges + edgesTree + store)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

while status!='SUCCESS':
	status = (connection.get_query_status(query_id))['status']
	time.sleep(2);
	if status=='ERROR':
		break;

if status=='SUCCESS':
	print 'QUERY SUCCESS 2/2'
else:
	print 'QUERY ERROR'


#Second Query: take care of the splits -- 
print "/*****FINDING PROGENITORS QUERY*****/";
print "PART 1"
edges = "edges = scan(" + table_prefix + "edgesFinal);"
progenitors = "progenitors = [from edges where currentTime = 1 emit nowGroup, currentTime as currentTime, currentGroup];"
maxShared = "maxShared = [from edges emit nowGroup, currentTime, currentGroup, max(sharedParticleCount) as maxSharedParticleCount];"
I = "I = [2 as i];"
maxTime = "maxTime = [from edges emit max(currentTime)+1 as maxT];"
loop = "do nextGroups = [from progenitors p, edges e, I where e.nowGroup = p.nowGroup and e.currentGroup = p.currentGroup and e.currentTime = p.currentTime and p.currentTime + 1 = I.i emit nowGroup, currentTime, currentGroup, max(sharedParticleCount) as maxShared]; maxGroup = [from nextGroups g, edges e where e.currentTime = g.currentTime and e.nowGroup = g.nowGroup and e.currentGroup = g.currentGroup and e.sharedParticleCount = g.maxShared emit e.nowGroup, e.currentTime+1 as currentTime, min(e.nextGroup) as currentGroup]; progenitors = distinct(progenitors + maxGroup); I = [from I emit i+1 as i]; while [from I, maxTime where I.i <= maxTime.maxT emit count(*) > 0];"
store = "store(progenitors, " + table_prefix + "progen);"
query_status = connection.execute_program(program=edges + progenitors + maxShared + I + maxShared + I + maxTime + loop + store)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

while status!='SUCCESS':
	status = (connection.get_query_status(query_id))['status']
	time.sleep(2);
	if status=='ERROR':
		break;

if status=='SUCCESS':
	print 'QUERY SUCCESS 1/2'
else:
	print 'QUERY ERROR'

print "PART 2"
edges = "edges = scan(" + table_prefix + "edgesFinal);"
progenitors = "progenitors = scan(" + table_prefix + "progen);"
haloTable1 = "haloTable1 = [from scan("+ table_prefix+"nodesTable) h, edges e where h.grpID = e.currentGroup and h.timeStep = e.currentTime emit e.nowGroup, h.*];"
haloTable2 = "haloTable2 =  [from scan(" + table_prefix + "nodesTable) h, edges e where h.grpID = e.nextGroup and h.timeStep = e.currentTime+1 emit e.nowGroup, h.*];"
haloTable = "haloTable = distinct(haloTable1 + haloTable2);"
findandLabelProg = "findProg = [from haloTable h, progenitors p where h.nowGroup = p.nowGroup and h.grpID = p.currentGroup and h.timeStep = p.currentTime emit h.*]; labelProg = [from findProg as f emit f.*, 1 as prog]; findNonProg = diff(haloTable,findProg); labelNonProg = [from findNonProg as f emit f.*, 0 as prog]; haloTableNew = labelProg + labelNonProg; final = [from haloTableNew emit *, -1 as massRatio];"
store = "store(final,"+ table_prefix + "haloTableProg);"
query_status = connection.execute_program(program=edges + progenitors + haloTable1 + haloTable2 + haloTable + findandLabelProg + store)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

while status!='SUCCESS':
	status = (connection.get_query_status(query_id))['status']
	time.sleep(2);
	if status=='ERROR':
		break;

if status=='SUCCESS':
	print 'QUERY SUCCESS 2/2'
else:
	print 'QUERY ERROR'


print "/*****MAJOR MERGERS QUERY*****/";

edgesFinal = table_prefix + "edgesFinal"
haloTableProg = table_prefix + "haloTableProg"

#first get schemas, create the catalog
f = open('schema.py', 'w')
f.write("{" + '\n');
#--nodes
current_schema = (MyriaRelation(relation= edgesFinal, connection=connection).schema.to_dict())
columnNames = [x.encode('utf-8') for x in current_schema['columnNames']]
columnTypes = [x.encode('utf-8') for x in current_schema['columnTypes']]
columns = zip(columnNames, columnTypes)
f.write("'" + edgesFinal + "' : " +  str(columns) + ',\n');
#--edges
current_schema = (MyriaRelation(relation=haloTableProg, connection=connection).schema.to_dict())
columnNames = [x.encode('utf-8') for x in current_schema['columnNames']]
columnTypes = [x.encode('utf-8') for x in current_schema['columnTypes']]
columns = zip(columnNames, columnTypes)
f.write("'" + haloTableProg + "' : " +  str(columns) + ',\n');
f.write("}" + '\n');
f.close()

catalog = FromFileCatalog.load_from_file("schema.py")
_parser = parser.Parser()

current_query = "sortByChildMass = [from scan("+ edgesFinal + ") as e, scan(" + haloTableProg+ ") as h where timeStep = currentTime + 1 and h.grpID = e.nextGroup and h.nowGroup = e.nowGroup emit nowGroup, currentTime, currentGroup, nextGroup, mass as nextGroupMass]; store(sortByChildMass," + table_prefix + "edgesWithMassSort);"

statement_list = _parser.parse(current_query);
processor = interpreter.StatementProcessor(catalog, True)
processor.evaluate(statement_list)

#modify and get the plans
p = processor.get_logical_plan()
tail = p.args[0].input
p.args[0].input = alg.Shuffle(tail, [UnnamedAttributeRef(0), UnnamedAttributeRef(1), UnnamedAttributeRef(2)])
p.args[0].input = alg.OrderBy(p.args[0].input, [0,1,2,4], [True, True, True, False])
p = processor.get_physical_plan()
finalplan = processor.get_json()

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
	print 'QUERY SUCCESS 1/2'
else:
	print 'QUERY ERROR'

#MASS RATIO -- 

runningRank = "apply RunningRank(haloGrp) { [0 as _rank, 0 as _grp]; [case when haloGrp = _grp then _rank + 1 else 1 end, case when haloGrp = _grp then _grp else haloGrp end]; _rank;};"
haloTable = "haloTable = scan(" + table_prefix + "haloTableProg);"
rankedEdges = "rankedEdges =  [from scan(" + table_prefix + "edgesWithMassSort) as e emit e.nowGroup, e.currentTime,  RunningRank(e.currentGroup) as splitOrder, e.currentGroup, e.nextGroup, e.nextGroupMass];"
edges1 = "edges1 = [from rankedEdges where splitOrder = 1 emit *];"
edges2 = "edges2 = [from rankedEdges where splitOrder = 2 emit *];"
bothMaxGroups = " bothMaxGroups = [from edges1 e1, edges2 e2 where e1.nowGroup = e2.nowGroup and e1.currentTime = e2.currentTime and e1.currentGroup = e2.currentGroup emit e1.nowGroup, e1.currentTime, e1.currentGroup, e1.nextGroup as e1NextGroup, e2.nextGroup as e2NextGroup, (e1.nextGroupMass/e2.nextGroupMass) as massRatio];"
maxMasses1 = "maxMasses1 = [from bothMaxGroups g, haloTable h where h.nowGroup = g.nowGroup and h.grpID = g.e1NextGroup and h.timeStep = g.currentTime+1 emit h.*, g.massRatio*1.0 as massRatio];"
maxMasses2 = "maxMasses2 = [from bothMaxGroups g, haloTable h where h.nowGroup = g.nowGroup and h.grpID = g.e2NextGroup and h.timeStep = g.currentTime+1 emit h.*, g.massRatio*1.0 as massRatio];"
maxMasses = "maxMasses = maxMasses1 + maxMasses2;"
remainingHalos = "remainingHalos = diff([from haloTable h emit h.*], [from maxMasses h emit h.*]);"
remainingHalosMass = "remainingHalosMass = [from remainingHalos h emit h.*, -1+0.0 as massRatio];"
newHaloTable = "newHaloTable = remainingHalosMass + maxMasses;"
store = "store(newHaloTable, " + table_prefix + "haloTableFinal);"

query_status = connection.execute_program(program=runningRank + haloTable + rankedEdges + edges1 + edges2 + bothMaxGroups + maxMasses1 + maxMasses2 + maxMasses + remainingHalos + remainingHalosMass + newHaloTable+ store)
query_id = query_status['queryId']
status = (connection.get_query_status(query_id))['status']

while status!='SUCCESS':
	status = (connection.get_query_status(query_id))['status']
	time.sleep(2);
	if status=='ERROR':
		break;

if status=='SUCCESS':
	print 'QUERY SUCCESS 2/2'
else:
	print 'QUERY ERROR'