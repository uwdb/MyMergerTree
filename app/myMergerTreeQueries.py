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
from myria import MyriaError
from raco.language.myrialang import compile_to_json
from raco.scheme import Scheme
from raco.language.myrialang import MyriaQueryScan


connection = MyriaConnection(hostname='rest.myria.cs.washington.edu', port=1776, ssl=True, execution_url='https://myria-web.appspot.com')


def get_unique_times(table):
    relation = MyriaRelation('{table:s}_unique_timesteps'.format(table=table), connection=connection)
    try:
        if len(relation) > 0:
            print "Timestep table already exists"
            return relation.to_dict()
    except MyriaError:
        # relation doesn't exist yet so much create it
        pass
    queryString = ('T1 = DISTINCT([FROM scan({table:s}) as R EMIT R.timeStep]); \n store(T1, {table:s}_unique_timesteps);'.format(table=table))
    print queryString
    query_status = connection.execute_program(program=queryString)
    query_id = query_status['queryId']
    if not query_successful(query_id):
        return {}
    else:
        relation = MyriaRelation('{table:s}_unique_timesteps'.format(table=table), connection=connection)
        return relation.to_dict()


def get_nowgroups_by_mass(user, table, timeStepAttr, nowGroupAttr, massAttr, minMass, maxMass):
    [table_user, table_program, table_name] = table.split(':')
    queryString = 'T1 = DISTINCT([FROM scan({table:s}) as R WHERE R.{timeStepAttr:s} = 1 AND R.{massAttr:s} >= {minMass:f} AND R.{massAttr:s} >= {maxMass:f} EMIT R.{nowGroupAttr:s} as {nowGroupAttr:s}]); \n store(T1, {user:s}:{program:s}:{table2:s}_myMerger_nowGroups);'.format(table=table, nowGroupAttr=nowGroupAttr, timeStepAttr=timeStepAttr, massAttr=massAttr, minMass=float(minMass), maxMass=float(maxMass), user=user, program=table_program, table2=table_name)
    print queryString
    query_status = connection.execute_program(program=queryString)
    query_id = query_status['queryId']
    if not query_successful(query_id):
        return {}
    else:
        print "NO ERROR IN MASS RANGE"
        relation = MyriaRelation('{user:s}:{program:s}:{table2:s}_myMerger_nowGroups'.format(user=user, program=table_program, table2=table_name), connection=connection)
        return relation.to_dict()


def get_mergertree(user, nodesTable, edgesTable, nowGroupAttr, group):
    [nodes_table_user, nodes_table_program, nodes_table_name] = nodesTable.split(':')
    [edges_table_user, edges_table_program, edges_table_name] = edgesTable.split(':')
    queryString = 'nodesT = [FROM scan({nodesTable:s}) as R WHERE R.{nowGroupAttr:s} = {group:s} EMIT R.*]; \n store(nodesT, {user:s}:{nodes_table_program:s}:{nodes_table_name:s}_myMerger_nodes); \n edgesT = [FROM scan({edgesTable:s}) as R WHERE R.{nowGroupAttr:s} = {group:s} EMIT R.*]; \n store(edgesT, {user:s}:{edges_table_program:s}:{edges_table_name:s}_myMerger_edges);'.format(user=user, nodesTable=nodesTable, edgesTable=edgesTable, nowGroupAttr=nowGroupAttr, group=group, nodes_table_program=nodes_table_program, nodes_table_name=nodes_table_name, edges_table_program=edges_table_program, edges_table_name=edges_table_name)
    print queryString
    query_status = connection.execute_program(program=queryString)
    query_id = query_status['queryId']
    if not query_successful(query_id):
        return []
    else:
        print "NO ERROR IN NODES AND EDGES"
        nodes_relation = MyriaRelation('{user:s}:{nodes_table_program:s}:{nodes_table_name:s}_myMerger_nodes'.format(user=user, nodes_table_program=nodes_table_program, nodes_table_name=nodes_table_name), connection=connection)
        edges_relation = MyriaRelation('{user:s}:{edges_table_program:s}:{edges_table_name:s}_myMerger_edges'.format(user=user, edges_table_program=edges_table_program, edges_table_name=edges_table_name), connection=connection)
        return [nodes_relation.to_dict(), edges_relation.to_dict()]


def verify_table(table):
    relations = [item['relationKey'] for item in connection.datasets()]
    for rel in relations:
        if ('{user:s}:{program:s}:{table:s}'.format(user=rel['userName'], program=rel['programName'], table=rel['relationName']) == table):
            return True
    return False


def query_successful(query_id):
    status = (connection.get_query_status(query_id))['status']
    while status != 'SUCCESS':
        status = (connection.get_query_status(query_id))['status']
        time.sleep(2)
        if status == 'ERROR':
            return False
    return True
