#!/usr/bin/env python3  
# -*- coding: utf-8 -*-

# from py2neo import Graph,Node,Relationship
from py2neo import PropertyDict
from .. import neo4jendpoint
import re

# TODO 连接neo4j服务器。


def companyPath(sourceCompany, targetCompany, steps):
    '''
    param sourceCompany: 源公司
    :param targetCompany: 目标公司
    :return:
    '''

    cql = '''
            match p=(m:Company{left1}NAME:'{sourceCompany}'{right1})-[r*1..{steps}]-(n:Company{left2}NAME:'{targetCompany}'{right2})
            return extract(n IN nodes(p)| n.NAME) AS company_names,
            extract(n IN nodes(p)|n) AS node_pr,
            extract(r IN relationships(p)|r) AS relation_pr
        '''
    cql = cql.format(left1='{', left2='{', right1='}', right2='}', sourceCompany=sourceCompany, steps=steps,
                     targetCompany=targetCompany)
    results = neo4jendpoint.run(cql)
    # print('results : ------',results)
    # print('共有%d条路径' % len(results))
    # for i, result in enumerate(results):
    #     print(i, result)

    # TODO 问题：存在多条重复边,以及两条边计数

    paths = []
    duplicate_index = []
    for index, result in enumerate(results):
        if len(result['company_names']) == len(list(set(result['company_names']))) and ['company_names'] not in paths:
            # len(result['company_names']) == len(list(set(result['company_names']))) and result
            paths.append(result['company_names'])
        else:
            duplicate_index.append(index)

    new_results = []
    for index, result in enumerate(results):
        if index not in duplicate_index:
            new_results.append(result)
    print(new_results)

    mydata = dict()
    mydata['edges'] = []
    mydata['nodes'] = {}
    for result in new_results:
        nodes, edges = parse_result(result)
        mydata['edges'].extend(edges)
        mydata['nodes'] = {**mydata['nodes'], **nodes}
    path_link = []
    for result in results:
        if result['company_names'] not in path_link:
            path_link.append(result['company_names'])

    return mydata


def parse_result(results):
    nodes = {}
    edges = []
    cID_cName = {}
    node_propertys = results['node_pr']
    link_propertys = results['relation_pr']

    # 解析nodes
    # 可选属性包括：APPR_DATE CREATE_DATE CREDITCODE DOM ENT_LOCATION ENT_STATUS ENT_TYPE ES_DATE ID INDUSTRYCO_CODE INDUSTRYPHY_DES
    # NAME OP_FROM OP_TO ORG_CODES REC_CAP REGCAP REGCAPCUR_DESC REG_CAG_CUR_CODE REG_NO REG_ORG ZSOP_SCOPE type
    for node_property in node_propertys:
        label = list(node_property.labels)[0]
        node_property = PropertyDict(node_property)

        company_name = node_property['NAME']
        info_node = []
        info_node.append(node_property['CREDITCODE'])  # nodes[company_name][0]
        info_node.append(node_property['INDUSTRYPHY_DES'])  # nodes[company_name][1]
        info_node.append(node_property['ES_DATE'])  # nodes[company_name][2]

        nodes[company_name] = info_node
        label_ID = label + '' + str(node_property['ID'])
        cID_cName[label_ID] = company_name

    link_pair = []

    # 解析relationships
    for link_property in link_propertys:


        r = str(link_property)+''
        pattern = re.compile("<Relationship.*nodes=\(<Node.*labels={'([a-zA-Z]+)'}.*'ID': ([0-9]+).*>, <Node.*labels={'([a-zA-Z]+)'}.*'ID': ([0-9]+).*>\)")
        labels = pattern.search(r)

        label1 = labels.group(1) if labels else None
        ID1 = labels.group(2) if labels else None
        label2 = labels.group(3) if labels else None
        ID2 = labels.group(4) if labels else None

        ID_label = {ID1: label1, ID2: label2}

        print(ID_label)

        link_property = PropertyDict(link_property)

        FROM = ID_label[str(link_property['ID_FROM'])] + '' + str(link_property['ID_FROM'])
        TO = ID_label[str(link_property['ID_TO'])] + '' + str(link_property['ID_TO'])
        print(FROM, TO)

        info_edge = {}
        source = cID_cName[FROM]
        target = cID_cName[TO]
        if (source, target) in link_pair or (target, source) in link_pair:
            # print(source, target)
            # TODO 删除原先添加节点
            for edge in edges:
                if (edge['source'], edge['target']) == (source, target) or (edge['target'], edge['source']) == (
                        source, target):
                    edges.remove(edge)
        else:
            link_pair.append((source, target))
            info_edge['source'] = source
            info_edge['target'] = target
            info_edge['create_time'] = link_property['CREATE_TIME']
            info_edge['relation'] = link_property['TYPE']
            info_edge['subconam'] = link_property['SUBCONAM']
            # info_edge['conprop'] = link_property['CONPROP']
            info_edge['con_date'] = link_property['CON_DATE']
            edges.append(info_edge)

    return nodes, edges



