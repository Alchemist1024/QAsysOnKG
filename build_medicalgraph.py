# -*- encoding: utf-8 -*-
'''
@File        :build_medicalgraph.py
@Time        :2020/12/03 08:45:48
@Author      :xiaoqifeng
@Version     :1.0
@Contact:    :unknown
'''

import os
import json
from py2neo import Graph, Node


class MedicalGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1]) #当前文件上层文件的绝对路径
        self.data_path = os.path.join(cur_dir, 'data/medical.json') #数据的绝对路径
        self.g = Graph(
            host='172.28.1.238', #neo4j 搭载服务器的ip地址，ifconfig可以获得
            http_port=7474, #neo4j 服务器监听的端口号
            user='neo4j', #数据库user name
            password='xqf123'
        )

    def read_nodes(self):
        '''读取文件
        '''
        # 构建节点，共7类节点
        drugs = [] #药品
        foods = [] #食物
        checks = [] #检查
        departments = [] #科室
        producers = [] #药品大类
        symptoms = [] #症状
        disease_infos = [] #疾病信息

        diseases = [] #疾病 这个不是用来构建节点的，是用来保存疾病名称的，在QA阶段会用。

        # 构建节点实体关系
        rels_department = [] #科室-科室关系
        rels_noteat = [] #疾病-忌吃食物关系，数组中保存两个实体，[疾病，忌吃食物]
        rels_doeat = [] #疾病-宜吃食物关系
        rels_recommandeat = [] #疾病-推荐吃食物关系
        rels_commonddrug = [] #疾病-通用药品关系
        rels_recommanddrug = [] #疾病-热门药品关系
        rels_check = [] #疾病-检查关系
        rels_drug_producer = [] #厂商-药物关系

        rels_sympton = [] #疾病症状关系
        rels_acompany = [] #疾病并发关系
        rels_category = [] #疾病与科室之间的关系

        count = 0
        for data in open(self.data_path):
            disease_dict = {} #保存每一种疾病的详细信息，也就是疾病的属性，描述、预防等等
            count += 1
            print('record:' + str(count))

            data_json = json.loads(data)
            
            disease = data_json['name']
            diseases.append(disease)
            
            disease_dict['name'] = disease
            disease_dict['desc'] = ''
            disease_dict['prevent'] = ''
            disease_dict['cause'] = ''
            disease_dict['easy_get'] = ''
            disease_dict['get_prob'] = ''
            disease_dict['cure_department'] = ''
            disease_dict['cure_way'] = ''
            disease_dict['cure_lasttime'] = ''
            disease_dict['symptom'] = '' #这里的症状不是疾病的属性，症状作为单独的节点，所以这个值一直为''
            disease_dict['cured_prob'] = ''
            
            #以下逻辑：某些值不是疾病的属性就建立关系，是疾病的属性就直接赋值
            if 'symptom' in data_json:
                symptoms += data_json['symptom'] #建立节点
                for symptom in data_json['symptom']:
                    rels_sympton.append([disease, symptom]) #建立关系

            if 'acompany' in data_json:
                for acompany in data_json['acompany']:
                    rels_acompany.append([disease, acompany])

            if 'desc' in data_json:
                disease_dict['desc'] = data_json['desc'] #疾病的属性

            if 'prevent' in data_json:
                disease_dict['prevent'] = data_json['prevent']

            if 'cause' in data_json:
                disease_dict['cause'] = data_json['cause']

            if 'get_prob' in data_json:
                disease_dict['get_prob'] = data_json['get_prob']

            if 'easy_get' in data_json:
                disease_dict['easy_get'] = data_json['easy_get']

            if 'cure_department' in data_json:
                cure_department = data_json['cure_department'] #疾病属于什么科
                departments += cure_department
                if len(cure_department) == 1:
                    rels_category.append([disease, cure_department[0]])
                if len(cure_department) == 2:
                    big = cure_department[0]
                    small = cure_department[1]
                    rels_department.append([small, big])
                    rels_category.append([disease, small])

                disease_dict['cure_department'] = cure_department

            if 'cure_way' in data_json:
                disease_dict['cure_way'] = data_json['cure_way']

            if 'cure_lasttime' in data_json:
                disease_dict['cure_lasttime'] = data_json['cure_lasttime']

            if 'cured_prob' in data_json:
                disease_dict['cured_prob'] = data_json['cured_prob']

            if 'common_drug' in data_json:
                common_drug = data_json['common_drug']
                drugs += common_drug
                for drug in common_drug:
                    rels_commonddrug.append([disease, drug])    

            if 'recommand_drug' in data_json:
                recommand_drug = data_json['recommand_drug']
                drugs += recommand_drug
                for drug in recommand_drug:
                    rels_recommanddrug.append([disease, drug])

            if 'not_eat' in data_json:
                not_eat = data_json['not_eat']
                foods += not_eat
                for _not in not_eat:
                    rels_noteat.append([disease, _not])

                do_eat = data_json['do_eat']
                foods += do_eat
                for _do in do_eat:
                    rels_doeat.append([disease, _do])
                
                recommand_eat = data_json['recommand_eat']
                foods += recommand_eat
                for _recommand in recommand_eat:
                    rels_recommandeat.append([disease, _recommand])

            if 'check' in data_json:
                check = data_json['check']
                checks += check
                for _check in check:
                    rels_check.append([disease, _check])

            if 'drug_detail' in data_json:
                drug_detail = data_json['drug_detail']
                producer = [i.split('(')[0] for i in drug_detail] #厂商
                producers += producer
                rels_drug_producer += [[i.split('(')[0], i.split('(')[-1].replace(')', '')] for i in drug_detail]

            disease_infos.append(disease_dict)

        return set(drugs), set(foods), set(checks), set(departments), set(producers), set(symptoms), set(diseases), disease_infos,\
               rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer,\
               rels_recommanddrug, rels_sympton, rels_acompany, rels_category

    def create_node(self, label, nodes):
        '''建立节点
        '''
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print(label, count, len(nodes))
        return

    def create_diseases_nodes(self, disease_infos):
        '''创建知识图谱的中心节点，疾病节点
        '''
        count = 0
        for disease_dict in disease_infos:
            #前面为节点的名字，后面为节点的属性
            node = Node('Disease', name=disease_dict['name'], desc=disease_dict['desc'], prevent=disease_dict['prevent'],
                        cause=disease_dict['cause'], easy_get=disease_dict['easy_get'], cure_lasttime=disease_dict['cure_lasttime'],
                        cure_department=disease_dict['cure_department'], cure_way=disease_dict['cure_way'], cure_prob=disease_dict['cured_prob']) 
            self.g.create(node)
            count += 1
            print('Disease', count, len(disease_infos))
        return

    def create_graphnodes(self):
        '''创建知识图谱实体节点类型schema
        '''
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, disease_infos,rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, rels_recommanddrug,rels_symptom, rels_acompany, rels_category = self.read_nodes()
        self.create_diseases_nodes(disease_infos)
        self.create_node('Drug', Drugs)
        self.create_node('Food', Foods)
        self.create_node('Check', Checks)
        self.create_node('Department', Departments)
        self.create_node('Producer', Producers)
        self.create_node('Symptom', Symptoms)
        return

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        '''创建实体关联边
        '''
        count = 0
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all_ = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s' and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name
            ) #根据cypher命令创建指定节点的边
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all_)
            except Exception as e:
                print(e)
        return

    def create_graphrels(self):
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, disease_infos,rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, rels_recommanddrug,rels_symptom, rels_acompany, rels_category = self.read_nodes()
        self.create_relationship('Disease', 'Food', rels_recommandeat, 'recommand_eat', '推荐食谱') #关系包括type和属性
        self.create_relationship('Disease', 'Food', rels_noteat, 'not_eat', '忌吃')
        self.create_relationship('Disease', 'Food', rels_doeat, 'do_eat', '宜吃')
        self.create_relationship('Department', 'Department', rels_department, 'belongs_to', '属于')
        self.create_relationship('Disease', 'Drug', rels_commonddrug, 'common_drug', '常用药品')
        self.create_relationship('Producer', 'Drug', rels_drug_producer, 'drugs_of', '生产药品')
        self.create_relationship('Disease', 'Drug', rels_recommanddrug, 'recommand_drug', '好评药品')
        self.create_relationship('Disease', 'Check', rels_check, 'need_check', '诊断检查')
        self.create_relationship('Disease', 'Symptom', rels_symptom, 'has_symptom', '症状')
        self.create_relationship('Disease', 'Disease', rels_acompany, 'acompany_with', '并发症')
        self.create_relationship('Disease', 'Department', rels_category, 'belongs_to', '所属科室')

    def export_data(self, data_path):
        '''导出数据到data_path路径下
        '''
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category = self.read_nodes()
        f_drug = open(data_path + 'drug.txt', 'w')
        f_food = open(data_path + 'food.txt', 'w')
        f_check = open(data_path + 'check.txt', 'w')
        f_department = open(data_path + 'department.txt', 'w')
        f_producer = open(data_path + 'producer.txt', 'w')
        f_symptom = open(data_path + 'symptom.txt', 'w')
        f_disease = open(data_path + 'disease.txt', 'w')

        f_drug.write('\n'.join(list(Drugs)))
        f_food.write('\n'.join(list(Foods)))
        f_check.write('\n'.join(list(Checks)))
        f_department.write('\n'.join(list(Departments)))
        f_producer.write('\n'.join(list(Producers)))
        f_symptom.write('\n'.join(list(Symptoms)))
        f_disease.write('\n'.join(list(Diseases)))

        f_drug.close()
        f_food.close()
        f_check.close()
        f_department.close()
        f_producer.close()
        f_symptom.close()
        f_disease.close()

        return


if __name__ == '__main__':
    handler = MedicalGraph()
    print('step1:导入图谱节点中...')
    handler.create_graphnodes()
    print('step2:导入图谱边中...')
    handler.create_graphrels()
    print('step3:导出数据到指定文件夹下中...')
    data_path = 'dict/'
    handler.export_data(data_path)