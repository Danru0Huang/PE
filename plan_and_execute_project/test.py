from py2neo import Node, Relationship
import pandas as pd
from fuzzywuzzy import fuzz
from py2neo import Graph
from langchain.tools import tools
graph = Graph("bolt://121.40.186.242:7687/", auth=("neo4j", "123456"))
class IDGenerator:
    @staticmethod
    def generate_id(prefix):
        """
        生成带有固定前缀的唯一 ID。
        :param prefix: 节点类型前缀，例如 'SD'、'DA'、'DV'
        :return: 格式化的唯一 ID（例如 SD001, DA002, DV003）
        """
        query = f"""
        MATCH (n:{prefix}ID)
        RETURN n.name AS count
        """
        result = graph.run(query)
        current_count = result.evaluate()
        if current_count:
            count = int(current_count)
        else:
            count = 1
            # 创建新的 ID 节点
            graph.run(f"CREATE (n:{prefix}ID {{name: {count}}})")
        # 更新 ID 节点
        graph.run(f"MERGE (n:{prefix}ID) SET n.name = {count + 1}")
        return f"{prefix}{count:03d}"

def add_sub_domain_info(filename):
    """
    创建子域节点，子域的名称为文件名，并添加 ID。
    :param filename: 文件名称
    :return: 子域节点
    """
    # 创建子域节点，并生成 ID
    sub_domain_id = IDGenerator.generate_id("SD")  # 使用 "SD" 前缀生成子域 ID
    node_sub_domain = Node("子域", name=filename, id=sub_domain_id)
    graph.merge(node_sub_domain, "子域", "id")  # 使用 merge 避免重复创建
    return node_sub_domain

def register_data_from_file(file, filename):
    """
    从文件读取数据，创建数据节点并与子域节点建立关系。
    :param file: 文件
    :param filename: 文件名
    :return: 注册提示信息
    """
    # 创建子域节点
    sub_domain_node = add_sub_domain_info(filename)
    
    # 读取文件中的数据，跳过第一行表头
    df = pd.read_excel(file, header=0)  # 假设第一行为表头
    
    for _, row in df.iterrows():
        data_name = row.iloc[0]  # 数据名称
        data_values = str(row.iloc[1]).split("，") if not pd.isna(row.iloc[1]) else []  # 数据值以逗号分隔

        # 创建数据节点并添加 ID
        if data_name:  # 确保数据名称存在
            data_id = IDGenerator.generate_id("DA")  # 使用 "DA" 前缀生成数据 ID
            node_data = Node("数据", value=data_name, id=data_id)
            graph.merge(node_data, "数据", "id")  # 使用 merge 以确保节点不重复
            
            # 如果数据值存在，创建数据值节点并建立关系
            if data_values:
                for value in data_values:
                    if value:  # 确保值不为空
                        value_id = IDGenerator.generate_id("DV")  # 使用 "DV" 前缀生成数据值 ID
                        node_value = Node("数据值", value=value, id=value_id)
                        graph.merge(node_value, "数据值", "id")  # 使用 merge 
                        
                        # 创建数据与数据值之间的关系
                        relation = Relationship(node_data, "包含值", node_value)
                        graph.merge(relation)
            
            # 创建子域与数据节点之间的关系
            relation = Relationship(sub_domain_node, "包含", node_data)
            graph.merge(relation)
    
    return f"子域 '{filename}' 及其数据和数据值注册完成"

# 示例用法
file = "/root/data/sub_domain_data.xlsx"  # 文件路径
filename = "子域1"  # 文件名作为子域节点名称
@tool
def register_sub_data(file, filename):
    """
    上传子域数据
    """
    result = register_data_from_file(file, filename)
    print(result)
    return result

# 模糊匹配函数
def fuzzy_match(data_element, sub_data, threshold=50):
    """
    计算模糊匹配得分，返回得分最高的匹配项。
    """
    score = fuzz.ratio(data_element.strip(), sub_data.strip())
    return score if score >= threshold else 0

# 获取子域数据
def get_subdomain_data():
    query = "MATCH (s:数据) RETURN s"
    return graph.run(query).data()

# 获取子域数据值
def get_subdomain_values(subdomain_name):
    query = f"MATCH (s:数据 {{value: '{subdomain_name}'}})-[:包含值]->(v) RETURN v"
    return graph.run(query).data()

# 获取共享域数据元
def get_shared_data_elements():
    query = "MATCH (d:DataElement) RETURN d"
    return graph.run(query).data()

# 获取共享域数据元的值域
def get_value_domain(shared_data_element_name):
    query = f"MATCH (d:DataElement {{name: '{shared_data_element_name}'}})-[:HAS_VALUE_DOMAIN]->(v:ValueDomain) RETURN v"
    return graph.run(query).data()

# 获取值域的所有值
def get_values_for_value_domain(value_domain_name):
    query = f"""
    MATCH (vd:ValueDomain)-[:INCLUDE]->(v:Value)
    WHERE vd.name = '{value_domain_name}'
    RETURN v
    """
    values = graph.run(query).data()  # 执行查询并返回数据
    return values


# 第一层映射：查找子域数据与共享域数据元的最佳匹配
def first_level_mapping(subdomain_data_nodes, shared_data_elements):
    for subdomain_data_node in subdomain_data_nodes:
        subdomain_name = subdomain_data_node["s"]["value"]
        best_match_score = 0
        best_shared_data_element = None

        # 遍历共享域数据元，寻找最佳匹配
        for shared_data_element in shared_data_elements:
            shared_name = shared_data_element["d"]["name"]
            score = fuzzy_match(shared_name, subdomain_name)
            if score > best_match_score:
                best_match_score = score
                best_shared_data_element = shared_data_element["d"]

        if best_shared_data_element:
            print(f"子域数据 '{subdomain_name}' 与共享域数据元 '{best_shared_data_element['name']}' 匹配")
            # 调用第二层映射
            handle_second_level_mapping(subdomain_name, best_shared_data_element['name'])
        else:
            print(f"未找到与子域数据 '{subdomain_name}' 相关的共享域数据元")

# 第二层映射：匹配子域数据值与值域值
def handle_second_level_mapping(subdomain_name, shared_data_element_name):
    print(f"正在处理子域数据：{subdomain_name} 与共享域数据元：{shared_data_element_name} 的实例匹配")

    # 获取与共享数据元相关的值域
    value_domains = get_value_domain(shared_data_element_name)
    
    if value_domains:
        print(f"找到值域: {value_domains}")
    else:
        print(f"未找到值域")
        return  # 如果没有找到值域，直接返回

    for value_domain in value_domains:
        value_domain_name = value_domain["v"]["name"]
        print(f"正在处理值域：{value_domain_name}")

        # 获取该值域的所有值
        value_values = get_values_for_value_domain(value_domain_name)

        if not value_values:
            print(f"未找到与值域 '{value_domain_name}' 相关的值")
            continue

        # 获取子域数据的所有值
        subdomain_values_query = f"MATCH (s:数据 {{value: '{subdomain_name}'}})-[:包含值]->(v) RETURN v"
        subdomain_values = graph.run(subdomain_values_query).data()

        if not subdomain_values:
            print(f"未找到与子域数据 '{subdomain_name}' 相关的数据值")
            continue

        for sub_value in subdomain_values:
            subdomain_value = sub_value["v"]["value"]  # 获取子域数据的值

            # 循环匹配子域数据值与值域值
            for value_value in value_values:
                domain_value = value_value["v"].get("name") # 获取值域的值

                # 确保sub_value和domain_value都不为None，并且进行匹配
                if subdomain_value and domain_value and subdomain_value in domain_value:
                    print(f"子域数据值：'{subdomain_value}' 与值域值：'{domain_value}' 匹配")
                    # 在此处建立子域数据值和值域值的映射关系
                    # 可以在此处添加图数据库中的关系创建语句
                else:
                    if subdomain_value is None:
                        print(f"子域数据值为空：'{sub_value}'")
                    if domain_value is None:
                        print(f"值域值为空：'{domain_value}'")

# 创建映射关系（根据需要修改数据库操作）
def create_mapping(subdomain_value, domain_value):
    query = f"CREATE (sub:SubdomainValue {{value: '{subdomain_value}'}})-[:MAPPED_TO]->(val:DomainValue {{value: '{domain_value}'}})"
    graph.run(query)
# 主逻辑：获取所有子域数据和共享域数据元，并进行第一层和第二层映射
    
@tool
def map_and_register_data():
    """
    建立子域到共享域的映射
    """
    subdomain_data_nodes = get_subdomain_data()
    shared_data_elements = get_shared_data_elements()
    first_level_mapping(subdomain_data_nodes, shared_data_elements)

tools = [
    register_sub_data,
    map_and_register_data,
]