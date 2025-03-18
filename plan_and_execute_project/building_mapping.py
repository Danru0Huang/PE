import pandas as pd
from fuzzywuzzy import fuzz
from py2neo import Graph
# 连接到Neo4j数据库
graph = Graph("bolt://121.40.186.242:7687/", auth=("neo4j", "123456"))

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
            create_mapping(subdomain_name, best_shared_data_element['name'])
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
                    create_mapping(subdomain_value, domain_value)
                else:
                    if subdomain_value is None:
                        print(f"子域数据值为空：'{sub_value}'")
                    if domain_value is None:
                        print(f"值域值为空：'{domain_value}'")

# 创建映射关系
def create_mapping(subdomain_name, shared_data_element_name):
    # 创建子域数据与共享数据元之间的映射关系
    query = f"""
    MATCH (sub:Subdomain {{value: '{subdomain_name}'}})
    MATCH (shared:DataElement {{name: '{shared_data_element_name}'}})
    CREATE (sub)-[:MAPPED_TO]->(shared)
    """
    graph.run(query)

# 主逻辑：获取所有子域数据和共享域数据元，并进行第一层和第二层映射
def map_and_register_data():
    subdomain_data_nodes = get_subdomain_data()
    shared_data_elements = get_shared_data_elements()
    first_level_mapping(subdomain_data_nodes, shared_data_elements)

