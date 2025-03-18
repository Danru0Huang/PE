from py2neo import Node, Relationship
import pandas as pd
from py2neo import Graph
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

# 主逻辑：上传子域数据
def register_sub_data():
    result = register_data_from_file(file, filename)
    print(result)
