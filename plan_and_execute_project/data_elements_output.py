from py2neo import Graph
import pandas as pd

# 连接到Neo4j数据库
neo4j_url = "bolt://121.40.186.242:7687/" 
username = "neo4j" 
password = "123456"  

graph = Graph(neo4j_url, auth=(username, password))

def flatten_values(values):
    """
    展平嵌套的值列表，并转换为字符串。
    :param values: 列表，可能包含嵌套列表
    :return: 展平后的字符串，值用逗号分隔
    """
    flattened = []
    for value in values:
        if isinstance(value, list):
            flattened.extend(value)  # 展平嵌套列表
        else:
            flattened.append(value)  # 添加非列表值
    return ",".join(map(str, flattened))  # 转为字符串并用逗号分隔

def export_all_data_elements_to_excel(output_file):
    """
    从数据库中查询所有数据元及其ID和值域信息，并导出为Excel文件。
    
    :param output_file: 输出的Excel文件路径
    """
    # 查询数据元、ID、值域名称和值域中的值
    query = """
    MATCH (de:DataElement)-[:BASED_ON]->(dec:DataElementConcept)
    OPTIONAL MATCH (de)-[:HAS_VALUE_DOMAIN]->(vd:ValueDomain)
    OPTIONAL MATCH (v:Value)-[:BELONGS_TO]->(vd)
    RETURN de.name AS DataElementName,
           de.id AS DataElementID,
           vd.name AS ValueDomainName,
           collect(v.name) AS Values
    ORDER BY de.name
    """
    # 执行查询
    result = graph.run(query).data()

    # 处理查询结果
    processed_result = []
    for row in result:
        processed_result.append({
            "DataElementName": row["DataElementName"],
            "DataElementID": row["DataElementID"],
            "ValueDomainName": row["ValueDomainName"] if row["ValueDomainName"] else "",  # 如果无值域则为空
            "Values": flatten_values(row["Values"]) if row["Values"] else ""  # 展平值列表并转为字符串
        })

    # 将结果转换为 DataFrame
    df = pd.DataFrame(processed_result)

    # 导出为 Excel 文件
    df.to_excel(output_file, index=False)
    print(f"所有数据元及其ID和值域信息已导出至: {output_file}")

# 示例用法
output_file = "/root/data/domain_output.xlsx"  # 输出的Excel文件路径
export_all_data_elements_to_excel(output_file)
