from langchain.tools import tool
from neo4j import GraphDatabase
import json
from typing import Union

# 配置 Neo4j 驱动
NEO4J_URI = "bolt://localhost:7687/"  
NEO4J_USER = "neo4j" 
NEO4J_PASSWORD = "123456"  

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ID生成器
class IDGenerator:
    @staticmethod
    def generate_id(prefix):
        with driver.session() as session:
            query = f"""
            MATCH (n:{prefix}ID)
            RETURN n.name AS count
            """
            result = session.run(query)
            current_count = result.single()
            if current_count:
                count = int(current_count["count"])
            else:
                session.run(f"CREATE (n:{prefix}ID {{name: 1}})")
                count = 1

            session.run(f"MERGE (n:{prefix}ID) SET n.name = $new_count", new_count=count + 1)
        return f"{prefix}{count:03d}"

# 节点注册函数
def register_entity(label, name):
    """
    通用注册函数，用于创建节点，避免重复注册。
    返回值：是否新注册节点 (True/False)
    """
    with driver.session() as session:
        # 检查是否已经存在
        check_query = f"""
        MATCH (n:{label} {{name: $name}})
        RETURN n
        """
        existing_node = session.run(check_query, name=name).single()
        if existing_node:
            print(f"{label} '{name}' 已存在，跳过注册")
            return False  # 节点已存在，返回 False

        # 如果不存在，则进行注册
        unique_id = IDGenerator.generate_id(label)

        query = f"""
        CREATE (entity:{label} {{id: $id, name: $name}})
        RETURN entity
        """
        try:
            session.run(query, id=unique_id, name=name)
            print(f"注册{label} '{name}' 成功，ID: {unique_id}")
            return True  # 新注册节点，返回 True
        except Exception as e:
            print(f"注册{label} '{name}' 失败: {str(e)}")
            raise

# 创建关系
def create_relationship(from_label, from_name, to_label, to_name, relation):
    """
    创建两个节点之间的关系，避免重复创建。
    """
    with driver.session() as session:
        query = f"""
        MATCH (a:{from_label} {{name: $from_name}})
        MATCH (b:{to_label} {{name: $to_name}})
        MERGE (a)-[r:{relation}]->(b)
        RETURN a, b, r
        """
        try:
            session.run(query, from_name=from_name, to_name=to_name)
            print(f"创建关系: ({from_label}:{from_name}) -[:{relation}]-> ({to_label}:{to_name}) 成功")
        except Exception as e:
            print(f"创建关系失败: {str(e)}")
            raise

# 格式化值和值含义
def parse_values_and_meanings(value_str: str, meaning_str: str) -> tuple[list[list[str]], list[str]]:
    """
    将输入的values和value_meanings按分号分组，每组内的值用逗号分隔
    例如：values="1,A,有效;0,B,无效" → [[1,A,有效], [0,B,无效]]
         meanings="有效;无效" → [有效, 无效]
    """
    # 解析值组：按分号分割组，每组内按逗号分割单个值
    grouped_values = [group.split(',') for group in value_str.split(';')] if value_str else []
    # 解析值含义：按分号分割
    value_meanings = meaning_str.split(';') if meaning_str else []
    return grouped_values, value_meanings


@tool
def register_object_class(object_class: Union[str, dict]) -> str:
    """
    注册对象类，并生成唯一的ID（例如OC001）。
    如果 object_class 是字典类型，提取 'title' 字段作为对象类名。
    """
    if isinstance(object_class, dict):
        # 如果传入的是字典类型，提取 'title' 字段
        object_class = object_class.get("title")
        if not object_class:
            raise ValueError("字典类型的 object_class 缺少 'title' 字段")
    
    # 注册对象类
    is_new = register_entity("ObjectClass", object_class)
    if not is_new:
        print(f"对象类 '{object_class}' 已存在，继续检查是否需要建立关系或执行其他操作")
    
    return f"对象类 '{object_class}' 已注册{'（新建）' if is_new else '（已存在）'}。"

@tool
def register_property(property: Union[str, dict]) -> str:
    """
    注册属性类，并生成唯一的ID（例如PR001）。
    如果 property 是字典类型，提取 'title' 字段作为属性名并立即进行注册。
    """
    # 如果传入的是字典，提取 'title'
    if isinstance(property, dict):
        property = property.get("title")
        if not property:
            raise ValueError("字典类型的 property 缺少 'title' 字段")
        # 直接进行注册
        is_new = register_entity("Property", property)
        return f"属性 '{property}' 已注册{'（新建）' if is_new else '（已存在）'}。"
    
    # 确保 property 是字符串类型
    if not isinstance(property, str):
        raise TypeError("property 必须是字符串类型")
    
    # 注册属性类
    is_new = register_entity("Property", property)
    if not is_new:
        print(f"属性 '{property}' 已存在，跳过节点注册")
    return f"属性 '{property}' 已注册{'（新建）' if is_new else '（已存在）'}。"

@tool
def register_concept_domain(concept_domain: Union[str, dict]) -> str:
    """
    注册概念域类，并生成唯一的ID（例如CD001）。
    如果 concept_domain 是字典类型，提取 'title' 字段作为概念域名。
    """

    # 如果传入的是字典，提取 'title'
    if isinstance(concept_domain, dict):
        concept_domain = concept_domain.get("title")
        if not concept_domain:
            raise ValueError("字典类型的 concept_domain 缺少 'title' 字段")
    
    # 确保 concept_domain 是字符串类型 
    if not isinstance(concept_domain, str):
        raise TypeError("concept_domain 必须是字符串类型")
    
    # 注册概念域类
    is_new = register_entity("ConceptDomain", concept_domain)
    if not is_new:
        print(f"概念域 '{concept_domain}' 已存在，跳过节点注册")
    return f"概念域 '{concept_domain}' 已注册{'（新建）' if is_new else '（已存在）'}。"

@tool
def register_data_element_concept_with_relationships(data_element_concept: Union[str, dict], 
                                                     object_class: Union[str, dict], 
                                                     property: Union[str, dict], 
                                                     concept_domain: Union[str, dict], 
                                                     ) -> str:
    """
    注册数据元概念类，并生成唯一的ID（例如DC001）。
    同时建立相关关系。如果任何参数是字典类型，提取 'title' 字段。
    """
    
    # 如果参数是字典类型，提取 'title'
    if isinstance(data_element_concept, dict):
        data_element_concept = data_element_concept.get("title")
        if not data_element_concept:
            raise ValueError("字典类型的 data_element_concept 缺少 'title' 字段")
    
    if isinstance(object_class, dict):
        object_class = object_class.get("title")
        if not object_class:
            raise ValueError("字典类型的 object_class 缺少 'title' 字段")
    
    if isinstance(property, dict):
        property = property.get("title")
        if not property:
            raise ValueError("字典类型的 property 缺少 'title' 字段")
    
    if isinstance(concept_domain, dict):
        concept_domain = concept_domain.get("title")
        if not concept_domain:
            raise ValueError("字典类型的 concept_domain 缺少 'title' 字段")
    
    # 确保参数都是字符串类型
    if not all(isinstance(arg, str) for arg in [data_element_concept, object_class, property, concept_domain]):
        raise TypeError("所有参数必须是字符串类型")
    
    # 注册数据元概念类
    is_new = register_entity("DataElementConcept", data_element_concept)
    if not is_new:
        print(f"数据元概念 '{data_element_concept}' 已存在，跳过节点注册，继续创建关系")

    # 创建关系
    create_relationship("DataElementConcept", data_element_concept, "ObjectClass", object_class, "HAS_OBJECT_CLASS")
    create_relationship("DataElementConcept", data_element_concept, "Property", property, "HAS_PROPERTY")
    create_relationship("DataElementConcept", data_element_concept, "ConceptDomain", concept_domain, "HAS_CONCEPT_DOMAIN")

    return f"数据元概念 '{data_element_concept}' 已注册{'（新建）' if is_new else '（已存在）'}，关系已建立。"

@tool
def register_value_domain_with_values(value_domain: Union[str, dict], concept_domain: Union[str, dict], value_str: str) -> str:
    """
    注册值域类，并生成唯一的ID（例如VD001）。
    同时注册值节点并建立值与值域的关系。
    
    Args:
        value_domain: 值域名称或包含title的字典
        concept_domain: 概念域名称或包含title的字典
        value_str: 可枚举值的字符串，使用分号分隔不同值组，例如"1,A,有效;0,B,无效"
    
    Returns:
        注册成功的消息，包含值域名称和已注册的值组
    """
    # 格式化值
    grouped_values, _ = parse_values_and_meanings(value_str, "")

    # 处理输入
    if isinstance(value_domain, dict):
        value_domain = value_domain.get("title")
    if isinstance(concept_domain, dict):
        concept_domain = concept_domain.get("title")
    if not value_domain or not concept_domain or not isinstance(grouped_values, list):
        raise ValueError("参数错误：值域和概念域必须是字符串，值组列表必须是数组")

    # 注册值域
    is_new_value_domain = register_entity("ValueDomain", value_domain)
    if is_new_value_domain:
        create_relationship("ValueDomain", value_domain, "ConceptDomain", concept_domain, "BASED_ON")

    # 注册值及其关系
    for group in grouped_values:
        for value in group:  # 每组中的值逐一注册
            is_new_value = register_entity("Value", value)
            if is_new_value:
                create_relationship("ValueDomain", value_domain,"Value",value ,"INCLUDE")
                print(f"成功注册值: {value} 并与值域 {value_domain} 创建了关系")
    
    return f"值域 '{value_domain}' 和值组 {grouped_values} 已注册，并与概念域 '{concept_domain}' 建立关系。"

@tool
def register_value_meanings_with_relationship(concept_domain: Union[str, dict], value_str: str, meaning_str: str) -> str:
    """
    注册值含义，并与值建立对应关系。
    
    Args:
        concept_domain: 概念域名称（字符串）或包含"title"键的字典，用于关联值含义的概念域
        value_str: 可枚举值的字符串，格式为「值组1;值组2」，每组内值用逗号分隔（如 "1,A,有效;0,B,无效"）
        meaning_str: 对应的值含义字符串，格式为「含义1;含义2」，数量需与值组数量一致（如 "有效;无效"）
    
    Returns:
        注册成功的消息，包含值组、值含义与概念域的关系信息
        
    Raises:
        ValueError: 当概念域无效、值组与含义数量不匹配时抛出
    """
    # 格式化值和值含义
    grouped_values, value_meanings = parse_values_and_meanings(value_str, meaning_str)

    # 输入验证
    if isinstance(concept_domain, dict):
        concept_domain = concept_domain.get("title")
    if not concept_domain or not isinstance(grouped_values, list) or not isinstance(value_meanings, list):
        raise ValueError("参数错误：概念域必须是字符串，值组和值含义列表必须是数组")
    if len(grouped_values) != len(value_meanings):
        raise ValueError("值组数量和值含义数量不匹配")

    # 注册值含义
    for group, meaning in zip(grouped_values, value_meanings):
        is_new_meaning = register_entity("ValueMeaning", meaning)
        if is_new_meaning:
            create_relationship("ConceptDomain", concept_domain,"ValueMeaning", meaning, "INCLUDE")
        
        # 建立值和值含义的对应关系
        for value in group:
            create_relationship("Value", value, "ValueMeaning", meaning, "HAS_MEANING")
    
    return f"值组和对应值含义已成功注册，并与概念域 '{concept_domain}' 建立关系。"

@tool
def register_data_element_with_relationships(data_element: Union[str, dict], 
                                             data_element_concept: Union[str, dict], 
                                             value_domain: Union[str, dict], 
                                             ) -> str:
    """
    注册数据元类，并生成唯一的ID（例如DE001）。
    同时建立相关关系。如果参数是字典类型，提取 'title' 字段。
    """

    # 如果参数是字典类型，提取 'title'
    if isinstance(data_element, dict):
        data_element = data_element.get("title")
        if not data_element:
            raise ValueError("字典类型的 data_element 缺少 'title' 字段")
    
    if isinstance(data_element_concept, dict):
        data_element_concept = data_element_concept.get("title")
        if not data_element_concept:
            raise ValueError("字典类型的 data_element_concept 缺少 'title' 字段")
    
    if isinstance(value_domain, dict):
        value_domain = value_domain.get("title")
        if not value_domain:
            raise ValueError("字典类型的 value_domain 缺少 'title' 字段")
    
    # 确保参数都是字符串类型
    if not all(isinstance(arg, str) for arg in [data_element, data_element_concept, value_domain]):
        raise TypeError("所有参数必须是字符串类型")
    
    # 注册数据元类
    is_new = register_entity("DataElement", data_element)
    if not is_new:
        print(f"数据元 '{data_element}' 已存在，跳过节点注册，继续创建关系")

    # 创建关系
    create_relationship("DataElement", data_element, "DataElementConcept", data_element_concept, "BASED_ON")
    create_relationship("DataElement", data_element, "ValueDomain", value_domain, "HAS_VALUE_DOMAIN")

    return f"数据元 '{data_element}' 已注册{'（新建）' if is_new else '（已存在）'}，关系已建立。"

# 工具集合
tools = [
    register_object_class,
    register_property,
    register_concept_domain,
    register_data_element_concept_with_relationships,
    register_value_domain_with_values,
    register_value_meanings_with_relationship,
    register_data_element_with_relationships,
]
