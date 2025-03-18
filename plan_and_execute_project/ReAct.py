# 引入必要的库
from langchain_experimental.plan_and_execute import PlanAndExecute, load_agent_executor, load_chat_planner
# from langchain_community.chat_models import ChatOpenAI 
from langchain_core.prompts import ChatPromptTemplate
from tools import tools
from langchain_openai import ChatOpenAI
import os
import pandas as pd
# 设置 OPENAI_API_KEY 环境变量
os.environ["OPENAI_API_KEY"] = "sk-0DYXSY24ZoiDfSuf6f7667Cf4e534839828d93A448Ba0556"
# 设置 OPENAI_BASE_URL 环境变量
os.environ["OPENAI_BASE_URL"] = "https://xiaoai.plus/v1"
client = ChatOpenAI()

planner_prompt = """
    你是一个计划智能体，你的任务是根据输入的本体类和属性以及值和值含义，按照 MDR 规范的步骤顺序生成六大注册类的计划，并要求为每个属性独立生成注册内容。
    输出内容要求：
   - 仅生成每一步注册操作的任务计划，**不执行任何操作**。
   - 如果属性包含可枚举的值（{values} 不为空），则注册值含义{value_meanings}的注册计划；如果为空，则跳过注册值含义{value_meanings}的相关操作
   - **请确保严格按照提供的输入值 {values} 、值含义{value_meanings}注册，不允许更改、缺少或推测新的值。**
   - 生成每一步计划时，包含工具调用的参数示例。
   - **不要返回执行结果**，只返回计划。
   - **请确保所有注册内容一定全部都是（包括名称和描述）为中文，不允许出现英文。**

    输入的本体类：{ontology_class}
    输入的属性：{attribute}
    输入的值和值含义：{values},{value_meanings}

    请生成如下六类注册信息：
1. 对象类：从输入的类直接生成对象类。
2. 属性：从输入的属性 {attribute} 生成每个的注册内容。
3. 概念域：为属性生成一个对应概念域。
4. 数据元概念：根据对象类和属性组合，去除冗余生成唯一的数据元概念。
5.值域：根据属性所属的值域生成，如申请日期的值域为日期。如果属性包含可枚举的值，请单独分开注册这些值。
6. 值含义：如果存在值，注册相应的值含义。
7.数据元：基于数据元概念，生成数据元，格式为 DE数据元概念。

    **案例1**
    输入的本体类：专利
    输入的属性：法律状态
    输入的值：1，A，有效；0，B，无效
    输入的值含义：有效；无效
    注册对象类：“专利”。
    注册属性：“法律状态”。
    注册概念域：“法律状态域”。
    注册数据元概念:“专利法律状态”
    注册值域：“法律状态域”及其值：“1，A，有效；0，B，无效”
    注册值含义：“有效；无效”
    注册数据元：“DE专利法律状态”
"""

# 使用 ChatOpenAI 模型并生成 Plan
llm = ChatOpenAI(model="gpt-4", temperature=0)
# 使用 load_chat_planner 生成 Planner 实例
planner = load_chat_planner(llm, planner_prompt)

# 设置执行 Agent 的提示模板
executor_prompt = """
你是一个执行智能体，必须严格按照计划智能体的任务执行每一步，不许跳过任何一步。
确保使用工具时参数的个数，尤其是data_element_concept的注册与3个类有关，分别是object_class、property和concept_domain
 `register_value_domain_with_relationship` 工具注册可枚举值{values}时，要分开单独注册每个值，不要注册值组
输出内容要求：
- **按照计划执行每一步**，并返回执行结果。
- 如果执行成功，返回操作成功的消息。
- 如果遇到任何异常或错误，返回详细的错误信息。
当前任务：{step_description}

请执行以下操作：
1. 如果任务是注册object_class，使用 `register_object_class` 工具：
   格式：`register_object_class(object_class="{object_class}")`
2. 如果任务是注册property，使用 `register_property` 工具：
   格式：`register_property(property_name="{property}")`
3. 如果任务是生成concept_domain，使用 `register_concept_domain` 工具：
   格式：`register_concept_domain(concept_domain="{concept_domain}")`
4. 如果任务是生成data_element_concept，与3个类有关，分别object_class、property、concept_domain关联，使用 `register_data_element_concept_with_relationships` 工具：
   格式：`register_data_element_concept_with_relationships(
        data_element_concept="{data_element_concept}", 
        object_class="{object_class}", 
        property="{property}", 
        concept_domain="{concept_domain}")`
5. 如果任务是生成value_domain，与1个类有关，concept_domain关联，使用 `register_value_domain_with_relationship` 工具：
   格式：`register_value_domain_with_relationship(
        value_domain="{value_domain}", 
        concept_domain="{concept_domain}"),
        values="{values}")`
6. 如果任务注册值含义value_meanings，与1个类有关，concept_domain关联，使用 `register_value_meanings_with_relationship` 工具：
   格式：`register_value_meanings_with_relationship( 
        concept_domain="{concept_domain}",
        values="{values}",
        value_meanings="{value_meanings}")`
7. 如果任务是生成data_element，与2个类有关，分别是data_element_concept、value_domain关联，使用 `register_data_element_with_relationships` 工具：
   格式：`register_data_element_with_relationships(
        data_element="{data_element}", 
        data_element_concept="{data_element_concept}", 
        value_domain="{value_domain}")`

    **案例1**
    按照计划进行注册：
    1. 注册object_class：使用register_object_class(object_class="专利")
    输出：对象类 '专利' 已存在，继续检查是否需要建立关系或执行其他操作
    2. 注册property：使用register_property(property_name="法律状态")
    输出：已成功注册属性 '法律状态'，继续按计划进行下一步
    3. 注册concept_domain：使用register_concept_domain(concept_domain="法律状态")
    输出：已成功注册概念域 '法律状态'，继续按计划进行下一步
    4. 注册data_element_concept，与3个类有关，分别是object_class、property、concept_domain关联：
    使用register_data_element_concept_with_relationships(
        data_element_concept="专利法律状态", 
        object_class="专利", 
        property="法律状态",
        concept_domain="法律状态")
    输出：已成功注册数据元概念 '专利法律状态'，并与对象类专利，属性法律状态和概念域法律状态建立关系，继续按计划进行下一步
    5. 注册value_domain，与1个类有关，是concept_domain关联，存在可枚举的值"1，A，有效；0，B，无效"也需要进行注册：
    使用register_value_domain_with_values(
        value_domain="法律状态", 
        concept_domain="法律状态",
        values="1，A，有效；0，B，无效")
    输出：已成功注册值域 '法律状态'，并与概念域建立关系，其中可枚举值单独注册并与值域建立关系，
    注册values: Value - 1 创建关系: (ValueDomain:法律状态) -[:INCLUDE]-> (Value:1)
    注册values: Value - A 创建关系: (ValueDomain:法律状态) -[:INCLUDE]-> (Value:A)
    注册values: Value - 有效 创建关系: (ValueDomain:法律状态) -[:INCLUDE]-> (Value:有效)
    注册values: Value - 0 创建关系: (ValueDomain:法律状态) -[:INCLUDE]-> (Value:0)......继续按计划进行下一步
    6. 注册值含义value_meanings：存在可枚举的值"1，A，有效；0，B，无效"则需要为概念域注册值含义"有效；无效"，
    使用register_value_meanings_with_relationship( 
        concept_domain="法律状态",
        values="1，A，有效；0，B，无效",
        value_meanings="有效；无效")
    输出：已成功注册值含义 '有效；无效'，并与概念域法律状态建立关系，其中值含义单独注册并与可枚举值建立关系，
    注册value_meanings: 有效 创建关系: (values:1) -[:HAS_MEANING]-> (value_meanings: 有效)
                            创建关系: (values:A) -[:HAS_MEANING]-> (value_meanings: 有效)
                            创建关系: (values:有效) -[:HAS_MEANING]-> (value_meanings: 有效)
    注册value_meanings: 无效 创建关系: (values:0) -[:HAS_MEANING]-> (value_meanings: 无效)......继续按计划进行下一步
    7. 注册数据元data_element，与2个类有关，分别是data_element_concept、value_domain关联：
    使用register_data_element_with_relationships(
        data_element="DE专利法律状态", 
        data_element_concept="专利法律状态", 
        value_domain="法律状态")
    输出：已成功注册数据元 'DE专利法律状态'，并与数据元概念专利法律状态和值域法律状态建立关系，完成全部注册计划
"""
      
# 初始化执行器
executor = load_agent_executor(llm, tools,verbose=True)

# 创建并运行 PlanAndExecute Agent
agent = PlanAndExecute(planner=planner, executor=executor, verbose=True)

# 读取Excel文件
df = pd.read_excel("/root/data/ontology_data.xlsx")

def process_data(df):
    """
    将表格中的数据按照本体类、属性、值和值含义分组处理。
    """
    grouped_data = {}
    for _, row in df.iterrows():
        ontology_class = row["本体类"]
        attribute = row["属性"]
        value_str = row.get("值", "")
        meaning_str = row.get("值含义", "")

        if ontology_class not in grouped_data:
            grouped_data[ontology_class] = {}
        
        grouped_data[ontology_class][attribute] = {
            "value_str": value_str,
            "meaning_str": meaning_str
        }
    return grouped_data

data = process_data(df)

# 批处理
def process_in_batches(data, batch_size=3):
    """
    按照每批处理的本体类数量分批执行智能代理任务。
    """
    ontology_classes = list(data.keys())
    for i in range(0, len(ontology_classes), batch_size):
        batch_classes = ontology_classes[i:i + batch_size]
        batch_data = {cls: data[cls] for cls in batch_classes}
        print(f"当前处理的批次：\n{batch_data}\n")
        for ontology_class, attributes in batch_data.items():
            for attribute, details in attributes.items():
                input_description = (
                    f"为本体类 '{ontology_class}' 和属性 '{attribute}' 生成MDR注册计划，"
                    f"包含值字符串 '{details['value_str']}' 和值含义字符串 '{details['meaning_str']}'，"
                )
                try:
                    result = agent.invoke({"input": input_description})
                except Exception as e:
                    print(f"注册失败：{e}\n")
                    continue

process_in_batches(data, batch_size=3)
