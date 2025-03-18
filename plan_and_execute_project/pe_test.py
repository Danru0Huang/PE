# 1. 引入必要的库
from langchain_experimental.plan_and_execute import PlanAndExecute, load_agent_executor, load_chat_planner
#from langchain_community.chat_models import ChatOpenAI 
from langchain_core.prompts import ChatPromptTemplate
from tools_test import tools
from langchain_openai import ChatOpenAI
import os
import pandas as pd
# 设置 OPENAI_API_KEY 环境变量
os.environ["OPENAI_API_KEY"] = "sk-0DYXSY24ZoiDfSuf6f7667Cf4e534839828d93A448Ba0556"
# 设置 OPENAI_BASE_URL 环境变量
os.environ["OPENAI_BASE_URL"] = "https://xiaoai.plus/v1"
client = ChatOpenAI()
#completion = client.chat.completions.create(
#    model="gpt-4", 
#    messages=[
#        {"role":"system","content":"You are a helpful assistant."},
#       {"role": "user", "content": "Hello!"}
#   ]
#)
#print(completion)  # 响应
# 初始化 Planner 和 Executor
# 使用提供的详细 planner 提示模板
planner_prompt = """
    你是一个计划智能体，你的任务是根据输入的本体类和属性以及值和值含义，按照 MDR 规范的步骤顺序生成六大注册类的计划，并要求为每个属性独立生成注册内容。
    
    输出内容要求：
   - 仅生成每一步注册操作的任务计划，**不执行任何操作**。
   - 如果属性包含可枚举的值（{values} 不为空），则注册值含义{value_meanings}的注册计划；如果为空，则跳过注册值含义{value_meanings}的相关操作
     请确保严格按照提供的输入值 {values} 、值含义{value_meanings}注册，不允许修改或推测新的值。
   - 生成每一步计划时，包含工具调用的参数示例。
   - **不要返回执行结果**，只返回计划。
   - **请确保所有注册内容一定全部都是（包括名称和描述）为中文，不允许出现英文。**

    输入的本体类：{ontology_class}
    输入的属性：{attribute}
    输入的值和值含义：{values},{value_meanings}

    请生成如下六类注册信息：
    1. **object_class**：从输入的类 {ontology_class} 直接生成对象类{object_class}。
    2. **property**：从输入的属性 {attribute} 生成每个property的注册内容{property}。
    3. **concept_domain**：为property {property}生成一个对应concept_domain{concept_domain}，如申请日期的概念域为日期。
    4. **data_element_concept**：根据{object_class}和{property}组合，去除冗余生成唯一的data_element_concept{data_element_concept}。
    5. **value_domain**：根据property{property}所属的value_domain{value_domain}生成，如申请日期的值域为日期。如果property包含可枚举的值 {values}，请注册这些值。
    6. **value_meanings**：如果存在{values}，注册相应的{value_meanings}。
    7. **data_element**：基于data_element_concept{data_element_concept}，生成data_element{data_element}，格式为 DE{data_element_concept}。

    输出的计划一定包含工具调用的参数示例：
    生成的任务计划需包括以下步骤：
    1. 注册object_class：使用 register_object_class 工具，格式为：
    register_object_class(object_class="{object_class}")
    2. 注册property：使用 register_property 工具，格式为：
    register_property(property_name="{property}")
    3. 注册concept_domain：使用 register_concept_domain 工具，格式为：
    register_concept_domain(concept_domain="{concept_domain}")
    4. 注册data_element_concept，与3个类有关，分别是object_class{object_class}、property{property}、concept_domain{concept_domain}关联：使用 register_data_element_concept_with_relationships 工具，格式为：
    register_data_element_concept_with_relationships(
        data_element_concept="{data_element_concept}", 
        object_class="{object_class}", 
        property="{property}",
        concept_domain="{concept_domain}")
    5. 注册value_domain，与1个类有关，是concept_domain{concept_domain}关联，如果存在可枚举的值{values}也需要进行注册：使用 register_value_domain_with_relationship 工具，格式为：
    register_value_domain_with_values(
        value_domain="{value_domain}", 
        concept_domain="{concept_domain}",
        values="{values}")
    6. 如果值和值含义为空，跳过相关步骤并说明。注册值含义value_meanings：如果存在可枚举的值{values}则需要为概念域注册值含义{value_meanings}，使用 `register_value_meanings_with_relationship` 工具，格式为：
    register_value_meanings_with_relationship( 
        concept_domain="{concept_domain}",
        values="{values}",
        value_meanings="{value_meanings}")
    7. 注册数据元data_element，与2个类有关，分别是data_element_concept{data_element_concept}、value_domain{value_domain}关联：使用 register_data_element_with_relationships 工具，格式为：
    register_data_element_with_relationships(
        data_element="{data_element}", 
        data_element_concept="{data_element_concept}", 
        value_domain="{value_domain}")
"""

# 使用 ChatOpenAI 模型并生成 Plan
llm = ChatOpenAI(model="gpt-4", temperature=0)
# 使用 load_chat_planner 生成 Planner 实例
planner = load_chat_planner(llm, planner_prompt)

# 设置执行 Agent 的提示模板
executor_prompt = """
你是一个执行智能体，必须严格按照计划智能体的任务执行每一步，不许跳过任何一步。
确保使用工具时参数的个数，尤其是data_element_concept的注册与3个类有关，分别是object_class、property和concept_domain
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
"""
      
# 初始化执行器
executor = load_agent_executor(llm, tools, verbose=True)

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
                    f"并自动执行注册操作。"
                )
                try:
                    result = agent.invoke({"input": input_description})
                    print(f"注册结果：\n{result}\n")
                except Exception as e:
                    print(f"注册失败：{e}\n")
                    continue

process_in_batches(data, batch_size=3)
