from langchain_experimental.plan_and_execute import PlanAndExecute, load_agent_executor, load_chat_planner
from langchain_openai import ChatOpenAI
import os
from tools_test import tools

# 设置 OPENAI_API_KEY 环境变量
os.environ["OPENAI_API_KEY"] = "sk-0DYXSY24ZoiDfSuf6f7667Cf4e534839828d93A448Ba0556"
os.environ["OPENAI_BASE_URL"] = "https://xiaoai.plus/v1"

# 初始化模型
client = ChatOpenAI()
# 使用 ChatOpenAI 模型并生成 Plan
llm = ChatOpenAI(model="gpt-4", temperature=0)

# 规划智能体的提示模板
planner_prompt = """
你是一个计划智能体，你的任务是生成建立子域到共享域的映射计划。
输出内容要求：
- 仅生成每一步注册操作的任务计划，**不执行任何操作**。
- **不要返回执行结果**，只返回计划。
- **请确保所有计划都是中文，不要英文**

**案例1**
step1：上传子域数据
step2:建立子域到共享域的映射

"""

# 使用 load_chat_planner 生成 Planner 实例
planner = load_chat_planner(llm, planner_prompt)

# 执行智能体的提示模板
executor_prompt = """

"""

executor = load_agent_executor(llm, tools, verbose=True)

# 创建并运行 PlanAndExecute Agent
agent = PlanAndExecute(planner=planner, executor=executor, verbose=True)  # 将 verbose 传递给 PlanAndExecute

# 运行任务
def run_task():
    """运行 PlanAndExecute 任务"""
    input_description = f"为数据上传和映射创建计划"
    result = agent.invoke({"input": input_description})
    f"Generated result: {result}"

# 执行任务
run_task()
