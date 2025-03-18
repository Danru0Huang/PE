from langchain.tools import tool
from building_mapping import map_and_register_data
from sub_domain import register_sub_data

@tool
def register_and_map_data_tool():
    '''
    建立映射
    '''
    map_and_register_data()
    return "映射完成"

@tool
def register_sub_data_tool():
    '''
    上传子域数据
    '''
    register_sub_data()
    return "上传子域完成"


# 工具集合
tools = [
    register_and_map_data_tool,
    register_sub_data_tool
]
