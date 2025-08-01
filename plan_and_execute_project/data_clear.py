import xml.etree.ElementTree as ET
import pandas as pd

# 解析 XML 文件
tree = ET.parse('..\data\combined_data_cleaned.xml')
root = tree.getroot()
# 定义命名空间
ns = {'odm': 'http://www.cdisc.org/ns/odm/v1.3'}

# 存储所有的 CodeList
code_lists = {}
for code_list in root.findall('.//odm:CodeList', ns):
    oid = code_list.get('OID')
    values = []
    meanings = []
    for code_list_item in code_list.findall('odm:CodeListItem', ns):
        value = code_list_item.get('CodedValue')
        meaning = code_list_item.find('odm:Decode/odm:TranslatedText', ns).text
        values.append(value)
        meanings.append(meaning)
    code_lists[oid] = {
        'values': ';'.join(values),
        'meanings': ';'.join(meanings)
    }

# 存储转换后的数据
data = []
# 遍历所有 ItemGroupDef 元素
for item_group_def in root.findall('.//odm:ItemGroupDef', ns):
    ontology_class = item_group_def.get('Name')
    # 遍历每个 ItemGroupDef 下的 ItemRef 元素
    for item_ref in item_group_def.findall('odm:ItemRef', ns):
        item_oid = item_ref.get('ItemOID')
        # 根据 ItemOID 查找对应的 ItemDef 元素
        item_def = root.find(f'.//odm:ItemDef[@OID="{item_oid}"]', ns)
        if item_def is not None:
            attribute = item_def.get('Name')
            code_list_ref = item_def.find('odm:CodeListRef', ns)
            if code_list_ref is not None:
                code_list_oid = code_list_ref.get('CodeListOID')
                if code_list_oid in code_lists:
                    value_str = code_lists[code_list_oid]['values']
                    meaning_str = code_lists[code_list_oid]['meanings']
                else:
                    value_str = ''
                    meaning_str = ''
            else:
                value_str = ''
                meaning_str = ''
            # 将数据添加到列表中
            data.append({
                '本体类': ontology_class,
                '属性': attribute,
                '值': value_str,
                '值含义': meaning_str
            })

# 创建 DataFrame
df = pd.DataFrame(data)
# 将 DataFrame 保存为 Excel 文件
df.to_excel('..\data\output.xlsx', index=False)