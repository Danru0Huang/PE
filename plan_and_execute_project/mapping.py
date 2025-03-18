import pandas as pd
from fuzzywuzzy import fuzz

# 模式级别的匹配函数
def pattern_level_mapping(shared_data_df, sub_data_df, threshold=0.5):
    """
    模式级映射：将子域数据与共享域数据进行名称匹配，计算匹配分数
    """
    mapping_results = []
    for _, sub_data in sub_data_df.iterrows():
        best_match = None
        highest_score = 0
        for _, shared_data in shared_data_df.iterrows():
            # 计算相似度分数
            score = fuzz.ratio(shared_data['DataElementName'].strip(), sub_data['数据'].strip())  # 去除空格
            print(f"Matching {sub_data['数据']} with {shared_data['DataElementName']} - Score: {score}")  # 调试打印
            if score >= threshold and score > highest_score:  # 如果分数超过阈值并且更高
                highest_score = score
                best_match = (shared_data, score)
        
        if best_match:
            shared_data, score = best_match
            # 存储模式匹配的结果，并添加值域信息
            mapping_results.append({
                '子域数据': sub_data['数据'],
                '共享域数据元': shared_data['DataElementName'],
                '共享域数据元ID': shared_data['DataElementID'],
                '值域': shared_data['ValueDomainName'],  # 添加值域信息
                '实例级匹配': None 
            })
        else:
            mapping_results.append({
                '子域数据': sub_data['数据'],
                '共享域数据元': None,
                '共享域数据元ID': None,
                '值域': None,  # 没有匹配时值域为空
                '实例级匹配': None 
            })
    
    return mapping_results

# 实例级别的匹配函数
def instance_level_mapping(shared_data, sub_data):
    """
    对每一行进行实例级的匹配。
    子域数据值与共享域的数据值进行逐个匹配，直到全部匹配成功。
    """
    if pd.notna(sub_data['数据值']) and pd.notna(shared_data['Values']):
        # 预处理共享域和子域数据：去掉空格、分割成单个元素
        shared_value = shared_data['Values'].strip()  # 去除空格
        sub_value = sub_data['数据值'].strip()  # 去除空格

        # 将值分割成单个元素，并去除空格
        shared_value = shared_value.replace('，', ',')  # 替换中文逗号为英文逗号
        sub_value = sub_value.replace('，', ',')  # 替换中文逗号为英文逗号
        
        shared_value_list = [v.strip() for v in shared_value.split(',')]  # 去除空格
        sub_value_list = [v.strip() for v in sub_value.split(',')]  # 去除空格

        matched_values = []  # 存储匹配的值
        unmatched_shared_values = shared_value_list.copy()

        # 对每个子域值进行匹配
        for sub_item in sub_value_list:
            matched = False
            for shared_item in unmatched_shared_values:
                # 打印调试信息
                print(f"Comparing sub_value: {sub_item.strip()} with shared_value: {shared_item.strip()} - Score: {fuzz.ratio(shared_item.strip(), sub_item.strip())}")
                # 使用 fuzz.ratio 计算相似度，确保匹配值相似
                if fuzz.ratio(shared_item.strip(), sub_item.strip()) >= 80:  # 阈值可以调整
                    matched_values.append(f"{sub_item} -> {shared_item}")
                    unmatched_shared_values.remove(shared_item)  
                    matched = True
                    break
            if not matched:
                return None  # 如果某个子域值没有匹配上，则返回 None

        if len(matched_values) == len(sub_value_list):
            # 如果所有子域值都匹配成功
            return matched_values  # 返回每个匹配值的列表
        return None  # 未匹配成功
    elif pd.isna(sub_data['数据值']):
        return ""  # 如果子域数据值为空，返回空字符串
    return None  # 其他情况返回 None


# 读取共享域数据和子域数据
SHARED_DOMAIN_FILE = "/root/data/domain_output.xlsx"  # 共享域数据文件
SUB_DOMAIN_FILE = "/root/data/sub_domain_data.xlsx"  # 子域数据文件

# 假设Excel文件有两个sheet，一个是共享域数据，一个是子域数据
shared_data_df = pd.read_excel(SHARED_DOMAIN_FILE, sheet_name='Sheet1')
sub_data_df = pd.read_excel(SUB_DOMAIN_FILE, sheet_name='Sheet1')

# 获取模式级别映射
pattern_mapping = pattern_level_mapping(shared_data_df, sub_data_df, threshold=0.5)

# 对每一行进行实例级匹配并填充到结果中
# 创建一个新的列表来存储实例匹配的结果，以避免覆盖原始的模式匹配结果
final_mapping_results = []

for result in pattern_mapping:
    sub_data_name = result['子域数据']
    shared_data_name = result['共享域数据元']
    
    # 如果有匹配项，执行实例级匹配
    if shared_data_name:
        sub_data_row = sub_data_df[sub_data_df['数据'] == sub_data_name].iloc[0]
        shared_data_row = shared_data_df[shared_data_df['DataElementName'] == shared_data_name].iloc[0]
        
        instance_result = instance_level_mapping(shared_data_row, sub_data_row)
        
        if instance_result:
            # 将实例匹配结果拆分成单独的行并添加到final_mapping_results
            for matched_value in instance_result:
                final_mapping_results.append({
                    '子域数据': sub_data_name,
                    '共享域数据元': shared_data_name,
                    '共享域数据元ID': shared_data_row['DataElementID'],
                    '值域': shared_data_row['ValueDomainName'],  # 添加值域信息
                    '实例级匹配': matched_value
                })
        else:
            # 如果没有匹配到实例级别的结果，存储空值
            final_mapping_results.append({
                '子域数据': sub_data_name,
                '共享域数据元': shared_data_name,
                '共享域数据元ID': shared_data_row['DataElementID'],
                '值域': shared_data_row['ValueDomainName'],  # 添加值域信息
                '实例级匹配': ""  # 未匹配成功
            })
    else:
        # 如果没有模式匹配到共享域数据元，直接存储空值
        final_mapping_results.append({
            '子域数据': sub_data_name,
            '共享域数据元': None,
            '共享域数据元ID': None,
            '值域': None,  # 如果没有匹配到值域，存储空值
            '实例级匹配': ""
        })

# 将最终结果存储为 DataFrame
output_df = pd.DataFrame(final_mapping_results)

# 存储最终结果到 Excel 文件
output_df.to_excel("/root/data/mapping_results.xlsx", index=False)

print("模式匹配和实例匹配结果已保存到 mapping_results.xlsx")
