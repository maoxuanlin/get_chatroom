"""
天山四类工单同步与删除系统

功能说明：
1. 数据同步：将天山工单系统的5类工单数据同步到ClickHouse数据库
   - 申请报结：工单处理完成后申请结案
   - 申请延期：工单无法按期完成，申请延期
   - 转派：工单需要转给其他部门处理
   - 退单：工单被退回，需要重新处理
   - 延期报结：延期后再次申请结案

2. 记录删除：将天山系统中已提交的工单记录删除
   - 从ClickHouse查询"是否提交=是"的记录
   - 遍历天山5个工作表，定位并删除对应记录

数据流向：
天山工单系统(天山API) --同步--> ClickHouse数据库(数据仓库)
天山工单系统(天山API) <--删除-- ClickHouse数据库(查询已提交)

作者：毛宣霖
版本：v4
"""

import time
import requests
import json
import datetime
import logging
import pandahouse as ph

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================
# 配置区域
# ============================================================

# # 配置常量（不变）
# CK_DB_CONFIG = {
#     "host": "http://188.105.2.140:20107",
#     "user": "root",
#     "password": "Richr00t$",
#     "database": "wangyou"
# }

# # API固定参数（仅保留查询和新增API，移除更新API）
# TIANSHAN_GET_URL = "http://188.105.2.140:20098/api/v2/open/worksheet/getFilterRows"
# TIANSHAN_ADD_URL = "http://188.105.2.140:20098/api/v2/open/worksheet/addRows"
# OWNER_VALUE = "ef65e28c-6164-42bb-8814-3fcd08b0966b"

# 天山工单系统API配置
TIANSHAN_CONFIG = {
    "appKey": "a71fff57753023e7",  # 天山系统应用密钥
    "sign": "YjNkZDdiYzU3ODExMWFhNmI4ZjEyOGJmOTc3YmI5MTg4ODYwZDI2NDZmZjk1MmJkNDI1YjQ3NmIxZDI3ZmYwZA==",  # 签名密钥
    "get_rows_url": "http://10.76.134.138:30023/api/v2/open/worksheet/getFilterRows",  # 获取工作表数据接口
    "delete_row_url": "http://10.76.134.138:30023/api/v2/open/worksheet/deleteRow"  # 删除工作表记录接口
}

# ClickHouse数据库配置（数据仓库）
CK_DB_CONFIG = {
    "host": "http://10.212.190.67:8123",  # 数据库地址
    "user": "root",  # 用户名
    "password": "Richr00t$",  # 密码
    "database": "wangyou"  # 数据库名
}

# 工作表配置 - 定义需要同步的5类工单
# 配置内容：工作表ID、对应数据库表名、EOMS单号字段ID、提交字段ID、同步字段映射
WORKSHEET_CONFIG = {
    # 申请报结 - 工单处理完成申请结案
    "申请报结": {
        "worksheetId": "69892ec28f637866abdd92dd",  # 天山工作表ID
        "db_table": "wangyou.aran_tianshan_close",  # ClickHouse表名
        "eoms_field": "69893e8d181b6e7ca043f715",  # EOMS单号字段ID
        "submit_field": "69898a5b8f637866abdd9c3b",  # 是否提交字段ID
        "fields": {
            # 天山字段ID: 数据库字段名
            "69893e8d181b6e7ca043f71c": "工单来源",
            "69893e8d181b6e7ca043f715": "EOMS单号",
            "69893e8d181b6e7ca043f716": "受理号码",
            "69893e8d181b6e7ca043f718": "投诉标准地址",
            "69b37b4d2a3d5e4cc823dcac": "工单归属地市",
            "69893e8d181b6e7ca043f717": "工单归属区县",
            "69b278418f637866abdfe4a7": "主控小区ID",
            "69b374e7b873cda2bddc493d": "主控小区名称",
            "69b374e7b873cda2bddc493e": "网络制式",
            "69b377d27f2ceaee383a43f5": "投诉业务",
            "69b377d27f2ceaee383a43f6": "覆盖类型",
            "69b377d27f2ceaee383a43f7": "投诉点场景",
            "69b377d27f2ceaee383a43f8": "特别诉求",
            "69b377d27f2ceaee383a43f9": "客服落单质量",
            "69893e8d181b6e7ca043f71e": "问题原因分类",
            "69898a5b8f637866abdd9c3a": "补充说明",
            "698c98ea181b6e7ca062b8d9": "解决分类措施",
            "69893e8d181b6e7ca043f719": "解决措施附加说明",
            "69b27a017f2ceaee3836ee58": "处理结果",
            "69898a5b8f637866abdd9c37": "已答复客户",
            "69898a5b8f637866abdd9c3d": "报结意见",
            "69b27fb5c7388fa499046e74": "方案实施情况（附加报结信息）",
            "69b373f0b873cda2bddc25ab": "处理过程",
            "69c9cc187f2ceaee3881cdc4": "无法解决原因",
            "69898a5b8f637866abdd9c38": "是否派测试单",
            "69898a5b8f637866abdd9c3b": "是否提交",
            "69b278418f637866abdfe4a6": "投诉主因"
        }
    },
    # 申请延期 - 工单无法按期完成申请延期
    "申请延期": {
        "worksheetId": "69b37fa1c90cd70af32d79f9",
        "db_table": "wangyou.aran_tianshan_delay",
        "eoms_field": "69893e8d181b6e7ca043f715",
        "submit_field": "69898a5b8f637866abdd9c3b",
        "fields": {
            "69893e8d181b6e7ca043f71c": "工单来源",
            "69893e8d181b6e7ca043f715": "EOMS单号",
            "69893e8d181b6e7ca043f716": "受理号码",
            "69893e8d181b6e7ca043f718": "投诉标准地址",
            "69b37b4d2a3d5e4cc823dcac": "工单归属地市",
            "69893e8d181b6e7ca043f717": "工单归属区县",
            "69b278418f637866abdfe4a7": "主控小区ID",
            "69b374e7b873cda2bddc493d": "主控小区名称",
            "69b374e7b873cda2bddc493e": "网络制式",
            "69b377d27f2ceaee383a43f5": "投诉业务",
            "69b377d27f2ceaee383a43f6": "覆盖类型",
            "69b377d27f2ceaee383a43f7": "投诉点场景",
            "69b377d27f2ceaee383a43f8": "特别诉求",
            "69b377d27f2ceaee383a43f9": "客服落单质量",
            "69893e8d181b6e7ca043f71e": "问题原因分类",
            "69898a5b8f637866abdd9c3a": "补充说明",
            "698c98ea181b6e7ca062b8d9": "解决分类措施",
            "69893e8d181b6e7ca043f719": "解决措施附加说明",
            "69b27a017f2ceaee3836ee58": "处理结果",
            "69b38082f651e846c68b2694": "延期原因",
            "69b38082f651e846c68b2695": "预计解决时间",
            "69898a5b8f637866abdd9c37": "已答复客户",
            "69898a5b8f637866abdd9c3d": "报结意见",
            "69b27fb5c7388fa499046e74": "方案实施情况（当前处理进展）",
            "69898a5b8f637866abdd9c38": "是否派测试单",
            "69898a5b8f637866abdd9c3b": "是否提交",
            "69b278418f637866abdfe4a6": "投诉主因"
        }
    },
    # 转派 - 工单需要转给其他部门处理
    "转派": {
        "worksheetId": "69b380dbc90cd70af32d7d2f",
        "db_table": "wangyou.aran_tianshan_forward",
        "eoms_field": "69893e8d181b6e7ca043f715",
        "submit_field": "69898a5b8f637866abdd9c3b",
        "fields": {
            "69893e8d181b6e7ca043f71c": "工单来源",
            "69893e8d181b6e7ca043f715": "EOMS单号",
            "69893e8d181b6e7ca043f716": "受理号码",
            "69b38160181b6e7ca07bd222": "附加说明",
            "69b38160181b6e7ca07bd223": "请选择转派对象",
            "69898a5b8f637866abdd9c3b": "是否提交"
        }
    },
    # 退单 - 工单被退回需要重新处理
    "退单": {
        "worksheetId": "69b380e06c0b5347c05fb6f8",
        "db_table": "wangyou.aran_tianshan_return",
        "eoms_field": "69893e8d181b6e7ca043f715",
        "submit_field": "69898a5b8f637866abdd9c3b",
        "fields": {
            "69893e8d181b6e7ca043f71c": "工单来源",
            "69893e8d181b6e7ca043f715": "EOMS单号",
            "69893e8d181b6e7ca043f716": "受理号码",
            "69b3820e2a3d5e4cc823de06": "退单原因分类",
            "69b3820e2a3d5e4cc823de07": "退单附加说明",
            "69b382472a3d5e4cc823de35": "补充说明",
            "69898a5b8f637866abdd9c3b": "是否提交"
        }
    },
    # 延期报结 - 延期后再次申请结案
    "延期报结": {
        "worksheetId": "69b271e1c90cd70af32a3576",
        "db_table": "wangyou.aran_tianshan_delay_close",
        "eoms_field": "69b2659b9b75c1ea1a84916b",
        "submit_field": "69bba3c87f2ceaee38539490",
        "fields": {
            "69b266297f2ceaee38369d59": "工单来源",
            "69b2659b9b75c1ea1a84916b": "EOMS单号",
            "69b2659b9b75c1ea1a849188": "工单归属区县",
            "69b26dc6181b6e7ca075bb42": "受理号码",
            "69bba0ee7f2ceaee38537cdf": "问题原因分类",
            "69bba0c2176302b3fce97c48": "补充说明",
            "69bba238f651e846c6d24f7d": "解决措施分类",
            "69bba238f651e846c6d24f7f": "解决措施附加说明",
            "69bba238f651e846c6d24f80": "处理结果",
            "69c9cc71b873cda2bd22189a": "无法解决原因",
            "69bba238f651e846c6d24f81": "已答复客户",
            "69bba238f651e846c6d24f82": "报结意见",
            "69bba238f651e846c6d24f84": "方案实施情况（附加报结信息）",
            "69bba238f651e846c6d24f85": "处理过程",
            "69bba3c87f2ceaee38539490": "是否提交"
        }
    }
}

# 参考数据配置 - 用于解析天山系统的关联选择字段
# 天山系统中问题原因分类、报结意见等字段存储的是rowId，需要转换为实际显示文本
# 【新增】层级表配置：
#   - hierarchy_field_id: 层级关联字段ID，包含父级信息
#   - 层级表需要通过此字段获取完整路径（如 "一级 ->二级 ->三级"）
REFERENCE_WORKSHEETS = {
    "问题原因分类": {
        "worksheet_id": "69893e4f176302b3fc85bbc7",  # 工作表ID
        "display_field_id": "69893e4f176302b3fc85bbc8",  # 显示字段ID（最后一级名称）
        "hierarchy_field_id": "6989fc43f651e846c6c27a96"  # 【新增】层级关联字段ID（包含父级信息）
    },
    "报结意见": {
        "worksheet_id": "69898a40176302b3fc8852c0",
        "display_field_id": "69898a40176302b3fc8852c1",
        "hierarchy_field_id": "69898a40176302b3fc8852c2"  # 层级关联字段ID
    },
    "解决分类措施": {
        "worksheet_id": "698c9829e64d49268d595b61",
        "display_field_id": "69b76974176302b3fcd0905a"
    }
}


# ============================================================
# 函数定义
# ============================================================

def get_filter_rows(worksheet_id, page_index=1, page_size=1000, list_type=0):
    """
    从天山系统获取工作表的分页数据

    参数说明：
        worksheet_id: 工作表ID，天山系统中每个工作表都有唯一ID
        page_index: 页码，从1开始，用于分页获取数据
        page_size: 每页记录数，默认1000条
        list_type: 列表类型，0表示普通列表

    返回值：
        成功返回包含rows和total的字典
        失败返回None

    API调用：
        使用POST请求调用天山API的getFilterRows接口
        超时时间30秒，避免长时间等待
    """
    try:
        params = {
            "appKey": TIANSHAN_CONFIG["appKey"],
            "sign": TIANSHAN_CONFIG["sign"],
            "worksheetId": worksheet_id,
            "viewId": "",
            "pageSize": page_size,
            "pageIndex": page_index,
            "keyWords": "",
            "listType": list_type,
            "controls": [],
            "filters": [],
            "sortId": "",
            "isAsc": "",
            "notGetTotal": "",
            "useControlId": "",
            "getSystemControl": ""
        }

        response = requests.post(
            TIANSHAN_CONFIG["get_rows_url"],
            json=params,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            if result.get("success"):
                return result.get("data", {})
            else:
                logger.error(f"获取数据失败: {result.get('error_msg', '未知错误')}")
                return None
        else:
            logger.error(f"请求失败，状态码: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"获取数据异常: {e}")
        return None


def delete_row(worksheet_id, row_id, trigger_workflow=True, thorough_delete=False):
    """
    从天山系统删除一条记录

    参数说明：
        worksheet_id: 工作表ID
        row_id: 要删除的记录ID（行ID）
        trigger_workflow: 是否触发工作流，True表示删除后触发后续业务处理
        thorough_delete: 是否彻底删除，True表示物理删除，False表示逻辑删除

    返回值：
        成功返回True
        失败返回False

    用途：
        当工单标记为"已提交"时，调用此函数从天山系统中删除该记录
    """
    try:
        params = {
            "appKey": TIANSHAN_CONFIG["appKey"],
            "sign": TIANSHAN_CONFIG["sign"],
            "worksheetId": worksheet_id,
            "rowId": row_id,
            "triggerWorkflow": trigger_workflow,
            "thoroughDelete": thorough_delete
        }

        response = requests.post(
            TIANSHAN_CONFIG["delete_row_url"],
            json=params,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            if result.get("success"):
                logger.info(f"成功删除行记录: {row_id}")
                return True
            else:
                logger.error(f"删除失败: {result.get('error_msg', '未知错误')}")
                return False
        else:
            logger.error(f"删除请求失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"删除异常: {e}")
        return False


def get_reference_data(ref_name, ref_config):
    """
    获取天山系统的参考数据（字典表）- 支持层级表获取完整路径

    参数说明：
        ref_name: 参考数据名称（如"问题原因分类"、"报结意见"等）
        ref_config: 参考数据配置，包含worksheet_id、display_field_id和hierarchy_field_id

    返回值：
        返回 {rowId: 显示文本} 的字典
        层级表返回完整路径，如 "一级分类 -> 二级分类 -> 三级分类"

    用途：
        天山系统的某些字段（如问题原因分类）存储的是关联记录的rowId
        需要通过此函数获取的映射表，将rowId转换为实际分类名称

    层级表处理逻辑（完整追溯）：
        1. 先获取所有数据，建立 {rowid: row_data} 映射
        2. 对每个节点，递归向上追溯父级
        3. 构建从根节点到当前节点的完整路径
    """
    logger.info(f"正在获取 {ref_name} 参考数据...")
    worksheet_id = ref_config["worksheet_id"]
    display_field_id = ref_config["display_field_id"]
    hierarchy_field_id = ref_config.get("hierarchy_field_id")  # 层级字段ID

    # 【新增】第一步：获取所有数据，建立映射表
    all_rows = {}  # {rowid: row_data}
    page_index = 1
    page_size = 1000

    while True:
        result = get_filter_rows(worksheet_id, page_index, page_size)
        if not result:
            break
        rows = result.get("rows", [])
        if not rows:
            break
        for row in rows:
            row_id = row.get("rowid", "")
            if row_id:
                all_rows[row_id] = row
        if len(rows) < page_size:
            break
        page_index += 1
        time.sleep(0.2)

    logger.info(f"获取到 {len(all_rows)} 条记录")

    # 【新增】第二步：递归构建完整层级路径
    def build_full_path(row_id, visited=None):
        """
        递归构建完整的层级路径，追溯到根节点

        参数：
            row_id: 当前节点的row_id
            visited: 已访问的节点列表，防止循环引用

        返回：
            完整路径，如 "一级 ->二级 ->三级"
        """
        if visited is None:
            visited = []

        # 防止循环引用
        if row_id in visited:
            return ""
        visited = visited + [row_id]

        # 获取当前节点的数据
        current_row = all_rows.get(row_id)
        if not current_row:
            return ""

        # 获取当前节点的名称
        current_name = current_row.get(display_field_id, "")

        # 如果没有层级字段，返回当前名称（根节点）
        if not hierarchy_field_id:
            return current_name

        # 获取层级字段（包含父级信息）
        hierarchy_value = current_row.get(hierarchy_field_id, "")

        # 如果层级字段为空或者是空数组，说明是根节点
        if not hierarchy_value or hierarchy_value == "[]":
            return current_name

        # 解析层级字段
        try:
            if isinstance(hierarchy_value, str):
                hierarchy_data = json.loads(hierarchy_value)
            else:
                hierarchy_data = hierarchy_value

            # 如果是数组且有元素，获取父级信息
            if isinstance(hierarchy_data, list) and len(hierarchy_data) > 0:
                parent_item = hierarchy_data[0]
                parent_sid = parent_item.get("sid", "")  # 父级的row_id
                parent_name = parent_item.get("name", "")  # 【新增】父级的名称

                if parent_sid and parent_name:
                    # 【修改】先尝试递归获取父级的完整路径
                    parent_path = build_full_path(parent_sid, visited)

                    if parent_path and parent_path != parent_name:
                        # 父级有更完整的路径（包含更上级）
                        return f"{parent_path} -> {current_name}"
                    else:
                        # 【新增】父级行不在 all_rows 中，尝试从 sourcevalue 解析嵌套层级
                        sourcevalue = parent_item.get("sourcevalue", "")
                        if sourcevalue:
                            try:
                                sv_data = json.loads(sourcevalue)
                                # 从 sourcevalue 中获取父级的层级字段
                                parent_hierarchy = sv_data.get(hierarchy_field_id)
                                if parent_hierarchy:
                                    parent_h_data = json.loads(parent_hierarchy)
                                    if isinstance(parent_h_data, list) and len(parent_h_data) > 0:
                                        grandparent = parent_h_data[0]
                                        grandparent_name = grandparent.get("name", "")
                                        grandparent_sid = grandparent.get("sid", "")
                                        if grandparent_name and grandparent_sid:
                                            # 有祖父级，继续递归查找祖父级的完整路径
                                            grandparent_path = build_full_path(grandparent_sid, visited)
                                            if grandparent_path and grandparent_path != grandparent_name:
                                                # 祖父级有更上级
                                                return f"{grandparent_path} ->{parent_name} -> {current_name}"
                                            else:
                                                # 祖父级是根节点或没有更上级
                                                return f"{grandparent_name} ->{parent_name} -> {current_name}"
                                        elif grandparent_name:
                                            # 有祖父级名称但没有 sid
                                            return f"{grandparent_name} ->{parent_name} -> {current_name}"
                            except (json.JSONDecodeError, TypeError):
                                pass
                        # 没有更上级，返回 父级 ->当前
                        return f"{parent_name} -> {current_name}"
                elif parent_name:
                    # 有父级名称但没有 sid，直接拼接
                    return f"{parent_name} -> {current_name}"
                else:
                    # 没有父级信息，返回当前名称
                    return current_name
            else:
                # 层级字段格式异常，返回当前名称
                return current_name

        except (json.JSONDecodeError, TypeError):
            # JSON解析失败，返回当前名称
            return current_name

    # 【新增】第三步：为每个节点构建完整路径
    ref_map = {}

    for row_id, row in all_rows.items():
        display_value = row.get(display_field_id, "")

        if row_id and display_value:
            if hierarchy_field_id:
                # 层级表：递归构建完整路径
                full_path = build_full_path(row_id)
                ref_map[row_id] = full_path if full_path else str(display_value)
            else:
                # 非层级表：直接使用显示值
                if isinstance(display_value, list):
                    path_parts = [str(v) for v in display_value if v]
                    ref_map[row_id] = " -> ".join(path_parts) if path_parts else display_value
                else:
                    ref_map[row_id] = str(display_value)

    logger.info(f"{ref_name} 参考数据获取完成，共 {len(ref_map)} 条记录")
    return ref_map


def decode_field_value(value, ref_maps):
    """
    解析天山系统的字段值，将关联字段转换为实际显示文本

    参数说明：
        value: 字段原始值，可能是多种格式：
               - 列表格式: [{"rowId": "xxx"}, ...]
               - 对象格式: {"rowId": "xxx"}
               - JSON字符串: '[{"rowId": "xxx"}]'
               - 普通文本: "普通文本"
        ref_maps: 参考数据映射表，用于将rowId转换为显示文本

    返回值：
        返回字段的实际显示文本
        如果是多个值，用逗号连接

    处理逻辑：
        1. 如果是列表，遍历每个元素查找rowId
        2. 如果是对象，直接查找rowId
        3. 如果是JSON字符串，先解析再处理
        4. 如果都不匹配，直接返回原值

    用途：
        天山系统中关联字段（如问题原因分类）存储的是rowId
        需要通过此函数将rowId转换为实际的分类名称显示
    """
    if not value:
        return ""

    if isinstance(value, list):
        decoded_values = []
        for item in value:
            if isinstance(item, dict) and "rowId" in item:
                row_id = item["rowId"]
                for ref_map in ref_maps.values():
                    if row_id in ref_map:
                        ref_value = ref_map[row_id]
                        if isinstance(ref_value, list):
                            decoded_values.extend(ref_value)
                        else:
                            decoded_values.append(str(ref_value))
                        break
                else:
                    decoded_values.append(row_id)
            else:
                decoded_values.append(str(item))
        return ",".join(decoded_values)
    elif isinstance(value, dict) and "rowId" in value:
        row_id = value["rowId"]
        for ref_map in ref_maps.values():
            if row_id in ref_map:
                ref_value = ref_map[row_id]
                if isinstance(ref_value, list):
                    return ",".join([str(v) for v in ref_value])
                else:
                    return str(ref_value)
        return row_id
    elif isinstance(value, str):
        try:
            data = json.loads(value)
            if isinstance(data, list):
                decoded_values = []
                for item in data:
                    if isinstance(item, dict) and "sid" in item:
                        sid = item["sid"]
                        for ref_map in ref_maps.values():
                            if sid in ref_map:
                                ref_value = ref_map[sid]
                                if isinstance(ref_value, list):
                                    decoded_values.extend(ref_value)
                                else:
                                    decoded_values.append(str(ref_value))
                                break
                        else:
                            decoded_values.append(sid)
                    else:
                        decoded_values.append(str(item))
                return ",".join(decoded_values)
            elif isinstance(data, dict) and "sid" in data:
                sid = data["sid"]
                for ref_map in ref_maps.values():
                    if sid in ref_map:
                        ref_value = ref_map[sid]
                        if isinstance(ref_value, list):
                            return ",".join([str(v) for v in ref_value])
                        else:
                            return str(ref_value)
                    break
                return sid
            else:
                return str(value)
        except json.JSONDecodeError:
            return str(value)
    else:
        return str(value)


def get_table_fields(db_table):
    """
    获取ClickHouse数据库表的字段列表

    参数说明：
        db_table: 数据库表名，格式为 database.table_name

    返回值：
        返回该表所有字段名的集合（set）
        如果查询失败返回空集合

    用途：
        - 验证配置中的字段是否存在于数据库
        - 避免同步不存在的字段导致SQL错误
        - 用于过滤有效的同步字段
    """
    try:
        sql = f"DESCRIBE TABLE {db_table}"
        df = ph.read_clickhouse(sql, connection=CK_DB_CONFIG)
        return set(df['name'].tolist())
    except Exception as e:
        logger.error(f"获取表字段失败 {db_table}: {e}")
        return set()


def sync_worksheet_data(worksheet_name, ref_maps):
    """
    同步单个工作表的数据到ClickHouse数据库

    参数说明：
        worksheet_name: 工作表名称（如"申请报结"、"申请延期"等）
        ref_maps: 参考数据映射表，用于解析关联字段

    返回值：
        返回 (新增记录数, 更新记录数) 的元组

    同步流程：
        1. 分页从天山系统读取数据（每页100条）
        2. 对每条记录：
           - 跳过EOMS单号为空的记录
           - 解析关联字段（将rowId转为显示文本）
           - 根据EOMS单号判断记录状态
        3. 不存在则INSERT新增
        4. 已存在则UPDATE更新
        5. 页间延时0.5秒，避免请求过快

    增量同步机制：
        - 先查询数据库中是否存在该EOMS单号
        - 不存在 → INSERT新增
        - 已存在 → ALTER TABLE UPDATE更新
        - EOMS单号为主键，确保数据唯一性
    """
    config = WORKSHEET_CONFIG[worksheet_name]
    worksheet_id = config["worksheetId"]
    db_table = config["db_table"]
    eoms_field = config["eoms_field"]
    submit_field = config["submit_field"]
    fields = config["fields"]

    logger.info(f"\n{'=' * 60}")
    logger.info(f"开始同步工作表: {worksheet_name}")
    logger.info(f"{'=' * 60}")

    db_fields = get_table_fields(db_table)
    valid_fields = {k: v for k, v in fields.items() if v in db_fields}

    logger.info(f"配置字段数: {len(fields)}, 数据库有效字段数: {len(valid_fields)}")

    if not valid_fields:
        logger.error("错误: 没有有效的字段可以同步！")
        return 0, 0

    page_index = 1
    page_size = 100
    total_synced = 0
    total_updated = 0

    while True:
        result = get_filter_rows(worksheet_id, page_index, page_size)

        if not result:
            logger.warning("获取数据失败或无数据")
            break

        rows = result.get("rows", [])
        if not rows:
            break

        total_rows = result.get("total", 0)
        logger.info(f"第 {page_index} 页，获取到 {len(rows)} 条记录，总计 {total_rows} 条")

        for row in rows:
            row_id = row.get("rowid", "")
            eoms_no = row.get(eoms_field, "")

            if not eoms_no:
                logger.warning(f"跳过无EOMS单号的记录: {row_id}")
                continue

            record = {}
            for control_id, field_name in valid_fields.items():
                raw_value = row.get(control_id, "")
                decoded_value = decode_field_value(raw_value, ref_maps)
                record[field_name] = decoded_value

            try:
                sql_check = f"""
                SELECT COUNT(*) as cnt FROM {db_table} WHERE `EOMS单号` = '{eoms_no}'
                """
                existing = ph.read_clickhouse(sql_check, connection=CK_DB_CONFIG)

                if existing.iloc[0]["cnt"] == 0:
                    columns = ", ".join([f"`{k}`" for k in record.keys()])
                    placeholders = ", ".join([f"'{v}'" for v in record.values()])
                    insert_sql = f"""
                    INSERT INTO {db_table} ({columns}) VALUES ({placeholders})
                    """
                    ph.execute(insert_sql, connection=CK_DB_CONFIG)
                    total_synced += 1
                    logger.info(f"新增记录: {eoms_no}")
                else:
                    update_parts = []
                    for k, v in record.items():
                        if k != "EOMS单号":
                            update_parts.append(f"`{k}` = '{v}'")

                    if update_parts:
                        update_sql = f"""
                        ALTER TABLE {db_table}
                        UPDATE {", ".join(update_parts)}
                        WHERE `EOMS单号` = '{eoms_no}'
                        """
                        ph.execute(update_sql, connection=CK_DB_CONFIG)
                        total_updated += 1
                        logger.info(f"更新记录: {eoms_no}")

            except Exception as e:
                logger.error(f"处理记录失败 {eoms_no}: {e}")
                continue

        if len(rows) < page_size:
            break

        page_index += 1
        time.sleep(0.5)

    logger.info(f"\n{worksheet_name} 同步完成: 新增 {total_synced} 条, 更新 {total_updated} 条")
    return total_synced, total_updated


def delete_submitted_records():
    """
    删除天山系统中已提交的记录

    返回值：
        返回删除的记录总数

    删除流程：
        1. 从ClickHouse 5个表中查询"是否提交=是"的EOMS单号
        2. 收集所有待删除的EOMS单号并去重
        3. 遍历天山5个工作表，根据EOMS单号找到对应的rowId
        4. 调用天山API删除指定rowId的记录
        5. 每处理一条记录延时0.2秒，避免请求过快

    删除条件：
        - 查询条件：`是否提交 = '是'`
        - 删除范围：5类工单工作表
        - 删除方式：触发工作流的物理删除

    去重处理：
        同一工单可能在多个表中出现（如申请延期和延期报结）
        需要去重后再遍历删除，避免重复删除

    异常处理：
        - 查询失败继续处理其他表
        - 删除失败继续处理其他表
        - 单条记录失败不影响其他记录
    """
    logger.info(f"\n{'=' * 60}")
    logger.info("开始删除天山中已提交的记录")
    logger.info(f"{'=' * 60}")

    four_tables = [
        ("申请报结", "wangyou.aran_tianshan_close"),
        ("申请延期", "wangyou.aran_tianshan_delay"),
        ("转派", "wangyou.aran_tianshan_forward"),
        ("退单", "wangyou.aran_tianshan_return"),
        ("延期报结", "wangyou.aran_tianshan_delay_close")
    ]

    submitted_eoms_set = set()

    for name, table in four_tables:
        try:
            sql = f"""
            SELECT `EOMS单号` FROM {table} WHERE `是否提交` = '是'
            """
            submitted_records = ph.read_clickhouse(sql, connection=CK_DB_CONFIG)

            if not submitted_records.empty:
                eoms_list = submitted_records["EOMS单号"].tolist()
                submitted_eoms_set.update(eoms_list)
                logger.info(f"{name} 表: 找到 {len(eoms_list)} 条待删除记录")
            else:
                logger.info(f"{name} 表: 无需删除的记录")

        except Exception as e:
            logger.error(f"查询 {name} 表失败: {e}")
            continue

    if not submitted_eoms_set:
        logger.info("没有需要删除的EOMS单号")
        return 0

    logger.info(f"\n共找到 {len(submitted_eoms_set)} 个需要删除的EOMS单号（去重后）")

    total_deleted = 0

    for eoms_no in submitted_eoms_set:
        logger.info(f"\n处理EOMS单号: {eoms_no}")

        for name, table in four_tables:
            worksheet_id = WORKSHEET_CONFIG[name]["worksheetId"]
            eoms_field = WORKSHEET_CONFIG[name]["eoms_field"]

            try:
                result = get_filter_rows(worksheet_id, page_size=1000)
                if not result:
                    logger.warning(f"  {name}表: 获取数据失败，跳过")
                    continue

                rows = result.get("rows", [])
                row_id = None
                for row in rows:
                    api_eoms_no = row.get(eoms_field, "")
                    if api_eoms_no == eoms_no:
                        row_id = row.get("rowid")
                        break

                if row_id:
                    if delete_row(worksheet_id, row_id):
                        total_deleted += 1
                        logger.info(f"  {name}表: 已删除")
                    else:
                        logger.warning(f"  {name}表: 删除失败")
                else:
                    logger.info(f"  {name}表: 未找到该单号")

                time.sleep(0.2)

            except Exception as e:
                logger.error(f"  {name}表: 删除异常 - {e}")
                continue

    logger.info(f"\n删除完成，总计删除 {total_deleted} 条记录")
    return total_deleted


# @cloudfunchelper.asyn_run(max_threads=5, timeout=3600)
def func():
    """
    主函数 - 程序入口

    执行流程：
        第1步：加载参考数据
            - 问题原因分类
            - 报结意见
            - 解决分类措施
            这些数据用于将天山系统中的关联字段rowId转换为实际显示文本

        第2步：同步工单数据
            - 按顺序同步5类工单工作表
            - 每类工单同步完成后延时1秒
            - 统计总新增和总更新记录数

        第3步：删除已提交记录
            - 从数据库查询所有"是否提交=是"的记录
            - 在天山系统中定位并删除这些记录

        第4步：输出执行统计
            - 打印总新增记录数
            - 打印总更新记录数
            - 打印执行开始和结束时间

    定时任务：
        通常配置为定时任务（如每日凌晨执行）
        可配合crontab或Windows任务计划程序使用

    典型输出示例：
        天山四类工单同步与删除任务开始
        开始同步工作表: 申请报结
        申请报结 同步完成: 新增 45 条, 更新 78 条
        ...
        删除完成，总计删除 31 条记录
        天山四类工单同步与删除任务完成
        总新增: 67 条
        总更新: 125 条
    """
    logger.info(f"\n{'#' * 60}")
    logger.info(f"# 天山四类工单同步与删除任务开始")
    logger.info(f"# 执行时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'#' * 60}\n")

    logger.info(f"\n{'=' * 60}")
    logger.info("开始加载参考数据（级联选择和关联记录）")
    logger.info(f"{'=' * 60}")

    ref_maps = {}
    for ref_name, ref_config in REFERENCE_WORKSHEETS.items():
        ref_map = get_reference_data(ref_name, ref_config)
        ref_maps[ref_name] = ref_map
        time.sleep(0.5)

    total_synced = 0
    total_updated = 0

    worksheets_to_sync = ["申请报结", "申请延期", "转派", "退单", "延期报结"]

    for worksheet_name in worksheets_to_sync:
        synced, updated = sync_worksheet_data(worksheet_name, ref_maps)
        total_synced += synced
        total_updated += updated
        time.sleep(1)

    delete_submitted_records()

    logger.info(f"\n{'#' * 60}")
    logger.info(f"# 天山四类工单同步与删除任务完成")
    logger.info(f"# 总新增: {total_synced} 条")
    logger.info(f"# 总更新: {total_updated} 条")
    logger.info(f"# 完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'#' * 60}\n")
func()