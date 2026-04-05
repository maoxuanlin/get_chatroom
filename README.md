# get_chatroom

通过群组wxid获取微信群组成员信息的工具。

## 功能

- 根据群组wxid查询群成员列表
- 提取成员的wxid和昵称
- 支持批量查找指定wxid的昵称

## 依赖

```bash
pip install requests
```

## 使用方法

```python
from get_chatroom import get_chatroom_members

# 获取群成员信息
chatroom_wxid = "12345678@chatroom"
members = get_chatroom_members(chatroom_wxid)

# 返回格式
# [{"wxid": "wxid_xxx", "nickname": "昵称"}, ...]
```

## 参数说明

| 参数 | 说明 |
|------|------|
| chatroom_wxid | 目标群组的wxid，格式如 `12345678@chatroom` |
| server_url | API服务地址，默认为 `http://10.74.16.108:35386` |

## 返回值

成功时返回成员列表，每个成员包含：
- `wxid`: 成员的微信ID
- `nickname`: 成员昵称

失败时返回 `None`。

## 示例

```python
# 获取群成员并批量查找昵称
members = get_chatroom_members("17910632064@chatroom")
target_wxids = ["wxid1", "wxid2"]

members_dict = {m['wxid']: m['nickname'] for m in members}
nicknames = [members_dict.get(wxid, "未找到") for wxid in target_wxids]
print(nicknames)
```

## License

MIT
