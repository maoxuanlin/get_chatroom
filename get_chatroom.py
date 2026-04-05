import requests


def get_chatroom_members(chatroom_wxid, server_url="http://10.74.16.108:35386"):
    """
    通过群组wxid从数据库获取群组成员信息，并提取wxid和昵称
    使用接口 /api/dbchatroom
    """
    url = f"{server_url}/api/dbchatroom"
    params = {"wxid": chatroom_wxid}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        result = response.json()

        if result.get("code") == 200:
            # 从data.Members中提取成员信息（根据实际返回结构调整）
            chatroom_data = result.get("data", {})
            members_dict = chatroom_data.get("Members", {})  # 成员信息在Members字典中

            # 提取UserName和NickName的映射关系
            wxid_nickname = []
            for member_info in members_dict.values():
                wxid = member_info.get("UserName", "未知wxid")
                nickname = member_info.get("NickName", "未知昵称")
                wxid_nickname.append({"wxid": wxid, "nickname": nickname})

            print(f"成功提取 {len(wxid_nickname)} 个成员的wxid和昵称")
            return wxid_nickname
        else:
            print(f"获取失败：{result.get('message', '未知错误')}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"请求异常：{str(e)}")
        return None


if __name__ == "__main__":
    test_chatroom_wxid = "17910632064@chatroom"  #目标群组
    members = get_chatroom_members(test_chatroom_wxid)
    wxid = ["chenjinnan596598", "maoxuanlin"]  #要查找的wxid
    members_dict = {m['wxid']: m['nickname'] for m in members}

    # 批量查找
    nickname_map = {
        wxid.strip(): members_dict.get(wxid.strip(), "未找到昵称")
        for wxid in wxid
    }
    print(nickname_map.values())