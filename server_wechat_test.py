import requests

def wechat_msg(text):
    key = "SCT297293TIHpzTNsOteNlEj0ssdh4TH2m"
    url = f"https://sctapi.ftqq.com/{key}.send"
    data = {"title": "通知", "desp": text}
    requests.post(url, data=data)

if __name__ == "__main__":
    wechat_msg("测试：Server酱通知来了 ✅")
