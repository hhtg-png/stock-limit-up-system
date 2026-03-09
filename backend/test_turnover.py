import requests

def test():
    # 华盛昌 002980
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/'
    }
    
    # 获取东方财富数据
    resp = requests.get(
        'https://push2.eastmoney.com/api/qt/stock/get',
        params={
            'secid': '0.002980',
            'fields': 'f43,f44,f45,f46,f47,f117,f183',
            'ut': 'fa5fd1943c7b386f172d6893dbbd1',
            'fltt': '2'
        },
        headers=headers,
        timeout=10
    )
    data = resp.json()
    print("东方财富返回:", data)
    
    if data.get('data'):
        d = data['data']
        price = d.get('f43', 0) / 100  # 当前价（分->元）
        volume = d.get('f47', 0)  # 成交量（手）
        circ_mv = d.get('f117', 0)  # 流通市值
        free_mv = d.get('f183', 0)  # 自由流通市值
        
        print(f"\n当前价: {price}")
        print(f"成交量: {volume}手")
        print(f"流通市值: {circ_mv/100000000:.2f}亿")
        print(f"自由流通市值: {free_mv/100000000:.2f}亿")
        
        # 成交额
        amount = volume * 100 * price  # 手*100*价格 = 元
        print(f"成交额: {amount/100000000:.2f}亿")
        
        # 普通换手率 = 成交额/流通市值*100
        if circ_mv > 0:
            normal_turnover = amount / circ_mv * 100
            print(f"\n普通换手率: {normal_turnover:.2f}%")
        
        # 真实换手率 = 成交额/自由流通市值*100
        if free_mv > 0:
            real_turnover = amount / free_mv * 100
            print(f"真实换手率: {real_turnover:.2f}%")

test()
