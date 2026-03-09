import httpx
import asyncio
import sys
sys.path.insert(0, '.')
from app.data_collectors.tencent_api import tencent_api

async def test():
    # 先测试东方财富获取连板股
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            'https://push2ex.eastmoney.com/getTopicZTPool',
            params={
                'ut':'7eea3edcaed734bea9cbfc24409ed989',
                'dpt':'wz.ztzt',
                'Pageindex':'0',
                'pagesize':'10000',
                'sort':'fbt:asc',
                'date':'20260303'
            },
            headers={'User-Agent':'Mozilla/5.0'}
        )
        data = resp.json()
        pool = data.get('data', {}).get('pool', [])
        
        # 统计连板股
        continuous = [p for p in pool if p.get('lbc', 1) >= 2]
        print(f'\n东方财富连板股: {len(continuous)}只')
        
        # 获取这些股票的腾讯行情
        codes = [p['c'] for p in continuous[:5]]
        print(f'\n输入股票代码: {codes}')
        
        quotes = await tencent_api.get_quotes_batch(codes)
        print(f'\n腾讯返回的keys: {list(quotes.keys())}')
        
        # 检查key是否匹配
        for code in codes:
            print(f"\n{code}: 在quotes中={code in quotes}")
            if code in quotes:
                print(f"  找到数据: {quotes[code].get('name')}")
            else:
                print(f"  尝试其他匹配...")
                for k in quotes.keys():
                    if code in k or k in code:
                        print(f"  可能匹配: {k}")

asyncio.run(test())
