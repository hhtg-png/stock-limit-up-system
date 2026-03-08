import asyncio
import httpx

async def test():
    url = 'https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://data.10jqka.com.cn/',
        'Accept': 'application/json'
    }
    
    for limit in [100, 200, 250, 300]:
        params = {
            'page': 1, 'limit': limit,
            'field': '199112,10,9001,330323,330324,330325,9002,330329,133,330326,330327,330328',
            'filter': 'HS,GEM2STAR', 'order_field': '330324', 'order_type': 0
        }
        async with httpx.AsyncClient(verify=False, timeout=15) as client:
            resp = await client.get(url, params=params, headers=headers)
            data = resp.json()
            sc = data.get('status_code')
            info = data.get('data', {}).get('info', [])
            print(f'limit={limit}: status_code={sc}, info count={len(info)}')

asyncio.run(test())
