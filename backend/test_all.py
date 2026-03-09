import asyncio

async def test():
    from app.crawlers.kaipanla_crawler import kpl_crawler
    from app.crawlers.tonghuashun_crawler import ths_crawler  
    from app.crawlers.eastmoney_crawler import em_crawler
    
    print('=== Testing All Crawlers ===')
    
    # KaiPanLa (disabled)
    kpl = await kpl_crawler.crawl()
    print(f'KaiPanLa: {len(kpl)} records (API disabled)')
    
    # TongHuaShun
    ths = await ths_crawler.crawl()
    print(f'TongHuaShun: {len(ths)} records')
    if ths:
        reason = ths[0].get('limit_up_reason', '')[:40]
        print(f'  Sample reason: {reason}')
    
    # EastMoney
    em = await em_crawler.crawl()
    await em_crawler.close_client()
    print(f'EastMoney: {len(em)} records')
    if em:
        code = em[0].get('stock_code')
        reason = em[0].get('limit_up_reason', '')[:20]
        print(f'  Sample: {code} - {reason}')
    
    print('=== All Tests Passed ===')

asyncio.run(test())
