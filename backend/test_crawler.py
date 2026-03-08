import asyncio
from app.crawlers.eastmoney_crawler import em_crawler
from datetime import date

async def test():
    print("Testing crawler...")
    data = await em_crawler.crawl(date.today())
    sealed = sum(1 for d in data if d.get('is_final_sealed'))
    opened = sum(1 for d in data if not d.get('is_final_sealed'))
    print(f'Total: {len(data)}, Sealed: {sealed}, Opened: {opened}')
    if opened > 0:
        print("First opened stock:", [d for d in data if not d.get('is_final_sealed')][0])
    
if __name__ == "__main__":
    asyncio.run(test())
