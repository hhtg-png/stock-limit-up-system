"""
更新换手率数据 - 计算真实换手率(成交量/自由流通股本)
"""
import asyncio
import sys
import time
sys.path.insert(0, '.')

from datetime import date
from sqlalchemy import select
from app.database import async_session_maker
from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord
from app.models.big_order import BigOrder
from app.models.order_flow import OrderBookSnapshot
from app.utils.real_turnover import calc_real_turnover_rate


async def update_turnover_rates():
    """计算真实换手率并更新数据库"""
    print("正在计算真实换手率(成交量/自由流通股本)...")

    try:
        async with async_session_maker() as db:
            query = (
                select(LimitUpRecord, Stock)
                .join(Stock, LimitUpRecord.stock_id == Stock.id)
                .where(LimitUpRecord.trade_date == date.today())
            )
            result = await db.execute(query)
            records = result.all()

            if not records:
                print("今天没有涨停记录")
                return

            print(f"需要更新 {len(records)} 只股票的真实换手率\n")
            print(f"{'代码':<8} {'名称':<10} {'真实换手率':>10}")
            print("-" * 40)

            updated_count = 0
            for record, stock in records:
                real_tr, circ_shares = calc_real_turnover_rate(
                    stock.stock_code, stock.market
                )
                if real_tr is not None:
                    old_tr = record.turnover_rate
                    record.turnover_rate = real_tr
                    if circ_shares:
                        stock.circulating_shares = circ_shares
                    updated_count += 1
                    print(f"{stock.stock_code:<8} {stock.stock_name:<10} {old_tr:>7.2f}% -> {real_tr:>7.2f}%")
                else:
                    print(f"{stock.stock_code:<8} {stock.stock_name:<10} 获取数据失败")

                time.sleep(0.2)

            await db.commit()
            print(f"\n成功更新 {updated_count}/{len(records)} 条记录的真实换手率")

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(update_turnover_rates())
