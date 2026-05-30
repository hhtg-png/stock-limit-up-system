import asyncio
import unittest
from datetime import date

from app.services.tdx_external_sources import (
    DabankeStockMoveProvider,
    ExternalStockMove,
    LwwhyStockMoveProvider,
    PublicStockMoveProvider,
)


class LwwhyStockMoveProviderTests(unittest.TestCase):
    def test_parse_review_action_card_extracts_move_title_content_and_board_label(self):
        html = """
        <div class="p-2 space-y-1">
          <p>
            <a class="underline text-primary font-medium" href="https://quote.eastmoney.com/sh603989.html?jump_to_web=true">603989</a>
            <a class="underline text-primary font-medium" href="/stock/detail/SH603989">艾华集团</a>
            <span class="badge badge-outline badge-sm badge-error">9天5板</span>
          </p>
          <p class="text-sm">电阻电容+数据中心（MLPC）+算力+充电桩</p>
          <p class="text-sm text-secondary line-clamp-2 break-words" title="1、AI服务器供电及VRM电源。\n2、产品定向配套新能源。">省略内容</p>
        </div>
        """

        moves = LwwhyStockMoveProvider.parse_review_action_html(html, date(2026, 5, 28))

        self.assertEqual(len(moves), 1)
        self.assertEqual(moves[0].stock_code, "603989")
        self.assertEqual(moves[0].stock_name, "艾华集团")
        self.assertEqual(moves[0].board_label, "9天5板")
        self.assertEqual(moves[0].title, "电阻电容+数据中心（MLPC）+算力+充电桩")
        self.assertIn("AI服务器供电", moves[0].content)

    def test_parse_stock_detail_html_extracts_latest_move_section(self):
        html = """
        <div class="border rounded">
          <div class="border-b px-4 py-3">
            <span class="font-bold text-primary">最新异动解析</span>
            <span class="text-secondary text-sm ml-2">(2026-05-28)</span>
          </div>
          <div class="p-4 space-y-1">
            <p>字节算力+算力租赁+数据中心</p>
            <p class="text-secondary" title="1、字节资本开支扩建数据中心。\n2、公司提供租赁服务。\n3、森华易腾提供IDC服务。">省略内容</p>
          </div>
        </div>
        """

        move = LwwhyStockMoveProvider.parse_stock_detail_html(html, "600589")

        self.assertIsInstance(move, ExternalStockMove)
        self.assertEqual(move.stock_code, "600589")
        self.assertEqual(move.trade_date, date(2026, 5, 28))
        self.assertEqual(move.title, "字节算力+算力租赁+数据中心")
        self.assertIn("森华易腾", move.content)

    def test_parse_stock_detail_html_skips_latest_move_metadata_before_real_title(self):
        html = """
        <div class="border rounded">
          <div class="border-b px-4 py-3">
            <span class="font-bold text-primary">最新异动解析</span>
            <span class="text-secondary text-sm ml-2">(2026-05-15)</span>
          </div>
          <div class="p-4 space-y-1">
            <p>板块:</p>
            <p>机器人</p>
            <p>异动时间:</p>
            <p>10:50:53</p>
            <p>机器人+宁波国资+家电零部件+冷锻工艺</p>
            <p class="text-secondary" title="1、机器人零部件小批交样。\n2、实控人是宁波国资委。">省略内容</p>
          </div>
        </div>
        """

        move = LwwhyStockMoveProvider.parse_stock_detail_html(html, "603677")

        self.assertIsInstance(move, ExternalStockMove)
        self.assertEqual(move.plate, "机器人")
        self.assertEqual(move.title, "机器人+宁波国资+家电零部件+冷锻工艺")
        self.assertIn("宁波国资委", move.content)

    def test_parse_stock_detail_html_skips_inline_board_count_metadata(self):
        html = """
        <div class="border rounded">
          <div class="border-b px-4 py-3">
            <span class="font-bold text-primary">最新异动解析</span>
            <span class="text-secondary text-sm ml-2">(2026-05-06)</span>
          </div>
          <div class="p-4 space-y-1">
            <p>板块: 电池产业链</p>
            <p>异动时间: 09:25:00</p>
            <p>连板: 4天4板</p>
            <p>锂矿+一季度业绩扭亏</p>
            <p class="text-secondary" title="1、锂盐产品价格上涨。\n2、年产2.2万吨高纯度锂盐项目已开工。">省略内容</p>
          </div>
        </div>
        """

        move = LwwhyStockMoveProvider.parse_stock_detail_html(html, "603399")

        self.assertIsInstance(move, ExternalStockMove)
        self.assertEqual(move.plate, "电池产业链")
        self.assertEqual(move.board_label, "4天4板")
        self.assertEqual(move.title, "锂矿+一季度业绩扭亏")
        self.assertIn("锂盐产品价格上涨", move.content)


class DabankeStockMoveProviderTests(unittest.TestCase):
    def test_parse_latest_history_move_and_enriches_with_ths_concepts(self):
        dabanke_html = """
        <html><head><title>通达动力(SZ002576) 股票涨停原因</title></head>
        <body>
          <table>
            <tbody>
              <tr>
                <td>2025-08-20</td>
                <td>09:44:51</td>
                <td>涨停</td>
                <td class="text-start">机器人概念+业绩增长+电机铁芯 · 世界机器人大会召开；宇树科技...</td>
              </tr>
            </tbody>
          </table>
        </body></html>
        """
        ths_html = """
        <table>
          <tr>
            <td>1</td><td class="gnName">机器人概念</td><td></td><td class="wider">简略</td>
          </tr>
          <tr class="extend_content"><td colspan="4">
            根据2025年3月19日互动易回复：人形机器人市场正处于高速发展期，目前公司生产的伺服电机铁芯可适用于机器人的驱动电机。
          </td></tr>
          <tr>
            <td>2</td><td class="gnName">新能源汽车</td><td></td><td class="wider">简略</td>
          </tr>
          <tr class="extend_content"><td colspan="4">
            2024年12月12日互动易回复，公司生产新能源汽车驱动电机铁芯，适用于所有新能源汽车。
          </td></tr>
          <tr>
            <td>3</td><td class="gnName">比亚迪概念</td><td></td><td class="wider">简略</td>
          </tr>
          <tr class="extend_content"><td colspan="4">
            根据2023年9月22日互动易：公司向比亚迪提供的是主驱动电机的定转子铁芯。
          </td></tr>
        </table>
        """

        move = DabankeStockMoveProvider.parse_stock_history_html(dabanke_html, "002576", ths_html)

        self.assertIsInstance(move, ExternalStockMove)
        self.assertEqual(move.stock_code, "002576")
        self.assertEqual(move.stock_name, "通达动力")
        self.assertEqual(move.trade_date, date(2025, 8, 20))
        self.assertEqual(move.title, "机器人+核心客户比亚迪+驱动电机铁芯+新能源汽车")
        self.assertIn("伺服电机铁芯", move.content)
        self.assertIn("比亚迪", move.content)


class PublicStockMoveProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_stock_move_prefers_lwwhy_when_both_sources_have_data(self):
        lwwhy_move = ExternalStockMove(
            stock_code="603677",
            stock_name="奇精机械",
            trade_date=date(2026, 5, 15),
            title="机器人+宁波国资+家电零部件+冷锻工艺",
            content="目标站口径解析",
        )
        dabanke_move = ExternalStockMove(
            stock_code="603677",
            stock_name="奇精机械",
            trade_date=date(2026, 5, 15),
            title="机器人概念",
            content="历史涨停原因",
        )
        lwwhy = _FakeStockMoveProvider(lwwhy_move)
        dabanke = _FakeStockMoveProvider(dabanke_move)
        provider = PublicStockMoveProvider(lwwhy_provider=lwwhy, dabanke_provider=dabanke)

        move = await provider.get_stock_move("603677")

        self.assertIs(move, lwwhy_move)
        self.assertEqual(lwwhy.calls, 1)
        self.assertEqual(dabanke.calls, 1)

    async def test_stock_move_falls_back_to_dabanke_when_lwwhy_missing(self):
        dabanke_move = ExternalStockMove(
            stock_code="002576",
            stock_name="通达动力",
            trade_date=date(2025, 8, 20),
            title="机器人+核心客户比亚迪+驱动电机铁芯+新能源汽车",
            content="历史涨停和同花顺F10合成解析",
        )
        lwwhy = _FakeStockMoveProvider(None)
        dabanke = _FakeStockMoveProvider(dabanke_move)
        provider = PublicStockMoveProvider(lwwhy_provider=lwwhy, dabanke_provider=dabanke)

        move = await provider.get_stock_move("002576")

        self.assertIs(move, dabanke_move)
        self.assertEqual(lwwhy.calls, 1)
        self.assertEqual(dabanke.calls, 1)

    async def test_stock_move_returns_dabanke_before_slow_empty_lwwhy(self):
        dabanke_move = ExternalStockMove(
            stock_code="002576",
            stock_name="通达动力",
            trade_date=date(2025, 8, 20),
            title="机器人+核心客户比亚迪+驱动电机铁芯+新能源汽车",
            content="历史涨停和同花顺F10合成解析",
        )
        lwwhy = _FakeStockMoveProvider(None, delay=0.2)
        dabanke = _FakeStockMoveProvider(dabanke_move)
        provider = PublicStockMoveProvider(
            lwwhy_provider=lwwhy,
            dabanke_provider=dabanke,
            lwwhy_prefer_timeout=0.01,
        )

        move = await provider.get_stock_move("002576")

        self.assertIs(move, dabanke_move)
        self.assertEqual(lwwhy.calls, 1)
        self.assertEqual(dabanke.calls, 1)


class _FakeStockMoveProvider:
    def __init__(self, move, delay=0):
        self.move = move
        self.delay = delay
        self.calls = 0

    async def get_stock_move(self, stock_code, trade_date=None):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.move

