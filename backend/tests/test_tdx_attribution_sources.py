import unittest

from app.services.tdx_attribution_sources import (
    FupanwangThsAttributionProvider,
    StockConcept,
)


class FupanwangThsAttributionProviderTests(unittest.TestCase):
    def test_parse_fupanwang_reason_extracts_public_limit_up_title(self):
        html = """
        <html><body>
        <section>涨停原因： 医药(原料药)；据2026年5月19日投资者关系活动记录表，公司主要从事化学原料药。</section>
        </body></html>
        """

        reason = FupanwangThsAttributionProvider.parse_fupanwang_reason(html)

        self.assertEqual(reason, "医药(原料药)")

    def test_parse_ths_concepts_extracts_concept_names_and_summaries(self):
        html = """
        <table class="gnContent"><tbody>
          <tr>
            <td>1</td><td class="gnName">家用电器</td><td></td>
            <td class="wider"><div class="tdContent">公司主营家电零部件、汽车零部件。</div></td>
          </tr>
          <tr>
            <td>2</td><td class="gnName">机器人概念</td><td></td>
            <td class="wider"><div class="tdContent">工业机器人零部件进入客户验证。</div></td>
          </tr>
        </tbody></table>
        """

        concepts = FupanwangThsAttributionProvider.parse_ths_concepts(html)

        self.assertEqual(concepts[0], StockConcept(name="家用电器", summary="公司主营家电零部件、汽车零部件。"))
        self.assertEqual(concepts[1].name, "机器人概念")

    def test_infer_plate_concepts_uses_fupan_parentheses_like_target_board(self):
        plate, concepts = FupanwangThsAttributionProvider.infer_plate_concepts(
            "地产链(房地产)",
            [],
        )

        self.assertEqual(plate, "地产链")
        self.assertEqual(concepts[:2], ["房地产", "地产链"])

    def test_infer_plate_concepts_uses_ths_secondary_concept_for_target_like_display(self):
        plate, concepts = FupanwangThsAttributionProvider.infer_plate_concepts(
            "汽车零部件+锂电池",
            [
                StockConcept("汽车零部件", "公司生产新能源汽车结构件。"),
                StockConcept("比亚迪概念", "公司已获得比亚迪、广汽埃安等新能源车厂供应商代码。"),
            ],
        )

        self.assertEqual(plate, "汽车零部件")
        self.assertEqual(concepts[0], "比亚迪产业链")

    def test_infer_plate_concepts_keeps_mlcc_stock_in_component_theme(self):
        plate, concepts = FupanwangThsAttributionProvider.infer_plate_concepts(
            "",
            [
                StockConcept("超级电容", "公司电阻器、MLCC、射频电感等产品已直接应用于光模块产品上。"),
                StockConcept("共封装光学(CPO)", "光模块相关应用。"),
            ],
        )

        self.assertEqual(plate, "元器件")
        self.assertEqual(concepts[0], "电阻电容")

    def test_infer_plate_concepts_prefers_robot_hot_theme_over_home_appliance_profile(self):
        plate, concepts = FupanwangThsAttributionProvider.infer_plate_concepts(
            "",
            [
                StockConcept("家用电器", "公司的主营业务是洗衣机离合器等家电零部件、汽车零部件。"),
                StockConcept("机器人概念", "公司工业机器人零部件已经进入客户验证阶段。"),
            ],
        )

        self.assertEqual(plate, "机器人概念")
        self.assertEqual(concepts[0], "汽车零部件")

    def test_infer_plate_concepts_adds_medical_secondary_for_innovation_drug(self):
        plate, concepts = FupanwangThsAttributionProvider.infer_plate_concepts(
            "",
            [
                StockConcept("细胞免疫治疗", "创新药相关研发。"),
                StockConcept("创新药", "公司为医药研发企业。"),
            ],
        )

        self.assertEqual(plate, "创新药")
        self.assertEqual(concepts[0], "医药")


if __name__ == "__main__":
    unittest.main()
