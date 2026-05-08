import unittest

from app.data_collectors.scheduler import DataScheduler


class DailyAnalysisSchedulerTests(unittest.TestCase):
    def test_start_registers_daily_analysis_after_close_job(self):
        scheduler = DataScheduler()
        scheduler.scheduler.start = lambda: None

        scheduler.start()

        job_ids = {job.id for job in scheduler.scheduler.get_jobs()}
        self.assertIn("daily_analysis", job_ids)


if __name__ == "__main__":
    unittest.main()
