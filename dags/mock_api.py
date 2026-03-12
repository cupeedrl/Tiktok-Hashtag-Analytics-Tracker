import random
import datetime
import hashlib
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TikTokMockAPI:

    def __init__(self):
        self.hashtags = [
            "#marketing",
            "#learnontiktok",
            "#dance",
            "#food",
            "#tech",
            "#travel",
        ]

    def _generate_seed(self, hashtag, report_date):
        """Tạo seed deterministic cho mỗi hashtag+date"""
        seed_str = f"{hashtag}_{report_date}"
        return int(hashlib.md5(seed_str.encode()).hexdigest(), 16) % (10**8)

    def get_hashtag_stats(self, hashtag: str, report_date) -> Dict:
        """Generate stats cho 1 hashtag vào 1 ngày cụ thể"""

        if isinstance(report_date, str):
            report_date = datetime.datetime.strptime(report_date, "%Y-%m-%d").date()

        random.seed(self._generate_seed(hashtag, report_date))

        base_views = random.randint(5000, 100000)
        base_likes = int(base_views * random.uniform(0.05, 0.15))
        base_shares = int(base_views * random.uniform(0.01, 0.05))
        base_comments = int(base_views * random.uniform(0.005, 0.02))

        engagement_rate = round(
            (base_likes + base_shares + base_comments) / base_views, 4
        )

        return {
            "hashtag": hashtag,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "views": base_views,
            "likes": base_likes,
            "shares": base_shares,
            "comments": base_comments,
            "engagement_rate": engagement_rate,
            "extracted_at": datetime.datetime.utcnow().isoformat(),
        }

    def fetch_all_hashtags(self, report_date):
        """Fetch data cho tất cả hashtags vào 1 ngày"""
        logger.info(f"Fetching data for {report_date}")

        data = []
        num_records = random.randint(20, 40)

        for i in range(num_records):
            tag = random.choice(self.hashtags)
            stat = self.get_hashtag_stats(tag, report_date)
            stat["views"] += random.randint(-3000, 3000)
            stat["likes"] += random.randint(-300, 300)
            stat["shares"] += random.randint(-80, 80)
            stat["comments"] += random.randint(-50, 50)

            if stat["views"] < 100:
                stat["views"] = 100

            data.append(stat)

        random.shuffle(data)

        return data