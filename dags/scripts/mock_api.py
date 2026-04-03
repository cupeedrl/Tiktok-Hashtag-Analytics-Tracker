"""
Mock TikTok API Simulation
Generates deterministic hashtag data for testing.
"""

import random
import datetime
import hashlib
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TikTokMockAPI:
    """Mock API for TikTok hashtag statistics"""
    
    def __init__(self):
        self.hashtags = [
            # Technology
            "#tech", "#ai", "#coding",
            # Business
            "#marketing", "#entrepreneur", "#business",
            # Entertainment
            "#dance", "#music", "#comedy",
            # Lifestyle
            "#travel", "#food", "#fashion",
            # Education
            "#learnontiktok",
            # Health
            "#fitness", "#health",
        ]
    
    def _generate_seed(self, hashtag: str, report_date) -> int:
        """Generate deterministic seed for reproducibility"""
        seed_str = f"{hashtag}_{report_date}"
        return int(hashlib.md5(seed_str.encode()).hexdigest(), 16) % (10**8)
    
    def get_hashtag_stats(self, hashtag: str, report_date) -> Dict:
        """Get statistics for a single hashtag"""
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
            "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    
    def fetch_all_hashtags(self, report_date) -> List[Dict]:
        """Fetch data for all hashtags - ALWAYS return 15 records/day (1 per hashtag)"""
        logger.info(f"Fetching data for {report_date}")
        
        data = []
        
        # Loop qua TẤT CẢ 15 hashtags - không random selection
        for hashtag in self.hashtags:
            # Generate base stats với deterministic seed
            stat = self.get_hashtag_stats(hashtag, report_date)
            
            # Add variation để data trông "thực tế" hơn
            stat["views"] += random.randint(-3000, 3000)
            stat["likes"] += random.randint(-300, 300)
            stat["shares"] += random.randint(-80, 80)
            stat["comments"] += random.randint(-50, 50)
            
            if stat["views"] < 100:
                stat["views"] = 100
            
            data.append(stat)
        
        # Shuffle để order không bị predictable
        random.shuffle(data)
        
        logger.info(f"Fetched {len(data)} records ({len(self.hashtags)} hashtags)")
        return data