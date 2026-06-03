import asyncio
import json
import logging
import random
import time
from datetime import datetime

from api.lightning import broadcast_lightning

logger = logging.getLogger(__name__)

async def run_lightning_crawler():
    """
    Kết nối tới nguồn dữ liệu Sét (ví dụ: Blitzortung WebSocket).
    Để tránh bị block khi IP không có whitelist, chúng ta sẽ viết mock 
    tạo ra các tia sét ngẫu nhiên xung quanh khu vực bão hoặc ngẫu nhiên toàn cầu.
    """
    logger.info("Started Realtime Lightning Crawler (Mock Mode)")
    
    # Vòng lặp vĩnh viễn duy trì kết nối
    while True:
        try:
            # TODO: Dùng thư viện websockets để connect tới wss://...
            # async with websockets.connect('wss://ws.blitzortung.org/') as ws:
            #     while True:
            #         msg = await ws.recv()
            #         data = json.loads(msg)
            #         await broadcast_lightning(data)
            
            # Giả lập tia sét ngẫu nhiên mỗi 1-3 giây
            await asyncio.sleep(random.uniform(1.0, 3.0))
            
            # Random tọa độ (tập trung gần biển Đông cho demo)
            lat = random.uniform(5.0, 25.0)
            lon = random.uniform(100.0, 120.0)
            
            strike_data = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "time": datetime.utcnow().isoformat() + "Z",
                    "polarity": random.choice(["+", "-"]),
                    "current": random.uniform(10.0, 100.0) # kA
                }
            }
            
            # Gửi tới tất cả client đang mở WebSocket của BE
            await broadcast_lightning(strike_data)
            
        except asyncio.CancelledError:
            logger.info("Lightning Crawler stopped.")
            break
        except Exception as e:
            logger.error(f"Lightning Connection Error: {e}")
            await asyncio.sleep(5) # Reconnect delay
