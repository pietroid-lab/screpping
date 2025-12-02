import time
import random
from scrapy import signals


class RotateUserAgentMiddleware:
    """Rotaciona User-Agent para cada requisição"""

    user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.57 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.58 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.58 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    ]

    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(self.user_agents)


class PauseOn302Middleware:
    """Pausa scraping ao detectar 302 (backoff progressivo)"""

    def __init__(self):
        self.fail_count = 0  # número de bloqueios consecutivos

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_response(self, request, response, spider):
        if response.status == 302:
            self.fail_count += 1

            # calcula pausa progressiva
            wait_time = min(420, 360 * self.fail_count)  
            spider.logger.warning(
                f"⚠️ Redirect 302 detectado! Pausando por {wait_time} segundos… " 
                f"(Tentativa {self.fail_count}) → URL: {request.url}"
            )

            time.sleep(wait_time)

        else:
            # Se voltou ao normal, reseta contador
            if self.fail_count > 0:
                spider.logger.info("✔ Status normalizado. Resetando contador de bloqueio.")
            self.fail_count = 0

        return response
