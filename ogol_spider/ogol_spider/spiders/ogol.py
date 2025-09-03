import scrapy


class OgolSpider(scrapy.Spider):
    name = "ogol"
    allowed_domains = ["ogol.com.br"]

    start_year = 2000
    end_year = 2025

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.teams = crawler.settings.get("OGOL_TEAMS", ["sao-paulo"])
        return spider

    def start_requests(self):
        base_url = (
            "https://www.ogol.com.br/equipe/{team}/todos-os-jogos"
            "?grp=1&ond=&compet_id_jogos=0&epoca_id=154"
            "&ano={year}&ano_fim=2025&type=year&epoca_id_fim=0&comfim=0&page={page}"
        )

        for team in self.teams:
            for year in range(self.start_year, self.end_year + 1):
                for page in [1, 2]:
                    url = base_url.format(team=team, year=year, page=page)
                    yield scrapy.Request(
                        url,
                        headers=self.headers,
                        callback=self.parse,
                        meta={"year": year, "page": page, "team": team},
                    )

    def parse(self, response):
        year = response.meta["year"]
        page = response.meta["page"]
        team = response.meta["team"]

        rows = response.css("div#team_games table.zztable.stats tbody tr")

        if not rows:
            self.logger.info(f"Nenhum jogo encontrado para {team} {year} página {page}")
            return

        for row in rows:
            yield {
                "time": team,
                "ano": year,
                "resultado": " ".join(row.css("td:nth-child(1) *::text").getall()).strip(),
                "data": " ".join(row.css("td:nth-child(2) *::text").getall()).strip(),
                "hora": " ".join(row.css("td:nth-child(3) *::text").getall()).strip(),
                "casa/fora": " ".join(row.css("td:nth-child(4) *::text").getall()).strip(),
                "resultado": " ".join(row.css("td:nth-child(7) *::text").getall()).strip(),
            }
