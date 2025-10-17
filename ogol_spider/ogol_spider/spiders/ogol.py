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
            jogo_link = row.css("td:nth-child(7) a::attr(href)").get()
            jogo_url = response.urljoin(jogo_link) if jogo_link else None

            item = {
                "time": team,
                "ano": year,
                "resultado": " ".join(row.css("td:nth-child(1) *::text").getall()).strip(),
                "data": " ".join(row.css("td:nth-child(2) *::text").getall()).strip(),
                "hora": " ".join(row.css("td:nth-child(3) *::text").getall()).strip(),
                "casa/fora": " ".join(row.css("td:nth-child(4) *::text").getall()).strip(),
                "placar": " ".join(row.css("td:nth-child(7) *::text").getall()).strip(),
            }

           # Pega link da partida (se existir)
            match_link = row.css("td:nth-child(7) a::attr(href)").get()
            if match_link:
                yield response.follow(
                    match_link,
                    headers=self.headers,
                    callback=self.parse_game,
                    cb_kwargs={"match_data": item},
                )
            else:
                yield item

    def parse_game(self, response, match_data):
        """
        Extrai escalações, estatísticas em destaque e cartões da página do jogo
        """

        # Escalações com cartões
        lineups = {"home": [], "away": []}

        sides = response.css("div.zz-tpl-row.game_report div.zz-tpl-col.is-6.fl-c")
        if len(sides) == 2:
            home_players = sides[0].css("div.player")
            away_players = sides[1].css("div.player")

            for p in home_players:
                name =  p.css("div.name div.micrologo_and_text div.text a::text").get()
                events = []
                for e in p.css("div.events span"):
                    title = e.attrib.get("title", "").lower()
                    if "amarelo" in title:
                        events.append("cartao_amarelo")
                    elif "vermelho" in title:
                        events.append("cartao_vermelho")
                lineups["home"].append({"nome": name, "eventos": events})

            for p in away_players:
                name = name =  p.css("div.name div.micrologo_and_text div.text a::text").get()
                events = []
                for e in p.css("div.events span"):
                    title = e.attrib.get("title", "").lower()
                    if "amarelo" in title:
                        events.append("cartao_amarelo")
                    elif "vermelho" in title:
                        events.append("cartao_vermelho")
                lineups["away"].append({"nome": name, "eventos": events})

        # Estatísticas em destaque (ajustar caso o site mude)
        stats = {}
        for row in response.css("div#match_stats .statRow"):
            label = row.css(".statLabel::text").get()
            home_val = row.css(".homeStat::text").get()
            away_val = row.css(".awayStat::text").get()
            if label:
                stats[label.strip()] = {"home": home_val, "away": away_val}

        # Retorna dados combinados
        match_data.update(
            {
                "escalacoes": lineups,
                "estatisticas": stats,
            }
        )
        yield match_data
