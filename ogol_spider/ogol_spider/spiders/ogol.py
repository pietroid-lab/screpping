import scrapy
import unicodedata


# -------------------------
# 🔥 DERBIES ESTADUAIS
# -------------------------
DERBIES = {
    "SP": ["palmeiras", "corinthians", "sao-paulo", "santos"],
    "RJ": ["flamengo", "vasco", "botafogo", "fluminense"],
    "MG": ["atletico-mineiro", "cruzeiro"],
    "RS": ["gremio", "internacional"],
    "CE": ["ceara", "fortaleza"],
    "BA": ["bahia", "vitoria"],
}


def normalize_name(name):
    """Remove acentos, transforma em lowercase e troca espaços/underscores por hífens."""
    if not name:
        return ""

    name = name.strip().lower()
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.replace(" ", "-").replace("_", "-")
    return name


def get_group(team_slug):
    """Retorna o grupo de clássicos em que o time pertence."""
    for group, teams in DERBIES.items():
        if team_slug in teams:
            return group
    return None


def is_classico(time_slug, adversario_nome):
    """Diz se o jogo é clássico estadual."""
    adversario_slug = normalize_name(adversario_nome)

    grupo = get_group(time_slug)
    if not grupo:
        return False  # time não está listado

    return adversario_slug in DERBIES[grupo]


# -------------------------------------------------------
# SCRAPY SPIDER
# -------------------------------------------------------

class OgolSpider(scrapy.Spider):
    name = "ogol"
    allowed_domains = ["ogol.com.br"]

    start_year = 2000
    end_year = 2025

    headers = {
        "User-Agent": (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.57 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.58 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.58 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"

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
                        meta={"year": year, "page": page, "team": team,},
                    )

    def parse(self, response):
        year = response.meta["year"]
        page = response.meta["page"]
        team_slug = response.meta["team"]

        rows = response.css("div#team_games table.zztable.stats tbody tr")

        if not rows:
            self.logger.info(f"Nenhum jogo encontrado para {team_slug} {year} página {page}")
            return

        for row in rows:
            adversario = " ".join(row.css("td:nth-child(6) *::text").getall()).strip()

            # -------------------------------------------
            # 🧠 FILTRO: SOMENTE CLÁSSICOS ESTADUAIS
            # -------------------------------------------
            if not is_classico(team_slug, adversario):
                continue  # ignora jogo que não é clássico

            jogo_link = row.css("td a[href*='/jogo/']::attr(href)").get()
            jogo_url = response.urljoin(jogo_link) if jogo_link else None

            item = {
                "time": team_slug,
                "ano": year,
                "data": row.css("td:nth-child(2)::text").get(default="").strip(),
                "competicao": row.css("td:nth-child(8) *::text").get(default="").strip(),
                "adversario": adversario,
                "local": row.css("td:nth-child(4)::text").get(default="").strip(),
                "resultado": " ".join(row.css("td:nth-child(1) *::text").getall()).strip(),
                "placar": " ".join(row.css("td:nth-child(7) *::text").getall()).strip(),
            }

            if jogo_url:
                yield response.follow(
                    jogo_url,
                    headers=self.headers,
                    callback=self.parse_game,
                    cb_kwargs={"match_data": item},
                )
            else:
                yield item

    # -------------------------------------------------------
    # PARSE DO JOGO (mantido igual ao seu atual)
    # -------------------------------------------------------
    def parse_game(self, response, match_data):
        lineups = {"home": [], "away": []}

        sides = response.css("div.zz-tpl-row.game_report div.zz-tpl-col.is-6.fl-c")
        for i, side_key in enumerate(["home", "away"]):
            for player in sides[i].css("div.player"):
                name = player.css("div.name div.micrologo_and_text div.text a::text").get(default="").strip()
                events = []
                for e in player.css("div.events span"):
                    title = e.attrib.get("title", "").lower()
                    if "amarelo" in title:
                        events.append("cartao_amarelo")
                    elif "vermelho" in title:
                        events.append("cartao_vermelho")
                if name:
                    lineups[side_key].append({"nome": name, "eventos": events})

        if not lineups["home"] and not lineups["away"]:
            old_sides = response.css("div#match_players div.team_players")
            if len(old_sides) == 2:
                for i, side_key in enumerate(["home", "away"]):
                    for row in old_sides[i].css("table tr, ul li"):
                        name = row.css("td.name a::text, a::text").get(default="").strip()
                        if name:
                            lineups[side_key].append({"nome": name, "eventos": []})

        stats = {}
        for row in response.css("div#match_stats .statRow"):
            label = row.css(".statLabel::text").get()
            home_val = row.css(".homeStat::text").get()
            away_val = row.css(".awayStat::text").get()
            if label:
                stats[label.strip()] = {"home": home_val, "away": away_val}

        match_data.update(
            {
                "escalacoes": lineups,
                "estatisticas": stats,
            }
        )

        yield match_data
