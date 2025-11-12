# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import logging
import pymysql


class MySQLPipeline:
    """
    Pipeline para salvar partidas na tabela `match_team` em MySQL.
    Adicione `ogol_spider.pipelines.MySQLPipeline` em ITEM_PIPELINES nas settings.
    """

    def open_spider(self, spider):
        settings = spider.crawler.settings
        self.host = settings.get("MYSQL_HOST", "localhost")
        self.user = settings.get("MYSQL_USER", "root")
        self.password = settings.get("MYSQL_PASSWORD", "")
        self.db = settings.get("MYSQL_DB", "ogol")
        self.port = settings.get("MYSQL_PORT", 3306)

        try:
            self.conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db,
                port=self.port,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
            self.cur = self.conn.cursor()
            # Cria tabela se não existir (colunas básicas; ajustáveis conforme necessidade)
            create_sql = """
            CREATE TABLE IF NOT EXISTS match_team (
                id INT AUTO_INCREMENT PRIMARY KEY,
                team VARCHAR(255),
                year INT,
                `date` VARCHAR(100),
                competition VARCHAR(255),
                opponent VARCHAR(255),
                location VARCHAR(255),
                `result` VARCHAR(255),
                score VARCHAR(50),
                lineups LONGTEXT,
                stats LONGTEXT,
                captcha TINYINT(1) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """
            self.cur.execute(create_sql)
        except Exception as e:
            logging.getLogger(__name__).exception("Erro ao conectar no MySQL: %s", e)
            self.conn = None
            self.cur = None

    def close_spider(self, spider):
        if getattr(self, "cur", None):
            self.cur.close()
        if getattr(self, "conn", None):
            self.conn.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if not getattr(self, "conn", None) or not getattr(self, "cur", None):
            return item  # não falhar o spider; apenas retorna o item

        team = adapter.get("time")
        year = adapter.get("ano")
        date = adapter.get("data")
        competition = adapter.get("competicao")
        opponent = adapter.get("adversario")
        location = adapter.get("local")
        result = adapter.get("resultado")
        score = adapter.get("placar")
        lineups = json.dumps(adapter.get("escalacoes", {}), ensure_ascii=False)
        stats = json.dumps(adapter.get("estatisticas", {}), ensure_ascii=False)
        captcha = 1 if adapter.get("captcha") else 0

        insert_sql = """
        INSERT INTO match_team
        (team, year, `date`, competition, opponent, location, `result`, score, lineups, stats, captcha)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.cur.execute(
                insert_sql,
                (
                    team,
                    year,
                    date,
                    competition,
                    opponent,
                    location,
                    result,
                    score,
                    lineups,
                    stats,
                    captcha,
                ),
            )
        except Exception as e:
            logging.getLogger(__name__).exception("Erro ao inserir item no MySQL: %s", e)

        return item

