# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

class OgolSQLPipeline:
    def __init__(self, database_url):
        self.database_url = database_url

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            database_url=crawler.settings.get("DATABASE_URL")
        )

    def open_spider(self, spider):
        self.engine = create_engine(self.database_url)
        self.connection = self.engine.connect()
        self.metadata = MetaData()

        self.games = Table(
            "jogos",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("time", String(100)),
            Column("ano", Integer),
            Column("data", String(50)),
            Column("hora", String(20)),
            Column("casa/fora", String(255)),
            Column("resultado", String(20)),
        )
        self.metadata.create_all(self.engine)

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        ins = self.games.insert().values(**item)
        self.connection.execute(ins)
        return item