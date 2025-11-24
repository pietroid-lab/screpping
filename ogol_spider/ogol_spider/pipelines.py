from itemadapter import ItemAdapter
import logging
import pyodbc


class SQLServerPipeline:

    def open_spider(self, spider):
        try:
            self.conn = pyodbc.connect(
                "DRIVER={ODBC Driver 18 for SQL Server};"
                "SERVER=www.thyagoquintas.com.br;"
                "DATABASE=OGOL;"
                "UID=ogol;"
                "PWD=ogolsenha;"
            )

            self.conn.autocommit = False
            self.cur = self.conn.cursor()
            self.batch_size = 10
            self.counter = 0

            spider.logger.info("✅ Conectado ao SQL Server com sucesso.")

        except Exception as e:
            logging.exception("❌ Erro ao conectar no SQL Server: %s", e)
            self.conn = None
            self.cur = None

    def close_spider(self, spider):
        if self.counter > 0:
            self.conn.commit()
            spider.logger.info("✅ Commit final executado.")

        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def get_or_create(self, table, field, value):
        self.cur.execute(f"SELECT id FROM {table} WHERE {field} = ?", value)
        row = self.cur.fetchone()

        if row:
            return row[0]

        self.cur.execute(f"INSERT INTO {table} ({field}) VALUES (?)", value)
        self.cur.execute("SELECT SCOPE_IDENTITY()")
        return self.cur.fetchone()[0]

    def process_item(self, item, spider):
        if not self.conn or not self.cur:
            return item

        adapter = ItemAdapter(item)

        time_casa = adapter.get("time")
        time_fora = adapter.get("adversario")

        time_casa_id = self.get_or_create("times", "nome", time_casa)
        time_fora_id = self.get_or_create("times", "nome", time_fora)

        self.cur.execute("""
            INSERT INTO partidas
            (ano, data_partida, competicao, local, placar, resultado, time_casa_id, time_fora_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("ano"),
            adapter.get("data"),
            adapter.get("competicao"),
            adapter.get("local"),
            adapter.get("placar"),
            adapter.get("resultado"),
            time_casa_id,
            time_fora_id
        ))

        self.cur.execute("SELECT SCOPE_IDENTITY()")
        partida_id = self.cur.fetchone()[0]

        escalacoes = adapter.get("escalacoes", {})

        for lado in ["home", "away"]:
            time_id = time_casa_id if lado == "home" else time_fora_id

            for jogador in escalacoes.get(lado, []):
                jogador_nome = jogador.get("nome")
                eventos = jogador.get("eventos", [])

                jogador_id = self.get_or_create("jogadores", "nome", jogador_nome)

                self.cur.execute("""
                    INSERT INTO escalacoes (partida_id, jogador_id, time_id, lado)
                    VALUES (?, ?, ?, ?)
                """, (partida_id, jogador_id, time_id, lado))

                self.cur.execute("SELECT SCOPE_IDENTITY()")
                escalacao_id = self.cur.fetchone()[0]

                for evento in eventos:
                    self.cur.execute("""
                        INSERT INTO eventos (escalacao_id, tipo_evento)
                        VALUES (?, ?)
                    """, (escalacao_id, evento))

        self.counter += 1

        if self.counter % self.batch_size == 0:
            self.conn.commit()
            spider.logger.info(f"✅ Commit automático após {self.batch_size} registros.")

        return item
