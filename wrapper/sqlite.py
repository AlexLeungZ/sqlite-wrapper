import csv
import sqlite3
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from sqlite3 import Connection, Row
from typing import Any, Literal, Self, TypeAlias

From: TypeAlias = str | list[str]
Select: TypeAlias = list[str] | None
Where: TypeAlias = dict[str, str] | None
Pair: TypeAlias = Mapping[str, Any]
Query: TypeAlias = list[Row]

OrderVal: TypeAlias = Literal["ASC", "DESC", "ASC NULLS LAST", "DESC NULLS FIRST"]
Order: TypeAlias = dict[str, OrderVal] | None


@dataclass(frozen=True)
# Wrapper class for sqlite3.Connection
class Handler:
    database: str | Path
    schema: str | Path
    backups: int = 3
    threshold: int = 10

    # SQLite Database schema runner
    def __post_init__(self: Self) -> None:
        with closing(sqlite3.connect(self.__database)) as conn:
            with Path(self.__schema).open() as schema:
                with conn:
                    conn.executescript(schema.read())

    @cached_property
    def __database(self: Self) -> Path:
        return Path(self.database)

    @cached_property
    def __schema(self: Self) -> Path:
        return Path(self.schema)

    @cached_property
    def __backups(self: Self) -> int:
        return abs(self.backups)

    # SQLite Connection common pragma
    def _conn_run(self: Self, conn: Connection) -> None:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")

    # SQLite Handler SQL statements executer
    def _execute(self: Self, sqls: str | list[str]) -> None:
        if not isinstance(sqls, list):
            sqls = [sqls]

        with closing(sqlite3.connect(self.__database)) as conn:
            with conn:
                self._conn_run(conn)
                with closing(conn.cursor()) as cur:
                    for sql in sqls:
                        cur.execute(sql)

    # SQLite Handler SQL data fetcher
    def _fetch(self: Self, sql: str, fetch: int) -> Query:
        with closing(sqlite3.connect(self.__database)) as conn:
            conn.row_factory = Row
            with conn:
                self._conn_run(conn)
            with closing(conn.cursor()) as cur:
                cur.execute(sql)
                return cur.fetchmany(fetch or -1)

    # SQLite Handler SQL data exporter
    def _export(self: Self, sql: str, path: Path) -> None:
        with closing(sqlite3.connect(self.__database)) as conn:
            conn.row_factory = Row
            with conn:
                self._conn_run(conn)
            with closing(conn.cursor()) as cur:
                cur.execute(sql)
                cols = (col[0] for col in cur.description)
                with path.open("w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow(cols)
                    writer.writerows(cur)

    # SQLite Database backup creator
    def backup_create(self: Self) -> None:
        bck_time = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        bck_path = self.__database.parent / f"{self.__database.stem}_{bck_time}{self.__database.suffix}.bak"

        backups = sorted(self.backup_list())
        if len(backups) >= self.__backups:
            backups[0].unlink()

        with closing(sqlite3.connect(self.__database)) as cur:
            with closing(sqlite3.connect(bck_path)) as bck:
                with cur, bck:
                    cur.backup(bck)

    # SQLite Database backup file checker
    def backup_check(self: Self, backup: Path) -> Path:
        if not backup.is_file():
            raise FileNotFoundError
        if backup.stat().st_mode != self.__database.stat().st_mode:
            raise PermissionError
        return backup

    # SQLite Database backup file list getter
    def backup_list(self: Self) -> Iterator[Path]:
        return self.__database.parent.glob(f"{self.__database.stem}_*{self.__database.suffix}.bak")

    # SQLite Database backup file setter
    def backup_set(self: Self, backup: Path) -> None:
        path = self.backup_check(backup)
        path.replace(self.__database)

    # SQLite Database backup file deleter
    def backup_pop(self: Self, backup: Path) -> None:
        path = self.backup_check(backup)
        path.unlink()

    # SQLite Handler internal select statements generator
    def _sql_select(self: Self, select: Select) -> str:
        return ", ".join(select) if select else "*"

    # SQLite Handler internal join statements generator
    def _sql_join(self: Self, tables: From) -> str:
        return " NATURAL JOIN ".join(tables) if isinstance(tables, list) else tables

    # SQLite Handler internal where statements generator
    def _sql_where(self: Self, where: Where) -> str:
        return f"WHERE {' AND '.join([f'{key} {where[key]}' for key in where])}" if where else ""

    # SQLite Handler internal order statements generator
    def _sql_order(self: Self, order: Order) -> str:
        return f"ORDER BY {','.join([f'{key} {order[key]}' for key in order])}" if order else ""

    # SQLite Handler internal ignore statements generator
    def _sql_ignore(self: Self, ignore: bool) -> str:
        return "OR IGNORE " if ignore else ""

    def _row_insert(self: Self, data: Pair, ignore: bool, table: str) -> str:
        return f"INSERT {self._sql_ignore(ignore)}INTO {table} {tuple(data.keys())} VALUES {tuple(data.values())};"

    # SQLite Handler insert rows
    def row_insert(self: Self, table: str, data: Pair | Sequence[Pair], ignore: bool = False) -> None:
        if not table:
            return
        if not isinstance(data, Sequence):
            data = [data]
        if len(data) > self.threshold:
            self.backup_create()

        sqls = [self._row_insert(pair, ignore, table) for pair in data if pair]
        self._execute(sqls)

    # SQLite Handler update rows by conditions
    def row_update(self: Self, table: str, data: Pair, where: Where) -> None:
        if not (table and data):
            return

        sql = f"UPDATE {table} SET {tuple(data.keys())} = {tuple(data.values())} {self._sql_where(where)};"
        self._execute(sql)

    # SQLite Handler delete rows by conditions
    def row_delete(self: Self, table: str, where: Where) -> None:
        if not (table):
            return

        sql = f"DELETE FROM {table} {self._sql_where(where)};"
        self._execute(sql)

    def _table_drop(self: Self, table: str) -> str:
        return f"DROP TABLE IF EXISTS {table};"

    # SQLite Handler drop tables by table names
    def table_drop(self: Self, tables: From) -> None:
        if not isinstance(tables, list):
            tables = [tables]

        sqls = [self._table_drop(table) for table in tables if table]
        self._execute(sqls)

    def _col_default(self: Self, default: object | None) -> str:
        return f"DEFAULT {default}" if default else ""

    def col_create(self: Self, table: str, col: str, sql_type: str, default: object | None = None) -> None:
        if not (table and col and sql_type):
            return

        sql = f"ALTER TABLE {table} ADD COLUMN {col} {sql_type.upper()} {self._col_default(default)};"
        self._execute(sql)

    def _col_name(self: Self, rows: Iterable[Row]) -> list[str]:
        return [row["name"] for row in rows]

    def _col_info(self: Self, table: str) -> Query:
        sql = f"PRAGMA table_info({table});"
        return self._fetch(sql, 0)

    def col_name(self: Self, table: str) -> list[str] | None:
        if query := self._col_info(table):
            return self._col_name(query)

    def col_name_group(self: Self, table: str, mask: str) -> dict[Any, list[str]] | None:
        if query := self._col_info(table):
            return {idx: self._col_name(group) for idx, group in groupby(query, itemgetter(mask))}  # type: ignore

    # SQLite Handler internal select statements generator
    def sql_select(self: Self, tables: From, select: Select = None, where: Where = None, order: Order = None) -> str:
        return f"SELECT {self._sql_select(select)} FROM {self._sql_join(tables)} {self._sql_where(where)} {self._sql_order(order)};"

    # SQLite Handler all data
    def fetch_all(self: Self, table: str, fetch: int = 0) -> Query:
        sql = f"SELECT * FROM {table};"
        return self._fetch(sql, fetch)

    # SQLite Handler fetch data by conditions
    def fetch(
        self: Self, tables: From, select: Select = None, where: Where = None, order: Order = None, fetch: int = 0
    ) -> Query:
        sql = self.sql_select(tables, select, where, order)
        return self._fetch(sql, fetch)

    # SQLite Handler export data by conditions as csv file
    def export(
        self: Self, path: Path, tables: From, select: Select = None, where: Where = None, order: Order = None
    ) -> None:
        sql = self.sql_select(tables, select, where, order)
        return self._export(sql, path)

    # SQLite Handler get number of rows by conditions
    def count(self: Self, tables: From, where: Where = None) -> int:
        sql = f"SELECT COUNT(*) FROM {self._sql_join(tables)} {self._sql_where(where)};"
        query = self._fetch(sql, 0)
        return int(query[0][0]) if query else 0
