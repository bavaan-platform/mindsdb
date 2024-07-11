import pandas as pd
from typing import Text, List, Dict
from vnstock3.explorer.fmarket.fund import Fund
from vnstock3.explorer.misc.gold_price import *
from vnstock3.explorer.misc.exchange_rate import vcb_exchange_rate
from datetime import datetime
from dateutil.relativedelta import relativedelta

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)


from mindsdb.utilities import log

logger = log.getLogger(__name__)


class StockTable(APITable):
    """The Shopify Products Table implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint
        self.today = datetime.today()

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, self.endpoint, self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )
        self.symbol = None
        self.source = "VCI"
        kwargs = dict()
        logger.info(f"this is where: {where_conditions}")
        where_conditions2 = where_conditions.copy()
        input_params = [
            "period",
            "lang",
            "dropna",
            "start",
            "end",
            "interval",
            "page_size",
            "last_time",
            "time",
            "price",
            "volume",
            "match_type",
            "symbols_list",
            "date",
        ]
        for i in where_conditions2:
            if i[1] == "symbol":
                self.symbol = i[2]
                where_conditions.remove(i)
            if i[1] == "data_source":
                self.source = i[2]
                where_conditions.remove(i)
            if i[1] in input_params:
                kwargs[f"{i[1]}"] = i[2]
                where_conditions.remove(i)

        logger.info(f"no way symbol={self.symbol}, source={self.source}")
        data_df = self.get_data(symbol=self.symbol, source=self.source, **kwargs)

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        return self.get_data(symbol="VCI", source="TCBS").columns.tolist()

    def get_data(self, symbol=None, source="VCI", **kwargs) -> List[Dict]:
        logger.info(f"kwammmmm: {kwargs}")
        connect = self.handler.connect()
        if 'list' in self.endpoint:
            data = connect.stock(source=source)
        data = connect.stock(symbol=symbol, source=source)
        logger.info(f'thisisL: {f"{data}.{self.endpoint}(**{kwargs})"}')
        result = eval(f"data.{self.endpoint}(**kwargs)")

        return result


class FmarketTable(APITable):
    """The Shopify Products Table implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, self.endpoint, self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        self.symbol = None
        logger.info(f"this is where: {where_conditions}")
        where_conditions2 = where_conditions.copy()
        for i in where_conditions2:
            if i[1] == "symbol":
                self.symbol = i[2]
                where_conditions.remove(i)

        data_df = self.get_data(symbol=self.symbol)

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        return self.get_data(symbol="SSISCA").columns.tolist()

    def get_data(self, symbol=None, **kwargs) -> List[Dict]:
        fund = Fund()
        result = eval(f"fund.{self.endpoint}(symbol)")
        return result


class FmarketListingTable(APITable):
    """The Shopify Products Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "fund_list", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )
        self.symbol = None
        self.source = None
        kwargs = dict()
        logger.info(f"this is where: {where_conditions}")
        where_conditions2 = where_conditions.copy()
        for i in where_conditions2:
            if i[1] == "fund_type":
                kwargs["fund_type"] = i[2]
                where_conditions.remove(i)

        data_df = self.get_data(**kwargs)

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        return self.get_data().columns.tolist()

    def get_data(self, **kwargs) -> List[Dict]:
        fund = Fund()
        result = fund.listing()
        return result


class GoldPriceTable(APITable):
    """The Shopify Products Table implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, self.endpoint, self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        data_df = self.get_data()

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        return self.get_data().columns.tolist()

    def get_data(self, **kwargs) -> List[Dict]:
        logger.info(f'"{self.endpoint}()"')
        result = eval(f"{self.endpoint}()")
        return result


class ExchangeRateTable(APITable):
    """The Shopify Products Table implementation"""

    def __init__(self, handler):
        super().__init__(handler)
        self.date = datetime.today().strftime("%Y-%m-%d")

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "exchange_rate", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )
        where_conditions2 = where_conditions.copy()
        date = self.date
        for i in where_conditions2:
            if i[1] == "date":
                date = i[2]
                where_conditions.remove(i)

        data_df = self.get_data(date)

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        return self.get_data(self.date).columns.tolist()

    def get_data(self, date, **kwargs) -> List[Dict]:
        result = vcb_exchange_rate(date)
        return result


class QuoteTable(APITable):
    """The Shopify Products Table implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint
        self.today = datetime.today()

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, self.endpoint, self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )
        self.symbol = None
        self.source = None
        start = None
        end = None
        kwargs = dict()
        logger.info(f"this is where: {where_conditions}")
        where_conditions2 = where_conditions.copy()
        input_params = [
            "start",
            "end",
            "interval",
            "page_size",
            "last_time",
        ]
        for i in where_conditions2:
            if i[1] == "symbol":
                self.symbol = i[2]
                where_conditions.remove(i)
            if i[1] == "data_source":
                self.source = i[2]
                where_conditions.remove(i)
            if i[1] == "date_start":
                start = i[2]
                where_conditions.remove(i)
            if i[1] == "date_end":
                end = i[2]
                where_conditions.remove(i)
            if i[1] in input_params:
                kwargs[f"{i[1]}"] = i[2]
                where_conditions.remove(i)

        logger.info(f"no way symbol={self.symbol}, source={self.source}")
        data_df = self.get_data(
            symbol=self.symbol, source=self.source, start_date=start, end_date=end, **kwargs
        )

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        return ['time', 'open', 'high', 'low', 'close', 'volume']

    def get_data(
        self, symbol=None, source="TCBS", start_date=None, end_date=None, **kwargs
    ) -> List[Dict]:
        logger.info(f"kwammmmm: {start_date}{end_date}")
        connect = self.handler.connect()
        data = connect.stock(symbol=symbol, source=source)
        if 'crypto' in self.endpoint:
            data = connect.crypto(symbol=symbol, source=source)
            self.endpoint.replace('.crypto', '')
        if 'fx' in  self.endpoint:
            data = connect.fx(symbol=symbol, source=source)
            self.endpoint.replace('.fx', '')
        if 'world_index' in  self.endpoint:
            data = connect.world_index(symbol=symbol, source=source)
            self.endpoint.replace('.world_index', '')
        logger.info(f'thisisL: {f"{data}.{self.endpoint}(**{kwargs})"}')
        result = eval(f"data.{self.endpoint}(start=start_date, end=end_date, **kwargs)")

        return result
