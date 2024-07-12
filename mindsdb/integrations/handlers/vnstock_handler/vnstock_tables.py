from datetime import datetime
from typing import Dict, List, Text

import pandas as pd
from mindsdb_sql.parser import ast
from vnstock3.explorer.fmarket.fund import Fund
from vnstock3.explorer.misc.exchange_rate import vcb_exchange_rate
from vnstock3.explorer.misc.gold_price import *

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryExecutor,
    SELECTQueryParser,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class StockTable(APITable):
    """The VNstock stock Tables implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint.replace("stock.", "")
        self.today = datetime.today()

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the VNstock "GET /resource" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            vnstock Products matching the query

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
        where_conditions_temp = where_conditions.copy()
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
        for i in where_conditions_temp:
            if i[0] == "=":
                if i[1] == "symbol":
                    self.symbol = i[2]
                    where_conditions.remove(i)
                if i[1] == "data_source":
                    self.source = i[2]
                    where_conditions.remove(i)
                if i[1] in input_params:
                    kwargs[f"{i[1]}"] = i[2]
                    where_conditions.remove(i)

        data_df = self.get_data(symbol=self.symbol, source=self.source, **kwargs)

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        source="TCBS"
        if 'ratio' in self.endpoint:
            source="VCI"
        return self.get_data(symbol="VCI", source=source).columns.tolist()

    def get_data(self, symbol=None, source="VCI", **kwargs) -> List[Dict]:
        connect = self.handler.connect()
        if "list" in self.endpoint:
            data = connect.stock(source=source)
        data = connect.stock(symbol=symbol, source=source)
        result = eval(f"data.{self.endpoint}(**kwargs)")
        # if 'ratio' in self.endpoint:
        #     old_columns = result.columns.tolist()
        #     new_columns = [attribute[1] for attribute in result.columns.tolist()]
        #     for i in range(len(new_columns)):
        #         result.rename(columns = {old_columns[i]:new_columns[i]}, inplace = True)
        return result


class FmarketTable(APITable):
    """The vnstock Fund Table implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint.replace("fund.", "")

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the VNStock "GET /fund resource" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            VNStock resource matching the query

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
        where_conditions_temp = where_conditions.copy()
        for i in where_conditions_temp:
            if i[1] == "symbol" and i[0] == "=":
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
    """The vnstock fund market list Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the VNStock "GET /FmarketList" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            VNStock FmarketList matching the query

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
        where_conditions_temp = where_conditions.copy()
        for i in where_conditions_temp:
            if i[1] == "fund_type" and i[0] == "=":
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
    """The VNStock Gold price Table implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the VNStock "GET /gold price" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            VNStock gold price matching the query

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
        result = eval(f"{self.endpoint}()")
        return result


class ExchangeRateTable(APITable):
    """The VNStock exchange rate Table implementation"""

    def __init__(self, handler):
        super().__init__(handler)
        self.date = datetime.today().strftime("%Y-%m-%d")

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the VNStock "GET /exchange rate" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            VNStock exchange rate matching the query

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
        where_conditions_temp = where_conditions.copy()
        date = self.date
        for i in where_conditions_temp:
            if i[1] == "date" and i[0] == "=":
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
    """The VNStock quote resources Table implementation"""

    def __init__(self, handler, endpoint):
        super().__init__(handler)
        self.endpoint = endpoint.replace("stock.", "")
        self.today = datetime.today()

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the VNStock "GET /quote resources" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            VNStock quote resources matching the query

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
        where_conditions_temp = where_conditions.copy()
        input_params = [
            "start",
            "end",
            "interval",
            "page_size",
            "last_time",
        ]
        for i in where_conditions_temp:
            if i[0] == "=":
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

        data_df = self.get_data(
            symbol=self.symbol,
            source=self.source,
            start_date=start,
            end_date=end,
            **kwargs,
        )

        select_statement_executor = SELECTQueryExecutor(
            data_df, selected_columns, where_conditions, order_by_conditions
        )

        data_df = select_statement_executor.execute_query()

        return data_df

    def get_columns(self) -> List[Text]:
        return ["time", "open", "high", "low", "close", "volume"]

    def get_data(
        self, symbol=None, source="TCBS", start_date=None, end_date=None, **kwargs
    ) -> List[Dict]:
        connect = self.handler.connect()
        data = connect.stock(symbol=symbol, source=source)
        if "crypto" in self.endpoint:
            data = connect.crypto(symbol=symbol, source=source)
            self.endpoint.replace(".crypto", "")
        if "fx" in self.endpoint:
            data = connect.fx(symbol=symbol, source=source)
            self.endpoint.replace(".fx", "")
        if "world_index" in self.endpoint:
            data = connect.world_index(symbol=symbol, source=source)
            self.endpoint.replace(".world_index", "")
        result = eval(f"data.{self.endpoint}(start=start_date, end=end_date, **kwargs)")

        return result
