import os
from mindsdb.integrations.handlers.vnstock_handler.vnstock_tables import (
    StockTable,
    FmarketListingTable,
    FmarketTable,
    ExchangeRateTable,
    QuoteTable,
    GoldPriceTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler_exceptions import (
    MissingConnectionParams,
    InvalidNativeQuery,
)
from mindsdb_sql import parse_sql

os.environ["ACCEPT_TC"] = "tôi đồng ý"

from vnstock3 import Vnstock

logger = log.getLogger(__name__)


class VNStockHandler(APIHandler):
    """
    The vnstock handler implementation.
    """

    name = "vnstock"

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        if kwargs.get("connection_data") is None:
            raise MissingConnectionParams(
                "Incomplete parameters passed to vnstock Handler"
            )

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        
        listing_all_symbols_data = StockTable(self, "listing.all_symbols")
        self._register_table("list_all_symbols", listing_all_symbols_data)
        
        listing_all_symbols_by_exchange_data = StockTable(self, "listing.symbols_by_exchange")
        self._register_table("list_all_symbols_by_exchange", listing_all_symbols_by_exchange_data)
        
        # listing_all_symbols_by_group_data = StockTable(self, "listing.symbols_by_group")
        # self._register_table("list_all_symbols_by_group", listing_all_symbols_by_group_data)
        
        listing_all_symbols_by_industries_data = StockTable(self, "listing.symbols_by_industries")
        self._register_table("listing_all_symbols_by_industries", listing_all_symbols_by_industries_data)
        
        company_overview_data = StockTable(self, "company.overview")
        self._register_table("company_overview", company_overview_data)

        company_profile_data = StockTable(self, "company.profile")
        self._register_table("company_profile", company_profile_data)

        company_shareholders_data = StockTable(self, "company.shareholders")
        self._register_table("company_shareholders", company_shareholders_data)

        company_officers_data = StockTable(self, "company.officers")
        self._register_table("company_officers", company_officers_data)

        company_subsidiaries_data = StockTable(self, "company.subsidiaries")
        self._register_table("company_subsidiaries", company_subsidiaries_data)

        company_dividends_data = StockTable(self, "company.dividends")
        self._register_table("company_dividends", company_dividends_data)

        company_insider_deals_data = StockTable(self, "company.insider_deals")
        self._register_table("company_insider_deals", company_insider_deals_data)

        company_events_data = StockTable(self, "company.events")
        self._register_table("company_events", company_events_data)

        company_news_data = StockTable(self, "company.news")
        self._register_table("company_news", company_news_data)

        finance_income_statement_data = StockTable(self, "finance.income_statement")
        self._register_table("finance_income_statement", finance_income_statement_data)

        finance_balance_sheet_data = StockTable(self, "finance.balance_sheet")
        self._register_table("finance_balance_sheet", finance_balance_sheet_data)

        finance_cash_flow_data = StockTable(self, "finance.cash_flow")
        self._register_table("finance_cash_flow", finance_cash_flow_data)

        finance_ratio_data = StockTable(self, 'finance.ratio')
        self._register_table("finance_ratio", finance_ratio_data)

        quote_history_data = QuoteTable(self, "quote.history")
        self._register_table("quote_history", quote_history_data)

        quote_intraday_data = StockTable(self, 'quote.intraday')
        self._register_table("quote_intraday", quote_intraday_data)

        # fx_quote_history_data = QuoteTable(self, 'fx.quote.history')
        # self._register_table("fx_quote_history", fx_quote_history_data)

        # crypto_quote_history_data = QuoteTable(self, 'crypto.quote.history')
        # self._register_table("crypto_quote_history", crypto_quote_history_data)
        
        # world_index_history_data = QuoteTable(self, 'world_index.quote.history')
        # self._register_table("world_index_history", world_index_history_data)
        
        fund_list_data = FmarketListingTable(self)
        self._register_table("fund_list", fund_list_data)
        
        fund_filter_data = FmarketTable(self, "filter")
        self._register_table("fund_filter", fund_filter_data)
        
        fund_details_nav_report_data = FmarketTable(self, "details.nav_report")
        self._register_table("fund_details_nav_report", fund_details_nav_report_data)
        
        fund_details_top_holding_data = FmarketTable(self, "details.top_holding")
        self._register_table("fund_details_top_holding", fund_details_top_holding_data)
        
        fund_details_industry_holding_data = FmarketTable(self, "details.industry_holding")
        self._register_table("fund_details_industry_holding", fund_details_industry_holding_data)
        
        fund_details_asset_holding_data = FmarketTable(self, "details.asset_holding")
        self._register_table("fund_details_asset_holding", fund_details_asset_holding_data)
        
        sjc_gold_price_data = GoldPriceTable(self, "sjc_gold_price")
        self._register_table("sjc_gold_price", sjc_gold_price_data)
        
        btmc_gold_price_data = GoldPriceTable(self, "btmc_goldprice")
        self._register_table("btmc_gold_price", btmc_gold_price_data)
        
        exchange_rate_data = ExchangeRateTable(self)
        self._register_table("exchange_rate", exchange_rate_data)
    def connect(self):
        connect = Vnstock()
        return connect

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(True)
        response.success = True
        self.is_connected = response.success

        return response


def native_query(self, query: str) -> StatusResponse:
    """Receive and process a raw query.
    Parameters
    ----------
    query : str
        query in a native format
    Returns
    -------
    StatusResponse
        Request status
    """
    try:
        ast = parse_sql(query, dialect="mindsdb")
    except Exception as e:
        raise InvalidNativeQuery(f"The query {query} is invalid.")
    return self.query(ast)
