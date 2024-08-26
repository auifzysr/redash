import logging
from flask import request
from redash.handlers.base import BaseResource
from redash.permissions import (
    require_any_of_permission,
)

logger = logging.getLogger(__name__)

# TODO: copy redash/handlers/query_results.py's run_query to simulate query execution
# data_source contains type, which should hold "bigquery", can be used to be effective
# only with bigquery data source

class QueryBytesProcessedResource(BaseResource):
    @require_any_of_permission(("view_query", "execute_query"))
    def get(self, query_id):
        logger.info(f"called dry run get with query_id:%s" % (query_id))