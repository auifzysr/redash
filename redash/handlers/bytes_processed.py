import logging

import unicodedata
from urllib.parse import quote

import regex
from flask import make_response, request
from flask_login import current_user
from flask_restful import abort

from redash import models, settings
from redash.handlers.base import BaseResource, get_object_or_404, record_event
from redash.models.parameterized_query import (
    InvalidParameterError,
    ParameterizedQuery,
    QueryDetachedFromDataSourceError,
    dropdown_values,
)
from redash.permissions import (
    has_access,
    not_view_only,
    require_access,
    require_any_of_permission,
    require_permission,
    view_only,
)
from redash.serializers import (
    serialize_job,
    serialize_query_result,
    serialize_query_result_to_dsv,
    serialize_query_result_to_xlsx,
)
from redash.tasks import Job
from redash.tasks.queries import enqueue_query
from redash.utils import (
    collect_parameters_from_request,
    json_dumps,
    to_filename,
)

logger = logging.getLogger(__name__)

def error_response(message, http_status=400):
    return {"job": {"status": 4, "error": message}}, http_status

error_messages = {
    "unsafe_when_shared": error_response(
        "This query contains potentially unsafe parameters and cannot be executed on a shared dashboard or an embedded visualization.",
        403,
    ),
    "unsafe_on_view_only": error_response(
        "This query contains potentially unsafe parameters and cannot be executed with read-only access to this data source.",
        403,
    ),
    "no_permission": error_response("You do not have permission to run queries with this data source.", 403),
    "select_data_source": error_response("Please select data source to run this query.", 401),
    "no_data_source": error_response("Target data source not available.", 401),
}

# TODO: copy redash/handlers/query_results.py's run_query to simulate query execution
# data_source contains type, which should hold "bigquery", can be used to be effective
# only with bigquery data source

def dry_run_query(query, parameters, data_source, query_id, should_apply_auto_limit, max_age=0):
    if not data_source:
        return error_messages["no_data_source"]

    if data_source.paused:
        if data_source.pause_reason:
            message = "{} is paused ({}). Please try later.".format(data_source.name, data_source.pause_reason)
        else:
            message = "{} is paused. Please try later.".format(data_source.name)

        return error_response(message)

    try:
        query.apply(parameters)
    except (InvalidParameterError, QueryDetachedFromDataSourceError) as e:
        abort(400, message=str(e))

    query_text = data_source.query_runner.apply_auto_limit(query.text, should_apply_auto_limit)

    if query.missing_params:
        return error_response("Missing parameter value for: {}".format(", ".join(query.missing_params)))

    if max_age == 0:
        query_result = None
    else:
        query_result = models.QueryResult.get_latest(data_source, query_text, max_age)

    record_event(
        current_user.org,
        current_user,
        {
            "action": "dry_run_query",
            "cache": "hit" if query_result else "miss",
            "object_id": data_source.id,
            "object_type": "data_source",
            "query": query_text,
            "query_id": query_id,
            "parameters": parameters,
        },
    )

    if query_result:
        return {"query_result": serialize_query_result(query_result, current_user.is_api_user())}
    else:
        job = enqueue_query(
            query_text,
            data_source,
            current_user.id,
            current_user.is_api_user(),
            metadata={
                "Username": current_user.get_actual_user(),
                "query_id": query_id,
                "dry_run": True,
            },
        )
        return serialize_job(job)


class QueryBytesProcessedResource(BaseResource):
    @require_any_of_permission(("view_query", "execute_query"))
    def post(self, query_id):
        params = request.get_json(force=True)

        logger.info(f"called dry run get with params" % (params))

        params = request.get_json(force=True, silent=True) or {}
        parameter_values = params.get("parameters", {})

        max_age = params.get("max_age", -1)
        # max_age might have the value of None, in which case calling int(None) will fail
        if max_age is None:
            max_age = -1
        max_age = int(max_age)

        query = get_object_or_404(models.Query.get_by_id_and_org, query_id, self.current_org)

        allow_executing_with_view_only_permissions = query.parameterized.is_safe
        if "apply_auto_limit" in params:
            should_apply_auto_limit = params.get("apply_auto_limit", False)
        else:
            should_apply_auto_limit = query.options.get("apply_auto_limit", False)

        if has_access(query, self.current_user, allow_executing_with_view_only_permissions):
            return dry_run_query(
                query.parameterized,
                parameter_values,
                query.data_source,
                query_id,
                should_apply_auto_limit,
                max_age,
            )
        else:
            if not query.parameterized.is_safe:
                if current_user.is_api_user():
                    return error_messages["unsafe_when_shared"]
                else:
                    return error_messages["unsafe_on_view_only"]
            else:
                return error_messages["no_permission"]