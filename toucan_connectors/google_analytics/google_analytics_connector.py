from enum import Enum
from typing import List

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pydantic import BaseModel

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import GoogleCredentials

API = 'analyticsreporting'
SCOPE = 'https://www.googleapis.com/auth/analytics.readonly'
VERSION = 'v4'


class Sampling(str, Enum):
    SAMPLING_UNSPECIFIED = 'SAMPLING_UNSPECIFIED'
    DEFAULT = 'DEFAULT'
    SMALL = 'SMALL'
    LARGE = 'LARGE'


class MetricType(str, Enum):
    METRIC_TYPE_UNSPECIFIED = 'METRIC_TYPE_UNSPECIFIED'
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    CURRENCY = 'CURRENCY'
    PERCENT = 'PERCENT'
    TIME = 'TIME'


class OrderType(str, Enum):
    ORDER_TYPE_UNSPECIFIED = 'ORDER_TYPE_UNSPECIFIED'
    VALUE = 'VALUE'
    DELTA = 'DELTA'
    SMART = 'SMART'
    HISTOGRAM_BUCKET = 'HISTOGRAM_BUCKET'
    DIMENSION_AS_INTEGER = 'DIMENSION_AS_INTEGER'


class SortOrder(str, Enum):
    SORT_ORDER_UNSPECIFIED = 'SORT_ORDER_UNSPECIFIED'
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'


class FilterLogicalOperator(str, Enum):
    OPERATOR_UNSPECIFIED = 'OPERATOR_UNSPECIFIED'
    OR = 'OR'
    AND = 'AND'


class Operator(str, Enum):
    OPERATOR_UNSPECIFIED = 'OPERATOR_UNSPECIFIED'
    REGEXP = 'REGEXP'
    BEGINS_WITH = 'BEGINS_WITH'
    ENDS_WITH = 'ENDS_WITH'
    PARTIAL = 'PARTIAL'
    EXACT = 'EXACT'
    NUMERIC_EQUAL = 'NUMERIC_EQUAL'
    NUMERIC_GREATER_THAN = 'NUMERIC_GREATER_THAN'
    NUMERIC_LESS_THAN = 'NUMERIC_LESS_THAN'
    IN_LIST = 'IN_LIST'


class Type(str, Enum):
    UNSPECIFIED_COHORT_TYPE = "UNSPECIFIED_COHORT_TYPE"
    FIRST_VISIT_DATE = "FIRST_VISIT_DATE"


class Dimension(BaseModel):
    name: str
    histogramBuckets: List[str] = None


class DimensionFilter(BaseModel):
    dimensionName: str
    operator: Operator
    expressions: List[str] = None
    caseSensitive: bool = False

    class Config:
        # TODO `not` param is not implemented
        allow_extra = True


class DimensionFilterClause(BaseModel):
    operator: FilterLogicalOperator
    filters: List[DimensionFilter]


class DateRange(BaseModel):
    startDate: str
    endDate: str


class Metric(BaseModel):
    expression: str
    alias: str = None

    class Config:
        # TODO `metricType` param is not implemented
        allow_extra = True


class MetricFilter(BaseModel):
    metricName: str
    operator: Operator
    comparisonValue: str

    class Config:
        # TODO `not` param is not implemented
        allow_extra = True


class MetricFilterClause(BaseModel):
    operator: FilterLogicalOperator
    filters: List[MetricFilter]


class OrderBy(BaseModel):
    fieldName: str
    orderType: OrderType = None
    sortOrder: SortOrder = None


class Pivot(BaseModel):
    dimensions: List[Dimension] = None
    dimensionFilterClauses: List[DimensionFilterClause] = None
    metrics: List[Metric] = None
    startGroup: int = None
    maxGroupCount: int = None


class Cohort(BaseModel):
    name: str
    type: Type
    dateRage: DateRange = None


class CohortGroup(BaseModel):
    cohorts: List[Cohort]
    lifetimeValue: bool = False


class ReportRequest(BaseModel):
    viewId: str
    dateRanges: List[DateRange] = None
    samplingLevel: Sampling = None
    dimensions: List[Dimension] = None
    dimensionFilterClauses: List[DimensionFilterClause] = None
    metrics: List[Metric] = None
    metricFilterClauses: List[MetricFilterClause] = None
    filtersExpression: str = ''
    orderBys: List[OrderBy] = []
    # TODO    segment: List[Segment]
    pivots: List[Pivot] = None
    cohortGroup: CohortGroup = None
    pageToken: str = ''
    pageSize: int = 10000
    includeEmptyRows: bool = False
    hideTotals: bool = False
    hideValueRanges: bool = False


def get_dict_from_response(report, request_date_ranges):

    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    all_rows = []
    for row_index, row in enumerate(rows):
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        for i, values in enumerate(dateRangeValues):
            for metricHeader, value in zip(metricHeaders, values.get('values')):
                row_dict = {
                    'row_index': row_index,
                    'date_range_id': i,
                    'metric_name': metricHeader.get('name'),
                }

                if request_date_ranges and (len(request_date_ranges) >= i):
                    row_dict['start_date'] = request_date_ranges[i].startDate
                    row_dict['end_date'] = request_date_ranges[i].endDate

                if metricHeader.get('type') == 'INTEGER':
                    row_dict['metric_value'] = int(value)
                elif metricHeader.get('type') == 'FLOAT':
                    row_dict['metric_value'] = float(value)
                else:
                    row_dict['metric_value'] = value

                for dimension_name, dimension_value in zip(dimensionHeaders, dimensions):
                    row_dict[dimension_name] = dimension_value

                all_rows.append(row_dict)

    return all_rows


def get_query_results(service, report_request):
    response = service.reports().batchGet(
        body={'reportRequests': report_request.dict()}).execute()
    return response.get('reports', [])[0]


class GoogleAnalyticsDataSource(ToucanDataSource):
    report_request: ReportRequest


class GoogleAnalyticsConnector(ToucanConnector):
    type = "GoogleAnalytics"
    data_source_model: GoogleAnalyticsDataSource

    credentials: GoogleCredentials
    scope: List[str] = [SCOPE]

    def get_df(self, data_source: GoogleAnalyticsDataSource) -> pd.DataFrame:
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            self.credentials.dict(), self.scope
        )
        service = build(API, VERSION, credentials=credentials)
        report_request = data_source.report_request

        report = get_query_results(service, report_request)
        reports_data = [pd.DataFrame(get_dict_from_response(report, report_request.dateRanges))]

        while 'nextPageToken' in report:
            report_request.pageToken = report['nextPageToken']

            report = get_query_results(service, report_request)
            reports_data.append(pd.DataFrame(
                get_dict_from_response(report, report_request.dateRanges)))

        return pd.concat(reports_data)