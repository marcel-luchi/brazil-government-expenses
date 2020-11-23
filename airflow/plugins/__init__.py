from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

class TransparenciaPlugin(AirflowPlugin):
    name = 'transparencia_plugin'

    operators = [
        operators.ExpensesDimensionRawToStage,
        operators.StageToDimension,
        operators.VouchersDimensionRawToStage,
        operators.ExpensesFactFromRaw,
        operators.VouchersFactFromRaw,
        operators.DataQualityOperator,
        operators.TransparenciaApiReaderOperator
    ]

    helpers = [
        helpers.Schemas,
        helpers.SparkUtils
    ]
