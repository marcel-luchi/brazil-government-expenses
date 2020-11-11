class TransparenciaPlugin(AirflowPlugin):
    name = 'transparencia_plugin'
    operators = [
        operators.TransparenciaApiReaderOperator,
        operators.TransparenciaLoadFactOperator,
        operators.TransparenciaLoadDimensionOperator,
        operators.DataQualityOperator,
        operators.TransparenciaRedshiftStageOperator
    ]
    helpers = [
        helpers.RedshiftQueries
    ]