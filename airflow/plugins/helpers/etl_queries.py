class EtlQueries:
    dim_agency_etl = """select staging.agency_code,
                               staging.agency,
                               staging.branch_code,
                               staging.branch,
                               staging.department_code,
                               staging.department,
                               staging.highest_department_code,
                               staging.highest_department,
                               staging.start_date,
                               staging.end_date
                          from staging
                    full outer join dimension
                            on staging.branch_code = dimension.branch_code
                           and dimension.branch_code is null"""
