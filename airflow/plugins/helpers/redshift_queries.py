class RedshiftQueries:
    copy_pagamentos = """copy stage_ft_pagamentos 
                         from 's3://brgovdata/analytic_tables/pagamentos/ano_extrato'
                         credentials 'aws_iam_role={arn_rule}'
                         format as parquet;"""

    copy_cartao = """copy stage_dm_cartao_funcionario
                     from 's3://brgovdata/analytic_tables/cartaofuncionario/'
                     credentials 'aws_iam_role={arn_rule}'
                     format as parquet;"""

    copy_estabelecimento = """copy stage_dm_estabelecimento
                              from 's3://brgovdata/analytic_tables/estabelecimento/'
                              credentials 'aws_iam_role={arn_rule}'
                              format as parquet;"""

    copy_unidade = """copy stage_dm_unidade_gestora
                      from 's3://brgovdata/analytic_tables/unidadegestora/'
                      credentials 'aws_iam_role={arn_rule}'
                      format as parquet;"""

    copy_municipio = """copy stage_dm_municipio
                        from 's3://brgovdata/analytic_tables/municipio/'
                        credentials 'aws_iam_role={arn_rule}'
                        format as parquet;"""

    insert_pagamentos = """insert
                             into
                             ft_pagamentos(
                                 select *
                             from stage_ft_pagamentos sfp
                             where
                             not exists(select
                             id_pagamento
                             from ft_pagamentos fp
                             where
                             sfp.id_pagamento = fp.id_pagamento));"""

    insert_cartao = """insert
                         into
                         dm_cartao_funcionario(
                             select *
                         from stage_dm_cartao_funcionario sdcf
                         where not exists(select doc_portador
                                            from dm_cartao_funcionario dcf
                                           where sdcf.doc_portador = dcf.doc_portador
                                             and sdcf.nome_portador = dcf.nome_portador
                                             and sdcf.id_tipo_cartao = dcf.id_tipo_cartao));"""

    insert_municipio = """insert
                                into
                                dm_municipio(
                                    select *
                                from stage_dm_municipio sdm
                                where
                                not exists(select
                                cod_ibge_municipio
                                from dm_municipio dm
                                where
                                sdm.cod_ibge_municipio = dm.cod_ibge_municipio));"""

    insert_unidade = """insert
              into
              dm_unidade_gestora(
                  select *
              from stage_dm_unidade_gestora sdug
              where
              not exists(select *
              from dm_unidade_gestora dug
              where
              sdug.cod_unidade_gestora = dug.cod_unidade_gestora));"""

    insert_estabelecimento = """insert
             into
             dm_estabelecimento(
                 select *
             from stage_dm_estabelecimento sde
             where
             not exists(select *
             from dm_estabelecimento de
             where
             sde.cpf_cnpj = de.cpf_cnpj
                            and sde.numero_inscr_social = de.numero_inscr_social));"""

