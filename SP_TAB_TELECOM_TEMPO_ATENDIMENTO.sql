USE [Telecom]
GO

/****** Object:  StoredProcedure [dbo].[SP_TAB_TELECOM_TEMPO_ATENDIMENTO]    Script Date: 19/10/2025 16:44:47 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SP_TAB_TELECOM_TEMPO_ATENDIMENTO]
    -- Parâmetro de entrada para a data que define a tabela
    @DataExecucao DATE
AS
BEGIN
    -- Impede a contagem de linhas afetadas de ser retornada
    SET NOCOUNT OFF;

    DECLARE @NomeTabela NVARCHAR(255);
    DECLARE @SQL NVARCHAR(MAX);

    -- 1. Constrói o nome da tabela dinamicamente
    -- Ex: Se @DataExecucao for '2025-09-26', o nome será 'Atlas.dbo.tb_Dialer_Calls_20250926'
    SET @NomeTabela = N'Atlas.dbo.tb_Dialer_Calls_' + CONVERT(NVARCHAR(8), @DataExecucao, 112);

    -- 2. Monta a string da consulta (SQL Dinâmico)
    SET @SQL = N'
    INSERT INTO telecom.dbo.TAB_TELECOM_TEMPO_ATENDIMENTO
    SELECT
        CAST(Calldate AS date) AS DATA,
        hora,
        id_servidor_callflex,
        fila,
        terminator,
        CASE
            WHEN statusatendimento = ''MACHINE'' THEN ''MACHINE''
            WHEN statusatendimento = ''ABANDON'' THEN ''ABANDONO''
            ELSE ''HUMAN''
        END AS "STATUS",
        -------------------------------------------------
        COUNT(CASE
            WHEN rbillsec < 3 THEN 1
        END) AS MENOR_3,

        COUNT(CASE
            WHEN rbillsec >= 3 AND rbillsec < 6 THEN 1
        END) AS ENTRE_3_6,

        COUNT(CASE
            WHEN rbillsec >= 6 THEN 1
        END) AS MAIOR_6,
        -------------------------------------------------
        SUM(CASE
            WHEN rbillsec < 3 THEN valor
            ELSE 0
        END) AS RS_MENOR_3,

        SUM(CASE
            WHEN rbillsec >= 3 AND rbillsec < 6 THEN valor
            ELSE 0
        END) AS RS_ENTRE_3_6,

        SUM(CASE
            WHEN rbillsec >= 6 THEN valor
            ELSE 0
        END) AS RS_MAIOR_6
    FROM ' + @NomeTabela + N' A
    WHERE
        status = ''ANSWERED''
        AND tipo = ''dis''
    GROUP BY
        CAST(Calldate AS date),
        hora,
        id_servidor_callflex,
        fila,
        terminator,
        CASE
            WHEN statusatendimento = ''MACHINE'' THEN ''MACHINE''
            WHEN statusatendimento = ''ABANDON'' THEN ''ABANDONO''
            ELSE ''HUMAN''
        END
    ORDER BY id_servidor_callflex, fila;';

    -- 3. Executa a consulta montada e deleta a da selecionada
    -- Usar sp_executesql é mais seguro e eficiente que EXEC()
	DELETE FROM telecom.dbo.TAB_TELECOM_TEMPO_ATENDIMENTO
	WHERE 
		DATA = @DataExecucao
		OR DATA < DATEADD(DAY, -90, CAST(GETDATE() AS DATE));
    EXEC sp_executesql @SQL;

END;
GO

