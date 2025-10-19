USE [Telecom]
GO

/****** Object:  StoredProcedure [dbo].[SP_TAB_CHAMADAS_DUPLICADAS]    Script Date: 19/10/2025 16:43:48 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		rodrigues1.allan
-- Create date: 19/10/2025
-- Description:	Alimenta telecom.dbo.TAB_CHAMADAS_DUPLICADAS
-- =============================================
CREATE PROCEDURE [dbo].[SP_TAB_CHAMADAS_DUPLICADAS]
	-- Add the parameters for the stored procedure here
	@DataExecucao Date
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	DECLARE @NomeTabela NVARCHAR(255);
	DECLARE @sql NVARCHAR(MAX);

	-- Formata a data no padrão AAAAMMDD
	SET @NomeTabela = N'Atlas.dbo.tb_Dialer_Calls_' + CONVERT(NVARCHAR(8), @DataExecucao, 112);
	--SET @NomeTabela = 'Atlas.dbo.tb_Dialer_Calls_20251018';

	-- Monta a query dinâmica
	SET @sql = N'
	WITH ContagemChamadasPorNumero AS (
		-- Subconsulta para contar as chamadas para cada número (origem) dentro de cada grupo principal
		SELECT
			YEAR(A.calldate) AS Ano,
			MONTH(A.calldate) AS Mes,
			DAY(A.calldate) AS Dia,
			A.hora AS HORA,
			CAST(A.calldate AS DATE) AS DATA,
			C.servidor AS SERVIDOR,
			C.fila AS FILA,
			C.tecnologia AS TECNOLOGIA,
			C.carteira AS CLIENTE,
			A.mailing,
			B.Rota AS ROTA,
			B.pag_sub AS PAG_SUB,
			B.Operadora AS OPERADORA,
			CASE WHEN A.classe IN (''vc1'',''vc2'',''vc3'') THEN ''MOVEL'' ELSE ''FIXO'' END AS CLASSE,
			A.origem,
			COUNT(*) AS quantidade_chamadas_por_numero
		FROM ' +@NomeTabela+ N' AS A with(nolock)
		LEFT JOIN
			Telecom.dbo.TAB_TELECOM_DP_ROTAS AS B ON A.terminator = B.Rota
		LEFT JOIN
			Telecom.dbo.TAB_TELECOM_DP_FILAS AS C ON A.fila = C.fila AND A.id_servidor_callflex = C.idf_servidor_callflex
		WHERE
			A.tipo = ''dis''
		GROUP BY
			YEAR(A.calldate),
			MONTH(A.calldate),
			DAY(A.calldate),
			A.hora,
			CAST(A.calldate AS DATE),
			C.servidor,
			C.fila,
			C.tecnologia,
			C.carteira,
			A.mailing,
			B.Rota,
			B.pag_sub,
			B.Operadora,
			CASE WHEN A.classe IN (''vc1'',''vc2'',''vc3'') THEN ''MOVEL'' ELSE ''FIXO'' END,
			A.origem
	)
	-- Consulta final para agregar os resultados e apresentar no formato desejado
	INSERT INTO telecom.dbo.TAB_CHAMADAS_DUPLICADAS
	SELECT
		Ano,
		Mes,
		Dia,
		HORA,
		DATA,
		SERVIDOR,
		FILA,
		TECNOLOGIA,
		CLIENTE,
		mailing,
		ROTA,
		PAG_SUB,
		OPERADORA,
		CLASSE,
		-- Contagem total de chamadas no grupo
		SUM(quantidade_chamadas_por_numero) AS TOTAL_DE_DISCAGENS,
		-- Soma das chamadas "extras" (duplicadas)
		SUM(CASE WHEN quantidade_chamadas_por_numero > 1 THEN quantidade_chamadas_por_numero - 1 ELSE 0 END) AS TOTAL_DUPLICADAS,
		-- Contagem de números de telefone únicos no grupo que receberam mais de uma chamada
		SUM(CASE WHEN quantidade_chamadas_por_numero > 1 THEN 1 ELSE 0 END) ASTOTAL_DE_NUMEROS_AFETADOS,
		-- Contagem de números que receberam exatamente 2 ligações
		SUM(CASE WHEN quantidade_chamadas_por_numero = 2 THEN 1 ELSE 0 END) AS DUAS_LIGACOES,
		-- Contagem de números que receberam exatamente 3 ligações
		SUM(CASE WHEN quantidade_chamadas_por_numero = 3 THEN 1 ELSE 0 END) AS TRES_LIGACOES,
		-- Contagem de números que receberam 4 ou mais ligações
		SUM(CASE WHEN quantidade_chamadas_por_numero >= 4 THEN 1 ELSE 0 END) AS QUATRO_LIGACOES_OU_MAIS
	FROM
		ContagemChamadasPorNumero
	GROUP BY
		Ano,
		Mes,
		Dia,
		HORA,
		DATA,
		SERVIDOR,
		FILA,
		TECNOLOGIA,
		CLIENTE,
		mailing,
		ROTA,
		PAG_SUB,
		OPERADORA,
		CLASSE
	ORDER BY
		DATA,
		HORA;
	'
	-- Executa a query montada
	DELETE telecom.dbo.TAB_CHAMADAS_DUPLICADAS
	WHERE 
		DATA = @DataExecucao
		OR DATA < DATEADD(DAY, -90, CAST(GETDATE() AS DATE))
	EXEC sp_executesql @sql;
END
GO

