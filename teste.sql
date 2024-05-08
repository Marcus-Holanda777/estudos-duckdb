   INSTALL spatial;
   LOAD spatial;

   -- CONFIGURACOES
   SET preserve_insertion_order = FALSE;
   SET temp_directory = 'c:\arquivos\temp.temp';
   SET memory_limit = '45G';

   /*
    *  FUNCAO PARA IMPORTAR OS DADOS DE
    *  UMA PASTA ESPECIFICA DENTRO DE data_lake, MES e PERIODO
    *  LER TODOS OA ARQUIVOS .parquet DENTRO DA PASTA
    */
   CREATE OR REPLACE MACRO data_lake(tipo, ano, mes)
   AS 'c:\vendas_uc\data_lake\' || format('{}\{}\{:02d}', tipo, ano, mes) || '\*.parquet';
     
   /*
    *  CARREGA BASE DIMENÇÕES
    */
   CREATE OR REPLACE MACRO dimensions(file)
   AS 'C:\dimensions\' || file;
     
   /*
    *  MACRO QUE RETORNA UMA TABELA, DE VENDAS
    *  TANTO VENDA como PRE_VENDA
    */
   CREATE OR REPLACE MACRO import_vendas (
       tipo, 
       ano, 
       mes,
       col_etiqueta,
       colunas
   )
   AS TABLE
   (
     WITH show_tbl AS (
	   FROM read_parquet(data_lake(tipo, ano, mes))
	   SELECT
	       COLUMNS(c -> list_contains(colunas, c) OR len(colunas) = 0),
		   LPAD(TRIM(COLUMNS(col_etiqueta)), 30, '0')
	   )
	   FROM show_tbl
	   SELECT 
	      COLUMNS(colunas[1])::date AS 'periodo',
	      COLUMNS(colunas[2])       AS 'filial',
	      COLUMNS(colunas[3])       AS 'cod',
	      COLUMNS(colunas[4])       AS 'venda',
	      COLUMNS(col_etiqueta)     AS 'etiqueta'
    );
   
   
    -- CARREGA AS DIMENÇÕES
    CREATE OR REPLACE TABLE mestre
    AS
     FROM read_parquet(dimensions('prod_mestre.parquet'));
   
       /*
        *  UNIR TABELA DE VENDA E PREVENDA
        *  CONSIDERAR O MAIOR VALOR DE VENDA NAS REPETICOES
        *  DE ETIQUETA
        */
       CREATE OR REPLACE MACRO gera_venda(
           ano,
           mes
       )
       AS TABLE 
       (
          WITH inner_vendas AS
          (
	        FROM import_vendas('COSMOSMOV', ano, mes,
	        'NUMERO_AUTORIZ_PAGUEMENOS',
	          [
	             'MVVC_DT_MOV', 
	             'MVVC_CD_FILIAL_MOV',
	             'MVVP_NR_PRD', 
	             'MVVP_VL_PRD_VEN'
	          ]
	        )
	        UNION all
	        FROM import_vendas('PRE_VENDA', ano, mes,
	        'VD_COD_ETIQUETA_ULCH',
	          [
	             'VC_DH_VENDA', 
	             'VC_CD_FILIAL', 
	             'VD_CD_PRODUTO', 
	             'VD_VL_PRODUTO_COM_DESCONTO'
	          ]
	        )
	      ),
	      where_id_row AS (
	       FROM inner_vendas AS r
		     SELECT 
		        DISTINCT ON(r.etiqueta) r.*
		     ORDER BY venda DESC
	      )
	      FROM where_id_row
	   );
	    
	   -- EXPORTA PARA EXCEL 
	   COPY(
	       SELECT 
		       v.periodo,
		       v.filial,
		       v.cod,
		       m.NM_PROD       AS nm,
		       m.N1            AS cat_01,
		       v.etiqueta,
		       v.venda::double AS venda
		   FROM gera_venda(2024, 1) AS v 
		   INNER JOIN MAIN.MESTRE AS m ON v.cod = m.COD
	    ) TO 'c:\arquivos\UC_VENDAS.xlsx'
		WITH (
		   FORMAT GDAL,
		   DRIVER 'xlsx'
		);
