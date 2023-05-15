from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def get_dtype(df, colname):
    '''
    funcao retorna o tipo de uma coluna
    Param: 
      df - Pyspark Dataframe de entrada
      colname - Nome da coluna de interesse
    '''
    return [dtype for name, dtype in df.dtypes if name == colname][0]


def testColumns(df, security_limit=True):
  # Set DF to results
  columns = ["column", "type", "all_null", "qnt_null", "all_zero",
      "qnt_zero", "qnt_not_null_or_zero", "total_rows"]
  vals = [('name', 'type',  True, 0, True, 0, 0, 0)]
  results = spark.createDataFrame(
      vals, columns).where(col('column') == 'apagar')
  results_list = []

  # Info about given DF
  columns = df.columns
  total_rows = df.count()

  # Check size of df
  security_limit_rows = 5 * 10**6
  if (security_limit and total_rows > security_limit_rows):
    print("Table with more than " + str(security_limit_rows) +
          " rows. \nTo process that dataframe either way set the optional parameter as False. \nex.: function(df, False)")
    return results

  for column in columns:
    qnt_null = df.filter(col(column).isNull()).count()
    all_null = qnt_null == total_rows

    column_type = get_dtype(df, column)
    if (column_type != 'timestamp'):
      qnt_zero = df.filter(col(column) == 0).count()
      all_zero = qnt_zero == total_rows
    else:
      qnt_zero = df.filter(col(column) == '1900-01-01').count()
      all_zero = qnt_zero == total_rows
    qnt_not_null_or_zero = total_rows - (qnt_null+qnt_zero)

    results_list.append([column, column_type, all_null, qnt_null,
                        all_zero, qnt_zero, qnt_not_null_or_zero, total_rows])

  return results.union(spark.createDataFrame(results_list))


def testRelation(df1, df2):
  columns = ["df1_column", "df2_column",
      "matching_number", "matching_porcentage"]
  vals = [('name', 'type',  2, 2)]
  results = spark.createDataFrame(vals, columns).where(
      col('df1_column') == 'apagar')
  result = results

  df1_size = df1.count()
  df2_size = df2.count()
  df1_columns = df1.columns
  df2_columns = df2.columns

  for df1_column in df1_columns:
    for df2_column in df2_columns:
      try:
        if (df1.dropDuplicates([df1_column]).count() == df1_size and df2.dropDuplicates([df2_column]).count() == df2_size):
          matching_number = df1.join(
              df2, df1[df1_column] == df2[df2_column], 'inner').count()
          matching_porcentage = matching_number / df1_size
          result = result.union(spark.createDataFrame(
              [(df1_column, df2_column, matching_number, matching_porcentage)], columns))
          print(df1_column, ' x ', df2_column, ' match-number: ',
                matching_number, 'match-%: ', matching_porcentage)
      except Exception as e:
        # print(e)
        result = result.union(spark.createDataFrame(
            [(df1_column, df2_column, 0, 0)], columns))
        print(df1_column, ' x ', df2_column, ' -> Tipos incompatíveis')
  return results.union(result)


def diff_dfs(df1, df2, chave_dfs):
  '''
  Funcao que verifica de várias formas se há 
  diferenças entre Pyspark dataFrames.

  Testes como: 
	- Número de colunas e se tem os mesmos nomes
	- Tipos das colunas
	- Quantidade de registros
	- Unicidade de chave primária
	- Diagrama de Veen relativo a chave primária
	- etc...
  
  '''
  class bcolors:
    black = '\x1b[30m'
    red = '\x1b[31m'
    green = '\x1b[32m'
    yellow = '\x1b[33m'
    blue = '\x1b[34m'
    magenta = '\x1b[35m'
    cyan = '\x1b[36m'
    white = '\x1b[37m'
    crimson = '\x1b[38m'
    def red_print(string):
      print(bcolors.red
          + string
          + bcolors.white
           )
    def green_print(string):
      print(bcolors.green
          + string
          + bcolors.white
           )
    def magenta_print(string):
      print(bcolors.magenta
          + string
          + bcolors.white
           )
    def cyan_print(string):
      print(bcolors.cyan
          + string
          + bcolors.white
           )
  
  # --- Testes similaridade de colunas
  bcolors.cyan_print("# --- Testes similaridade de colunas --- #")
  df1_col = df1.columns
  df2_col = df2.columns
  if(df1.columns != df2.columns):
    print('Dataframes com colunas DIFERENTES!')
  else:
    print('Dataframes com colunas iguais!')
    
  # --- Testes em numero de linhas
  bcolors.cyan_print("# --- Testes em numero de linhas --- #")
  df1_count = df1.count()
  df2_count = df2.count()
  
  print('df1: ', df1_count, '\ndf2: ', df2_count, )
  
  if(df1_count > df2_count):
    print('df1 > df2 em: ', df1_count - df2_count)
  elif(df1_count < df2_count):
    print('df2 > df1 em: ', df2_count - df1_count)
  else:
    print('df1 e df2 tem o MESMO tamanho')
    
  # --- Respeita chaves únicas
  bcolors.cyan_print("# --- Respeita chaves únicas --- #")
  df1_distinct_keys = df1.select(df1[chave_dfs]).distinct().count()
  df2_distinct_keys = df2.select(df2[chave_dfs]).distinct().count()
  
  if(df1_distinct_keys < df1_count):
    print('df1 tem chaves únicas REPETIDAS')
  else:
    print('df1 respeita chaves únicas')
    
  if(df2_distinct_keys < df2_count):
    print('df2 tem chaves únicas REPETIDAS')
  else:
    print('df2 respeita chaves únicas')
  
  # --- Testes de relcionamentos entre as bases
  bcolors.cyan_print("# --- Testes de relacionamentos entre as bases --- #")
  df1_join_df2 = df1.alias("df1").join(
    df2.alias("df2")
    , df1[chave_dfs] == df2[chave_dfs]
    , 'full'
  )
  
  # --- Quantos registro tem em uma e nao na outra
  df1_tem_df2_nao = df1_join_df2.filter(
    df1[chave_dfs].isNotNull()
    & df2[chave_dfs].isNull()
  )
  
  df2_tem_df1_nao = df1_join_df2.filter(
    df2[chave_dfs].isNotNull()
    & df1[chave_dfs].isNull()
  )
  
  print('df1_tem_df2_nao', df1_tem_df2_nao.count())
  if(df1_tem_df2_nao.count() > 0):
    df1_tem_df2_nao.display()

  print('df2_tem_df1_nao', df2_tem_df1_nao.count())
  if(df2_tem_df1_nao.count() > 0):
    df2_tem_df1_nao.display()
  
  df1_tem_df2_nao = df1_tem_df2_nao.count()
  df2_tem_df1_nao = df2_tem_df1_nao.count()
  
  print('"Venn Diagram": DF1 vs DF2 Keys')
  print('(', df1_tem_df2_nao, ' (', df1_count - df1_tem_df2_nao ,') ', df2_tem_df1_nao, ' )')
  
  df1_tem_df2_nao = df1_join_df2.filter(
    df1[chave_dfs].isNotNull()
    & df2[chave_dfs].isNull()
  )
  
  df2_tem_df1_nao = df1_join_df2.filter(
    df2[chave_dfs].isNotNull()
    & df1[chave_dfs].isNull()
  )

  # --- Verifica tipagem de colunas
  df1_types = dict(df1.dtypes)
  df2_types = dict(df2.dtypes)

  for item in df1_types:
    if(df1_types[item] != df2_types[item]):
      print('sao diferentes os tipos das colunas: ', item, end=' | ')
      print('tipos: ', df1_types[item] , ' e ', df2_types[item])

    
    
  
  # --- Quantos registros tem em uma e nao testando por coluna
  bcolors.cyan_print("# --- Registros diferentes entre bases --- #")


  df1 = df1.withColumn('key_row', concat_ws('|', *sorted(df1.columns)))
  df2 = df2.withColumn('key_row', concat_ws('|', *sorted(df2.columns)))


  
  nao_bate1 = df1.join(
    df2
    , df1['key_row'] == df2['key_row']
    , 'leftanti'
  ).withColumn('df', lit('df1'))

  nao_bate2 = df2.join(
    df1
    , df1['key_row'] == df2['key_row']
    , 'leftanti'
  ).withColumn('df', lit('df2'))

  nao_bate = nao_bate1.union(nao_bate2)

  print('Há ', nao_bate.count(), ' linhas com colunas diferentes ')

  nao_bate.display()

  return (df1_tem_df2_nao
        , df2_tem_df1_nao)