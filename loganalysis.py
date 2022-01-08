logg_rdd =  sc.textFile("/FileStore/tables/application.log")
unparsed_line = sc.accumulator(0)
empty_line = sc.accumulator(0)

def parse_fun(log_iter):
  global unparsed_line
  global empty_line
  for log in log_iter:
    temp_str = log.strip()
    if temp_str:
      main_str = temp_str.split(" ")
      if(len(main_str) >= 4):
        log_name = main_str[3]
        if log_name.lower() in ['[trace]','[debug]','[info]','[warn]','[fatal]','[error]'] :
          yield log_name
        else:
          unparsed_line += 1
      else:
        unparsed_line += 1
    else:
      empty_line += 1
log_count_dict = logg_rdd.mapPartitions(parse_fun).countByValue()

for i,j in log_count_dict.items():
  print(f'{i} count is {j}')
print(f'count of empty line is {empty_line.value}')
print(f'count of unparse line is {unparsed_line.value}')
