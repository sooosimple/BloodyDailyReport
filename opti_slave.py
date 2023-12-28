# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import time
import reports
import logging
from inspect import getmembers

logging.basicConfig(level=logging.INFO, filename='daily_rep.log', filemode='w',
                    format="%(levelname)s %(message)s")

list_types_reports = [1, 2]
print('opti_slave\nСreated Ilya S.')
print('______________________________________________________________\n')

report_daily = reports.DailyReportOpti()
while report_daily.checking_data_file_directory() == False:
    input('\nПоместите файл с данными в директорию(файл должен быть назван "data"): {}\n'
          'Если данные на месте нажмите Enter\n'.format(report_daily._data_book_directory))
try:
    report_daily.reading_workbook_xlsx()
except Exception as exs:
    logging.error(exc_info=True)

exit_flag = '1'
while exit_flag == '1':
    date_selection = input('\nДата отчета - {}?(да - 1, нет - 0): '.format(report_daily._date_report))
    while date_selection not in ['0', '1']:
        print('WARNING: Возможен ответ только: да - 1, нет - 0\n')
        date_selection = input('Ваш ответ:  ')

    if date_selection == '0':
        date = input('Введите новую дату отчета (ГГГГ-ММ-ДД): ')
        print('______________________________________________________________\n')
        report_daily.replacing_date(date=date)
        print(report_daily)
        logging.info(report_daily)
        print('______________________________________________________________\n')
        try:
            logging.info('START report_daily.anomaly_search()')
            report_daily.anomaly_search()
            logging.info('END report_daily.anomaly_search()')
            logging.info('START report_daily.build_and_save()')
            report_daily.build_and_save()
            logging.info('END report_daily.build_and_save()')
            logging.info('START report_daily.decoration()')
            report_daily.decoration()
            logging.info('END report_daily.decoration()')
        except Exception as exs:
            logging.error(exs, exc_info=True)
    else:
        print('______________________________________________________________\n')
        print(report_daily)
        try:
            logging.info('START report_daily.anomaly_search()')
            report_daily.anomaly_search()
            logging.info('END report_daily.anomaly_search()')
            logging.info('START report_daily.build_and_save()')
            report_daily.build_and_save()
            logging.info('END report_daily.build_and_save()')
            logging.info('START report_daily.decoration()')
            report_daily.decoration()
            logging.info('END report_daily.decoration()')
        except Exception as exs:
            logging.error(exs, exc_info=True)
    exit_flag = input('\nХотите ли вы собрать еще один отчет?(1 - да, 0 - нет): ')

