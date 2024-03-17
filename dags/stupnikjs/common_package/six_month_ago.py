import datetime
import calendar


def six_month_ago_ts(date_datetime):
    curr_month = date_datetime.month
    curr_day = date_datetime.day
    
    months = []
    # decremente les 6 derniers mois et les ajoutes dans une liste 
    for i in range(7): 
        months.append(curr_month)
        if curr_month != 1:
            curr_month -= 1
        else:
            curr_month = 12

    # liste correspondante des nombres de jours 
    day_count = []
    for i in months: 
        num_days = calendar.monthrange(date_datetime.year, i)[1]
        day_count.append(num_days)

    # est ce que il y a six mois était l'année dernière 
    last_year = months[0] < months[-1]


    if last_year : 
        if date_datetime.day < day_count[-1]: 
            six_mounth_ago = datetime.datetime(date_datetime.year - 1, months[-1], date_datetime.day)
        else:
            six_mounth_ago = datetime.datetime(date_datetime.year - 1, months[-1] , day_count[-1])
    else:
        if date_datetime.day < day_count[-1]: 
            six_mounth_ago = datetime.datetime(date_datetime.year, months[-1], date_datetime.day )
        else:
            six_mounth_ago = datetime.datetime(date_datetime.year, months[-1] , day_count[-1])

    # gerer le cas ou le jour est 31 ou 30 et n'existe pas dans le mois d'il ya 6mois

    six_mounth_ago_unix = six_mounth_ago.timestamp() 
      
    return six_mounth_ago_unix
