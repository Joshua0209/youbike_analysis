from datetime import date, datetime
import pandas as pd

HOURMIN, HOURMAX = 0, 23
data_dir = "release"
semester = list(range(20231002, 20231007)) + list(range(20231011, 20231014)) + \
    list(range(20231016, 20231021)) + list(range(20231030, 20231032)) + \
    list(range(20231101, 20231104)) + list(range(20231106, 20231111)) + \
    list(range(20231113, 20231118)) + list(range(20231120, 20231125)) + \
    list(range(20231127, 20231131)) + list(range(20231201, 20231202)) + \
    list(range(20231204, 20231208)) + list(range(20231211, 20231216))
midterm = list(range(20231023, 20231028))
final = list(range(20231218, 20231223))
stands = [500101005, 500101007, 500101008, 500101009, 500101010, 500101012, 500101014, 500101016, 500101018, 500101020, 500101021, 500101022, 500101023, 500101024, 500101025, 500101027, 500101028, 500101029, 500101032, 500101033, 500101034, 500101036, 500101037, 500101038, 500101039, 500101040, 500101041, 500101042, 500101093, 500101094, 500101104, 500101181, 500101184, 500101185, 500101191, 500101193, 500101209, 500101216, 500106001, 500106002, 500106003, 500106004, 500106007, 500106090, 500119005, 500119006, 500119007, 500119008,
          500119009, 500119043, 500119044, 500119045, 500119046, 500119047, 500119048, 500119049, 500119050, 500119051, 500119052, 500119053, 500119054, 500119055, 500119056, 500119057, 500119058, 500119059, 500119060, 500119061, 500119062, 500119063, 500119064, 500119065, 500119066, 500119067, 500119068, 500119069, 500119070, 500119071, 500119072, 500119074, 500119075, 500119076, 500119077, 500119078, 500119079, 500119080, 500119081, 500119082, 500119083, 500119084, 500119085, 500119086, 500119087, 500119088, 500119089, 500119090, 500119091]
print(semester)

df_all = pd.DataFrame()
# stands = [500101005, 500101007, 500101008, 500101009, 500101010, 500101012, 500101014, 500101022, 500101181, 500106003, 500106004]
for date in semester:
    for stand in stands:
        date_str = str(date)
        try:
            df = pd.read_json(
                f'release/{date}/{stand}.json').T.reset_index(names="time")
        except:
            continue
        df = df.dropna()
        df = df.drop(columns='act', axis=1)
        df['time'] = pd.to_datetime(df['time'])
        # df = df[df['time'].apply(lambda t: HOURMIN <= t.hour <= HOURMAX and t.minute % 5 == 0)]
        date_part = datetime(int(date_str[:4]), int(
            date_str[4:6]), int(date_str[6:]))
        df['time'] = df['time'].apply(
            lambda x: date_part + pd.Timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
        df['sno'] = stand
        df['sbi%'] = df['sbi']/df['tot']
        df['tot'] = df['tot'].astype('int')
        df['sbi'] = df['sbi'].astype('int')
        df['bemp'] = df['bemp'].astype('int')
        df_all = pd.concat([df_all, df], ignore_index=True)
df = df_all

df.to_parquet('./data/semester.parquet')
