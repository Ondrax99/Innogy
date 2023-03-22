import pandas as pd
import sqlite3
import os
import sys


class Analysis:
    def __init__(self, db_name):
        """
        Inicializace databáze a databázového připojení
        """
        if os.path.exists(os.path.join(os.getcwd(), db_name)):
            self.db_name = db_name
        else:
            print("Error: Database name doesn't exists, Expected DB name is test.db", sys.stderr)
            sys.exit(1)
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()
        self.cursor.execute("""DROP TABLE IF EXISTS ocenena_spotreba;""")

    def task_1(self):
        """
        V tabulce 'smlouvy' je ve sloupci 'platnost_do' chybný formát data, převeďte jej na YYYY-MM-DD
        """
        smlouvy_df = pd.read_sql_query("Select * from smlouvy", self.connection)
        try:
            smlouvy_df['platnost_do'] = pd.to_datetime(smlouvy_df['platnost_do'], format='%Y-%d-%m').dt.strftime('%Y-%m-%d')
        except ValueError:
            print("Error: Unexpected date format", sys.stderr)
            sys.exit(1)
        smlouvy_df.to_sql('smlouvy', con=self.connection, if_exists='replace', index=False)
        smlouvy_df.to_csv('task1.csv')

    def task_2(self, smlouva_id, datum_od, datum_do):
        """
        Najděte celkovou spotřebu v GWH za červen 2022 pro id 1234
        """
        spotreba_df = pd.read_sql_query(f"Select * from spotreba where spotreba.id_smlouvy = '{smlouva_id}' and spotreba.dates between '{datum_od}' and '{datum_do}'", self.connection)
        spotreba_df['spotreba_mwh'] = spotreba_df['spotreba_mwh'].astype('float64')
        celkova_spotreba = spotreba_df['spotreba_mwh'].sum() / 1000  # Celkova suma a prevod na GWH
        print(f"Celkova spotreba pro smlouvu:{smlouva_id} je:{celkova_spotreba:.5f} GWH")

    def task_3(self, year):
        """
        Vytvořte tabulku celkové oceněné spotřeby (tj. suma(cena*spotreba) ) za rok 2022 pro jednotlivé smlouvy. Smlouvy, které jsou v eurech, přepočtěte na koruny konstantním kurzem 1EUR = 25Kč
        """
        spotreba_df = pd.read_sql_query("Select smlouvy.id, spotreba.dates, spotreba.spotreba_mwh, smlouvy.mena, smlouvy.cena_za_mwh from smlouvy inner join spotreba on smlouvy.id = spotreba.id_smlouvy", self.connection)
        spotreba_df['dates'] = spotreba_df['dates'].astype('datetime64')
        spotreba_df = spotreba_df[spotreba_df['dates'].dt.year == year]
        spotreba_df['id'] = spotreba_df['id'].astype('int64')
        spotreba_df['spotreba_mwh'] = spotreba_df['spotreba_mwh'].astype('float64')
        spotreba_df['cena_za_mwh'] = spotreba_df['cena_za_mwh'].astype('float64')
        spotreba_df.loc[spotreba_df['mena'] == 'EUR', 'cena_za_mwh'] = spotreba_df['cena_za_mwh'] * 25
        spotreba_df['ocenena_spotreba'] = spotreba_df['cena_za_mwh'] * spotreba_df['spotreba_mwh']
        spotreba_df = spotreba_df[['id', 'ocenena_spotreba']]
        spotreba_df = spotreba_df.groupby('id').sum()
        spotreba_df.reset_index(inplace=True)
        spotreba_df['year'] = year
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS ocenena_spotreba (
                                id INTEGER PRIMARY KEY,
                                year Integer Not Null,
                                celkova_ocenena_spotreba REAL);""")
        spotreba_df.to_sql('ocenena_spotreba', con=self.connection, if_exists='replace', index=False)
        spotreba_df.to_csv('task3.csv')


if __name__ == "__main__":
    A = Analysis(db_name="test.db")
    A.task_1()
    A.task_2(smlouva_id='1234', datum_od='2022-06-01', datum_do='2022-07-01')
    A.task_3(year=2022)
