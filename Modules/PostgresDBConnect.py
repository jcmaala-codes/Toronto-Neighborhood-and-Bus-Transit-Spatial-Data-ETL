import os
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker
import os
from urllib.parse import quote
import geopandas as gpd
import geoalchemy2
import fiona


class PostgresDBConnect:

    def __init__(self):
       self._version = 'v1'


    class Connector:

        def __init__(self, environment):
            try:
                with open(r'F:\Toronto Railway Transit Routes\Modules\db_config.json') as config_file:
                    self._environments = json.load(config_file)['environments']
            except Exception as e:
                print('Error')
            self.environment = environment
            self.engine = None
            self.conn = None
            self._status = "Not Connected"

        def getAvailableEnvironments(self):
            return [{
                'env_name': env,
                'db_name': self._environments[env]['NAME'],
                'db_host': self._environments[env]['HOST'],
                'db_port': self._environments[env]['PORT']
            } for env in self._environments]

        def getStatus(self):
            return self._status

        def connect(self, echo=False):
            try:
                creds = self._environments[self.environment]
                print(f"Connecting to {self.environment} database")
                self.engine = create_engine(f"postgresql://{creds['USER']}:{quote(creds['PASS'])}@{creds['HOST']}:{creds['PORT']}/{creds['NAME']}" , echo=echo)
                self.conn = self.engine.connect()
                print(f"[Connect] Successfully connected to {self.environment} database ({creds['NAME']})")
                self._status = f"Connected to {self.environment} ({creds['NAME']} on {creds['HOST']} port {creds['PORT']})"
            except exc.SQLAlchemyError as e:
                print('[Connection Attempt Error] Error connecting to PostgreSQL database:', e)
            except KeyError as e:
                print('[Connection Attempt Error] Currently set env is unsupported.')

        def disconnect(self):
            if self.conn:
                self.engine.dispose()
                self.conn.close()
                self.engine = None
                self.conn = None
                print(f"[Disconnect] Successfully Disconnected from {self.environment} PostgreSQL database")
                self._status = "Not Connected"
            else:
                print('[Disconnection Attempt Error] No active connection to disconnect')

    class FileReader:
        def read_file(self, parent_folder, file_name, sheet=None, layer=None):

            file_extn = list(os.path.splitext(file_name))[1]
            file_path = os.path.join(parent_folder, file_name)
            
            match file_extn:
                case '.shp':
                    self._data = PostgresDBConnect._ShapeFileReader(os.path.join(file_path))
                    return self._data.df

                case '.csv':
                    self._data = PostgresDBConnect._CsvFileReader(os.path.join(file_path))
                    return self._data.df
                
                case '.xlsx':
                    self._data = PostgresDBConnect._XlsxFileReader(os.path.join(file_path, sheet))

                case '.kml':
                    self._data = PostgresDBConnect._KmlReader(file_path, layer)
                    return self._data.df

                case _:
                    print('Unsupported File Type')
                    return None

    class _ShapeFileReader:
        def __init__(self, file_path):
            self.df = gpd.read_file(file_path)

    class _CsvFileReader:
        def __init__(self,file_path):
            self.df = pd.read_csv(file_path)

    class _XlsxFileReader:
        def __init__(self,file_path, sheet):
            self.df = pd.read_excel(file_path, engine='openpyxl', sheet_name = sheet)

    # class _KmlReader:
    #     def __init__(self,file_path, layer=None):
    #         if layer:
    #             self.df = gpd.read_file(file_path, driver='KML', layer=layer)
    #         else:
    #             self.df = gpd.read_file(file_path, driver='KML')

    class _KmlReader:
        def __init__(self, file_path, layer=None):
            if layer:
                try:
                    self.df = gpd.read_file(file_path, driver='KML', layer=layer)
                except Exception as e:
                    print(f"[KML Reader] Skipping layer '{layer}' due to error: {e}")
                    self.df = gpd.GeoDataFrame()
            else:
                layers = fiona.listlayers(file_path)
                gdfs = []
                for lyr in layers:
                    try:
                        gdf = gpd.read_file(file_path, driver='KML', layer=lyr)
                        gdfs.append(gdf)
                    except Exception as e:
                        print(f"[KML Reader] Skipping layer '{lyr}' due to error: {e}")
                        continue

                if gdfs:
                    self.df = gpd.GeoDataFrame(
                        pd.concat(gdfs, ignore_index=True),
                        crs=gdfs[0].crs
                    )
                else:
                    print("[KML Reader] No valid layers could be loaded.")
                    self.df = gpd.GeoDataFrame()


    class DataDumper:
        def __init__(self, connection, engine):
            try:
                if connection and engine:
                    self.sql_conn = connection
                    self.sql_engine = engine
                    Session = sessionmaker(bind=self.sql_engine)
                    self.session = Session()
                else:
                    raise ValueError('Invalid Connection or Engine Values')
            except ValueError as ve:
                print(ve)
          
        def raw_geo_data_import(self, df_data, output_table_name, schema, if_exists='replace'):
            try:
                gdf = gpd.GeoDataFrame(df_data.copy(), geometry='geometry')
                gdf.to_postgis(output_table_name, self.sql_engine, if_exists=if_exists, index=False, schema=schema, chunksize=10000)
                print('[Data Dumper] Loaded to SQL Table')
                  
            except Exception as e:
                print('[Data Dumper Error] Error in Importing to SQL Table.')
                print(e)

        def raw_data_import(self, df_data, output_table_name, schema, if_exists='replace'):

            try:
                df = pd.DataFrame(df_data.copy())
                df.to_sql(output_table_name, self.sql_engine, if_exists=if_exists, index=False, schema=schema, chunksize=10000)
                print('[Data Dumper] Loaded to SQL Table')

            except Exception as e:
                print('[Data Dumper Error] Error in Importing to SQL Table.')
                print(e)

        def call_sp(self, sp_callback, params=None):
            try:
                with self.sql_engine.connect() as conn:
                    conn = conn.execution_options(autocommit=True)
                    if params:
                        placeholders = ", ".join([f":p{i}" for i in range(len(params))])
                        stmt = text(f"CALL {sp_callback}({placeholders})")
                        conn.execute(stmt, {f"p{i}": v for i, v in enumerate(params)})
                    else:
                        conn.execute(text(f"CALL {sp_callback}()"))
                print(f'[SP Callback] SP {sp_callback} executed successfully.')
            except Exception as e:
                print(f'[SP Callback] Error triggering stored procedure {sp_callback}')
                print(e)




