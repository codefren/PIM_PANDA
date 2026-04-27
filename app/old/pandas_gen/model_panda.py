from datetime import datetime

import numpy as np

from ..model_attribute import Attribute
from .Consulta_SQL import consulta, column_names_sql,column_names_csv
import pandas as pd
from ..model_traductor import Traductor
import paramiko
class PandasGen:

    def __init__(self,db,cons=None):
        self.db = db
        self.consulta = consulta if consulta else cons
        self.traductor = Traductor(db=self.db)
        self.tmp_traductions = pd.DataFrame(columns=["l_ori","l_dest","text_ori","text_dest"])

    def filter_blanks_data(self,data:list):
        for i in data:
            r = [str(j).replace(' ','') for j in i]
            if not all(r):
                data.remove(i)
        return data

    def get_proveedores(self):
        self.db.execute("select fldidproveedor, fldnombrefiscal from tbdproveedores")
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(tuple(row))
        r = self.filter_blanks_data(r)
        return r

    def get_familias(self):
        self.db.execute("select fldidfamilia, flddescripcion from tbdfamilias")
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(tuple(row))
        r = self.filter_blanks_data(r)
        return r

    def get_product_families(self,products:list):
        self.db.execute(f"select fldidfamilia from tbdarticulos0 where fldidarticulo in ({','.join(['?' for _ in products])})",products)
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(row[0])
        return r


    def get_subfamilias(self):
        self.db.execute("select fldidsubfamilia from tbdarticulos5 group by fldidsubfamilia")
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(tuple(row))
        r = self.filter_blanks_data(r)
        return r

    def get_temporadas(self):
        self.db.execute("select fldidtemporada, flddescripcion from tbdtemporadas")
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(tuple(row) if row[1] != '' else (row[0],row[0]))
        r = self.filter_blanks_data(r)
        return r

    def get_subtemporadas(self):
        self.db.execute("select fldidsubtemporada from tbdarticulos5 group by fldidsubtemporada")
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(tuple(row))
        r = self.filter_blanks_data(r)
        return r

    def get_generos(self):
        self.db.execute("select fldidsexo from tbdarticulos5 group by fldidsexo")
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(tuple(row))
        r = self.filter_blanks_data(r)
        return r

    def get_marcas(self):
        self.db.execute("select fldidmarca from tbdarticulos5 group by fldidmarca")
        res = self.db.fetchall()
        r = []
        for row in res:
            r.append(tuple(row))
        r = self.filter_blanks_data(r)
        return r

    def __transformar_peso(self,peso):
        if peso != '':
            try:
                peso = float(peso)
                if peso > 0:
                    return peso/1000
            except:
                return ''
        return ''

    def __get_tmp_translation(self,text,l_dest,l_ori):
        if text != '':
            try:
                text = str(text)
                if text != '':
                    T = self.tmp_traductions[(self.tmp_traductions['text_ori'] == text) & (self.tmp_traductions['l_ori'] == l_ori) & (self.tmp_traductions['l_dest'] == l_dest)]
                    if T.shape[0] > 0:
                        return T['text_dest'].values[0]
            except Exception as e:
                print('Error getting translation', str(e))
                return ''
        return ''

    def __save_tmp_translation(self,text,l_dest,l_ori,text_dest):
        try:
            text = str(text)
            if text:
                assert isinstance(self.tmp_traductions, pd.DataFrame)
                new = pd.DataFrame([{'l_ori':l_ori,'l_dest':l_dest,'text_ori':text,'text_dest':text_dest}])
                self.tmp_traductions = pd.concat([self.tmp_traductions,new],ignore_index=True)
        except Exception as e:
            print('Error saving translation, in __save_tmp_translation()', str(e))
            return ''


    def __save_tmp_translation_batch(self,data:list[dict[str:str]]):
        # Asegúrate de que self.tmp_traductions es un DataFrame.
        assert isinstance(self.tmp_traductions, pd.DataFrame)

        # Convertir los datos de entrada en un DataFrame.
        new_translations = pd.DataFrame(data)

        # Concatenar el nuevo DataFrame con el existente.
        # Usamos pd.concat en lugar de append para mejorar la eficiencia al trabajar con múltiples filas.
        self.tmp_traductions = pd.concat([self.tmp_traductions, new_translations], ignore_index=True)

    def __translate(self,text,l_dest,l_ori='DETECT LANGUAGE'):
        print('Translating',text,l_dest,l_ori)
        if text != '':
            try:
                text = str(text)
                if text != '':
                    T = self.__get_tmp_translation(text,l_dest,l_ori) if l_ori != 'DETECT LANGUAGE' else None
                    if T:
                        return T
                    init_time = datetime.now()
                    T = self.traductor.translate_save(l_ori=l_ori,l_dest=l_dest,text_ori=text)
                    print(f'Traduction elapsed {(datetime.now() - init_time).total_seconds()}')
                    self.__save_tmp_translation(text,l_dest,l_ori,T)
                    return T
            except Exception as e:
                print('Error translating', str(e))
                return ''
        return ''

    def __translate_batch(self,data:list[dict[str,str]]):
        try:
            data_final = self.traductor.translate_save_batch(data)
            self.__save_tmp_translation_batch(data_final)
            return data_final
        except Exception as e:
            print('Error translating batch', str(e))
            return None

    def limpia_temporal(self):
        self.db.execute("delete from zzTempArticuloColor")
        self.db.commit()

    def carga_temporal(self,articulo_inicial=None,articulo_final=None,proveedor=None,temporada=None,subtemporada=None,
                       familia=None,subfamilia=None,genero=None,marca=None):
        self.limpia_temporal()
        q = '''
            insert into zzTempArticuloColor 
            SELECT a.fldIdArticulo, a.fldIdColor FROM tbdArticulosColor a, tbdArticulos5 b, tbdArticulos0 c 
            where a.fldIdArticulo = b.fldIdArticulo 
            AND a.fldIdArticulo = c.fldIdArticulo 
            '''
        q += 'AND a.fldidarticulo >= ? ' if articulo_inicial else ''
        q += 'AND a.fldidarticulo <= ? ' if articulo_final else ''
        q += 'AND c.fldIdProveedor = ? ' if proveedor else ''
        q += 'AND b.fldIdTemporada = ? ' if temporada else ''
        q += 'AND b.fldIdSubTemporada = ? ' if subtemporada else ''
        q += 'AND b.fldIdFamilia = ? ' if familia else ''
        q += 'AND b.fldIdSubFamilia = ? ' if subfamilia else ''
        q += 'AND b.fldIdSexo = ? ' if genero else ''
        q += 'AND b.fldIdMarca = ? ' if marca else ''

        params = [articulo_inicial,articulo_final,proveedor,temporada,subtemporada,familia,subfamilia,genero,marca]
        params = [param for param in params if param is not None]
        self.db.execute(q,params)
        self.db.commit()



    def generate_pandas_aboutyou(self,current_app):
        # Ejecutamos la consulta
        current_app.logger.info('Executing_query')
        res = self.db.execute(self.consulta).fetchall()
        #res = self.db.fetchall()

        data = []
        for row in res:
            data.append(tuple(row))
        if not data:
            current_app.logger.info("No data from database, exiting")
            return 'No hay datos para generar el csv'

        # Creamos el dataframe inicial del cual haremos las modificaciones necesarias
        current_app.logger.info('Creating_dataframe')
        self.df = pd.DataFrame(data,columns=column_names_sql).astype(str)
        if self.df.shape[0] > 5000:
            return 'Demasiados registros, por favor, acote la consulta'

        # Eliminamos las columnas que no nos interesan, las guardamos por si las necesitamos más adelante
        current_app.logger.info('Dropping_columns')
        fldaymaterial = self.df["fldaymaterial"]
        fldcantidadcaja = self.df["fldcantidadcaja"]
        self.df.drop(columns=["fldaymaterial", "fldcantidadcaja"], inplace=True)

        # Renombramos las columnas para que coincidan con los nombres del csv
        current_app.logger.info('Renaming_columns')
        self.df.columns = column_names_csv

        #Obtenemos todos los ids de articulos distintos
        current_app.logger.info('Getting_distinct_product_ids')
        articulos_color = self.df['a_prodnr'].unique().tolist()

        #Obtenemos los atributos de los articulos
        current_app.logger.info('Getting_attributes')
        atributos,articulos_atributos = Attribute.get_attributes_batch_products_relation_from_db(self.db,
                                    [(articulo_color.split('-')[0],articulo_color.split('-')[1]) for articulo_color in articulos_color],
                                                                                                 BulletPoint=False)
        current_app.logger.info(f"Atributos: {atributos}\n {articulos_atributos}")
        # Creamos las columnas de atributos
        current_app.logger.info('Creating_attributes_columns')
        for i,atributo_id in enumerate(atributos.keys()):
            self.df.insert(self.df.columns.get_loc('p_comp[care]')+i+1,f'p_comp[{atributos[atributo_id]}]','')

        # Rellenamos la columnas de atributos
        current_app.logger.info('Filling_attributes_columns')
        lista_productos = []
        for (producto_id, color), atributos1 in articulos_atributos.items():
            for articulo_id, categorias in atributos1.items():
                lista_productos.append([f"{producto_id}-{color}", articulo_id, categorias])  # Nota el cambio aquí

        df_productos = pd.DataFrame(lista_productos, columns=['a_prodnr', 'articulo_id', 'categorias'])

        # Pivote del DataFrame de productos relacionados
        df_pivot = df_productos.pivot_table(index='a_prodnr', columns='articulo_id', values='categorias',
                                            aggfunc='first')

        # Mapear articulo_id a articulo_name
        df_pivot.columns = [f'p_comp[{atributos[col]}]' for col in df_pivot.columns]

        # Fusionar con el DataFrame principal
        merged_df = pd.merge(self.df, df_pivot, on='a_prodnr', how='left', suffixes=('', '_pivote'))

        # Iterar a través de las columnas del DataFrame pivote
        for col in df_pivot.columns:
            if col in self.df.columns:
                # Reemplazar los valores de la columna original con los del DataFrame pivote
                merged_df[col] = merged_df[col + '_pivote']

        # Eliminar las columnas adicionales del pivote
        merged_df.drop([col + '_pivote' for col in df_pivot.columns], axis=1, inplace=True)

        self.df = merged_df


        # Añadir BulletPoints
        current_app.logger.info('Adding_bullet_points')
        atributos_bullet, articulos_atributos_bullet = Attribute.get_attributes_batch_products_relation_from_db(self.db,
                                                        [(articulo_color.split('-')[0],articulo_color.split('-')[1])
                                                         for articulo_color in articulos_color],
                                                        BulletPoint=True)
        current_app.logger.info(f"Bullets: {atributos_bullet}\n {articulos_atributos_bullet}")

        # Creamos las columnas de Bullets
        current_app.logger.info('Creating_bullets_columns')

        # Ver cuantas columnas bullet hay disponibles
        num_columnas_bullet = 0
        for col in self.df.columns:
            if col.startswith('p_bullet') and ((self.df[col] == '').all() or self.df[col].isna().all()):
                num_columnas_bullet += 1

        if num_columnas_bullet >= len(atributos_bullet.keys()):
            for i in range(len(atributos_bullet.keys())-num_columnas_bullet):
                self.df.insert(self.df.columns.get_loc('p_bullet[{14}]') + i + 1, f'p_bullet[{15+i}]', '')

        lista_productos = []
        for (producto_id, color), atributos1 in articulos_atributos_bullet.items():
            for articulo_id, categorias in atributos1.items():
                lista_productos.append([f"{producto_id}-{color}", articulo_id, categorias])

        df_productos2 = pd.DataFrame(lista_productos, columns=['a_prodnr', 'articulo_id', 'categorias'])

        # Pivote del DataFrame de productos relacionados
        df_pivot2 = df_productos2.pivot_table(index='a_prodnr', columns='articulo_id', values='categorias',
                                            aggfunc='first')

        # Fusionar con el DataFrame principal
        merged_df2 = pd.merge(self.df, df_pivot2, on='a_prodnr', how='left', suffixes=('', '_pivote'))

        # Identificar la primer columna bullet vacia
        primera_columna_bullet = None
        for col in self.df.columns:
            if 'p_bullet{' in col and (self.df[col] == '').all() or self.df[col].isna().all():
                primera_columna_bullet = col
                break

        # Iterar a través de las columnas del DataFrame pivote
        if primera_columna_bullet is not None:
            idx_vacia = int(primera_columna_bullet.split('{')[1].rstrip('}'))  # Extrae el índice de la columna vacía
            for col in df_pivot2.columns:
                col_destino = f"p_bullet{{{idx_vacia}}}"
                if col_destino in self.df.columns:
                    merged_df2[col_destino] = merged_df2[col]
                    merged_df2.drop(columns=[col], inplace=True)
                    idx_vacia += 1

        self.df = merged_df2

        # Eliminar saltos de línea en todas las celdas
        current_app.logger.info('Deleting_line_breaks')
        self.df.replace(to_replace=[r"\r", r"\n"], value=[" ", " "], regex=True, inplace=True)

        # Traducimos las columnas necesarias
        current_app.logger.info('Translating_columns')
        for col in ["p_cluster{0}", "p_name", "p_text", "p_tag[gender]"]:
            data_trans = [{'l_ori':'ES','l_dest':'EN','text_ori':text} for text in self.df[col].unique()]
            print(col,"data_trans got", len(data_trans))
            self.__translate_batch(data_trans)
            self.df[col] = self.df[col].apply(lambda x: self.__translate(x, 'EN',l_ori='ES'))

        # La 5a y 6a son iguales a la 4a
        current_app.logger.info('Duplicating_columns')
        self.df['p_name_keyword'] = self.df['p_name']
        self.df['p_name_proper'] = self.df['p_name']

        # La decimotercera columna es la concatenación de la tercera y la decimotercera
        current_app.logger.info('Concatenating_columns')
        self.df['p_tag[sizeGrid]'] = self.df['p_cluster{0}'] + self.df['p_tag[sizeGrid]']

        # La decimooctava tiene que duplicarse y tienen que estar una detrás de otra
        #### Como la consulta de SQL ya duplica la columna, no es necesario hacer nada

        # Transformamos la columna peso a kilogramos
        current_app.logger.info('Transforming_weight_column')
        self.df['a_weight'] = self.df['a_weight'].apply(self.__transformar_peso)

        # Creamos las columnas de imagenes de producto que necesitamos duplicar
        current_app.logger.info('Creating_image_columns')
        for i in range(0,8):
            self.df[f"p_media[ayimage]{{{i}}}"] = self.df[f"a_media[image]{{{i}}}"]

        #Eliminamos las de a_media porque no deben estar en AboutYou
        current_app.logger.info('Dropping_image_columns')
        self.df.drop(columns=[f"a_media[image]{{{i}}}" for i in range(0,8)],inplace=True)

        # Creamos las columnas traducidas en alemán, solo algunas, no todas
        # Tenemos que añadir al final las columnas 3,4,5,6,7,8,9,10,11,12,13,18,19, de la 98 a la 112 y de la 113 a la 119
        # Creamos las columnas traducidas en alemán
        german_columns = ["p_cluster{0}", "p_name"]
        german_columns2 = ["p_comp[composition]",
                          "p_comp[care]"]
        german_columns2 += [col for col in df_pivot.columns if col.startswith('p_comp[')]
        german_columns2 += ["p_tag[gender]", "p_tag[ageGroup]", "p_tag[season]","p_tag[sizeGrid]"]
        # Traducimos y añadimos las columnas al final
        current_app.logger.info('Translating_columns_to_german')
        for col in german_columns:
            data_trans = [{'l_ori':'ES','l_dest':'DE','text_ori':text} for text in self.df[col].unique()]
            self.__translate_batch(data_trans)
            self.df[f'<de>{col}'] = self.df[col].apply(lambda x: self.__translate(x, 'DE',l_ori='ES'))
        self.df['<de>p_name_keyword'] = self.df['<de>p_name']
        self.df['<de>p_name_proper'] = self.df['<de>p_name']
        data_trans = [{'l_ori':'ES','l_dest':'DE','text_ori':text} for text in self.df['p_text'].unique()]
        self.__translate_batch(data_trans)
        self.df['<de>p_text'] = self.df['p_text'].apply(lambda x: self.__translate(x, 'DE',l_ori='ES'))

        for col in german_columns2:
            data_trans = [{'l_ori':'ES','l_dest':'DE','text_ori':text} for text in self.df[col].unique()]
            self.__translate_batch(data_trans)
            self.df[f'<de>{col}'] = self.df[col].apply(lambda x: self.__translate(x, 'DE',l_ori='EN'))

        self.df['<de>a_comp[size]'] = self.df['a_comp[size]']
        self.df["<de>a_comp[brandcolour]"] = self.df["a_comp[brandcolour]"].apply(lambda x: self.__translate(x, 'DE',l_ori='EN'))
        self.df["<de>a_comp[colour]"] = self.df["<de>a_comp[brandcolour]"]

        #Only for about you
        self.df['<de>p_comp[composition]'] = 'Obermaterial: '  + self.df['<de>p_comp[composition]']

        max_index = 15 - (int(primera_columna_bullet.split('{')[1].rstrip('}')) + len(atributos_bullet.keys()))
        max_index = 15 + abs(max_index) if max_index < 0 else 15
        for col in [f'p_bullet{{{i}}}' for i in range(0, max_index)] + [f'p_comp[{comp}]' for comp in
                                                                              ['Faux_fur_collar_material',
                                                                               'filling', 'inner_jacket_lining',
                                                                               'inner_jacket_padding',
                                                                               'insertMaterial', 'lining',
                                                                               'upper_material_sleeves']]:
            self.df[f'<de>{col}'] = self.df[col].apply(lambda x: self.__translate(x, 'DE',l_ori='EN'))

        current_app.logger.info('Applying visual final changes')
        # Aplicamos los cambios visuales finales
        self.df['p_name'] = self.df['p_name'].str.capitalize()
        self.df['p_name_keyword'] = self.df['p_name_keyword'].str.capitalize()
        self.df['p_name_proper'] = self.df['p_name_proper'].str.capitalize()

        self.df['<de>p_name'] = self.df['<de>p_name'].str.capitalize()
        self.df['<de>p_name_keyword'] = self.df['<de>p_name_keyword'].str.capitalize()
        self.df['<de>p_name_proper'] = self.df['<de>p_name_proper'].str.capitalize()

        # Definimos la expresión regular para los patrones deseados
        regex_pattern = r'^(a_active\[za|a_vk\[za|a_vk_old\[za)'

        # Filtramos las columnas que coinciden con el patrón
        columns_to_drop = self.df.filter(regex=regex_pattern).columns

        # Eliminamos estas columnas del DataFrame
        self.df.drop(columns=columns_to_drop,inplace=True)

        # Reemplazar todas las variantes de NaN por np.nan
        variantes_nan = ['nan', 'Nan', 'NaN', 'NAN']
        for variante in variantes_nan:
            self.df.replace(variante, np.nan, inplace=True)

        #Eliminar todos los Na por ''
        self.df.fillna('',inplace=True)

        current_app.logger.info('Finished creating pandas')
        return self.df


    def generate_pandas_zalando(self,current_app):
        current_app.logger.info('Generating pandas')
        # Ejecutamos la consulta
        current_app.logger.info('Executing_query')
        self.db.execute(self.consulta)
        res = self.db.fetchall()

        data = []
        for row in res:
            data.append(tuple(row))
        if not data:
            current_app.logger.info("No data from database, exiting")
            return 'No hay datos para generar el csv'

        # Creamos el dataframe inicial del cual haremos las modificaciones necesarias
        current_app.logger.info('Creating_dataframe')
        self.df = pd.DataFrame(data, columns=column_names_sql).astype(str)
        if self.df.shape[0] > 5000:
            return 'Demasiados registros, por favor, acote la consulta'

        # Eliminamos las columnas que no nos interesan, las guardamos por si las necesitamos más adelante
        current_app.logger.info('Dropping_columns')
        fldaymaterial = self.df["fldaymaterial"]
        fldcantidadcaja = self.df["fldcantidadcaja"]
        self.df.drop(columns=["fldaymaterial", "fldcantidadcaja"], inplace=True)

        # Renombramos las columnas para que coincidan con los nombres del csv
        current_app.logger.info('Renaming_columns')
        self.df.columns = column_names_csv

        # Obtenemos todos los ids de articulos distintos
        current_app.logger.info('Getting_distinct_product_ids')
        articulos_color = self.df['a_prodnr'].unique().tolist()

        # Obtenemos los atributos de los articulos
        current_app.logger.info('Getting_attributes')
        atributos, articulos_atributos = Attribute.get_attributes_batch_products_relation_from_db(self.db,
                                                                                                  [(
                                                                                                   articulo_color.split(
                                                                                                       '-')[0],
                                                                                                   articulo_color.split(
                                                                                                       '-')[1]) for
                                                                                                   articulo_color in
                                                                                                   articulos_color],
                                                                                                  BulletPoint=False)
        current_app.logger.info(f"Atributos: {atributos}\n {articulos_atributos}")
        # Creamos las columnas de atributos
        current_app.logger.info('Creating_attributes_columns')
        for i, atributo_id in enumerate(atributos.keys()):
            self.df.insert(self.df.columns.get_loc('p_comp[care]') + i + 1, f'p_comp[{atributos[atributo_id]}]', '')

        # Rellenamos la columnas de atributos
        current_app.logger.info('Filling_attributes_columns')
        lista_productos = []
        for (producto_id, color), atributos1 in articulos_atributos.items():
            for articulo_id, categorias in atributos1.items():
                lista_productos.append([f"{producto_id}-{color}", articulo_id, categorias])  # Nota el cambio aquí

        df_productos = pd.DataFrame(lista_productos, columns=['a_prodnr', 'articulo_id', 'categorias'])

        # Pivote del DataFrame de productos relacionados
        df_pivot = df_productos.pivot_table(index='a_prodnr', columns='articulo_id', values='categorias',
                                            aggfunc='first')

        # Mapear articulo_id a articulo_name
        df_pivot.columns = [f'p_comp[{atributos[col]}]' for col in df_pivot.columns]

        # Fusionar con el DataFrame principal
        merged_df = pd.merge(self.df, df_pivot, on='a_prodnr', how='left', suffixes=('', '_pivote'))

        # Iterar a través de las columnas del DataFrame pivote
        for col in df_pivot.columns:
            if col in self.df.columns:
                # Reemplazar los valores de la columna original con los del DataFrame pivote
                merged_df[col] = merged_df[col + '_pivote']

        # Eliminar las columnas adicionales del pivote
        merged_df.drop([col + '_pivote' for col in df_pivot.columns], axis=1, inplace=True)

        self.df = merged_df

        # Añadir BulletPoints
        current_app.logger.info('Adding_bullet_points')
        atributos_bullet, articulos_atributos_bullet = Attribute.get_attributes_batch_products_relation_from_db(self.db,
                                                                                                                [(
                                                                                                                 articulo_color.split(
                                                                                                                     '-')[
                                                                                                                     0],
                                                                                                                 articulo_color.split(
                                                                                                                     '-')[
                                                                                                                     1])
                                                                                                                 for
                                                                                                                 articulo_color
                                                                                                                 in
                                                                                                                 articulos_color],
                                                                                                                BulletPoint=True)
        current_app.logger.info(f"Bullets: {atributos_bullet}\n {articulos_atributos_bullet}")

        # Creamos las columnas de Bullets
        current_app.logger.info('Creating_bullets_columns')

        # Ver cuantas columnas bullet hay disponibles
        num_columnas_bullet = 0
        for col in self.df.columns:
            if col.startswith('p_bullet') and ((self.df[col] == '').all() or self.df[col].isna().all()):
                num_columnas_bullet += 1

        if num_columnas_bullet >= len(atributos_bullet.keys()):
            for i in range(len(atributos_bullet.keys()) - num_columnas_bullet):
                self.df.insert(self.df.columns.get_loc('p_bullet[{14}]') + i + 1, f'p_bullet[{15 + i}]', '')

        lista_productos = []
        for (producto_id, color), atributos1 in articulos_atributos_bullet.items():
            for articulo_id, categorias in atributos1.items():
                lista_productos.append([f"{producto_id}-{color}", articulo_id, categorias])

        df_productos2 = pd.DataFrame(lista_productos, columns=['a_prodnr', 'articulo_id', 'categorias'])

        # Pivote del DataFrame de productos relacionados
        df_pivot2 = df_productos2.pivot_table(index='a_prodnr', columns='articulo_id', values='categorias',
                                              aggfunc='first')

        # Fusionar con el DataFrame principal
        merged_df2 = pd.merge(self.df, df_pivot2, on='a_prodnr', how='left', suffixes=('', '_pivote'))

        # Identificar la primer columna bullet vacia
        primera_columna_bullet = None
        for col in self.df.columns:
            if 'p_bullet{' in col and (self.df[col] == '').all() or self.df[col].isna().all():
                primera_columna_bullet = col
                break

        # Iterar a través de las columnas del DataFrame pivote
        if primera_columna_bullet is not None:
            idx_vacia = int(primera_columna_bullet.split('{')[1].rstrip('}'))  # Extrae el índice de la columna vacía
            for col in df_pivot2.columns:
                col_destino = f"p_bullet{{{idx_vacia}}}"
                if col_destino in self.df.columns:
                    merged_df2[col_destino] = merged_df2[col]
                    merged_df2.drop(columns=[col], inplace=True)
                    idx_vacia += 1

        self.df = merged_df2

        # Eliminar saltos de línea en todas las celdas
        current_app.logger.info('Deleting_line_breaks')
        self.df.replace(to_replace=[r"\r", r"\n"], value=[" ", " "], regex=True, inplace=True)

        # Traducimos las columnas necesarias
        current_app.logger.info('Translating_columns')
        for col in ["p_cluster{0}", "p_name", "p_text", "p_tag[gender]"]:
            self.df[col] = self.df[col].apply(lambda x: self.__translate(x, 'EN', l_ori='ES'))

        # La 5a y 6a son iguales a la 4a
        current_app.logger.info('Duplicating_columns')
        self.df['p_name_keyword'] = self.df['p_name']
        self.df['p_name_proper'] = self.df['p_name']

        # La decimotercera columna es la concatenación de la tercera y la decimotercera
        current_app.logger.info('Concatenating_columns')
        self.df['p_tag[sizeGrid]'] = self.df['p_cluster{0}'] + self.df['p_tag[sizeGrid]']

        # La decimooctava tiene que duplicarse y tienen que estar una detrás de otra
        #### Como la consulta de SQL ya duplica la columna, no es necesario hacer nada

        # Transformamos la columna peso a kilogramos
        current_app.logger.info('Transforming_weight_column')
        self.df['a_weight'] = self.df['a_weight'].apply(self.__transformar_peso)

        '''
        SOLO PARA ABOUTYOU
        # Creamos las columnas de imagenes de producto que necesitamos duplicar
        current_app.logger.info('Creating_image_columns')
        for i in range(0, 8):
            self.df[f"p_media[ayimage]{{{i}}}"] = self.df[f"a_media[image]{{{i}}}"]
        '''

        #Eliminar columna de precios no correspondiente
        self.df.drop(columns=['a_active[ayfd]','a_vk[ayfd]','a_vk_old[ayfd]'],inplace=True)

        # Reemplazar todas las variantes de NaN por np.nan
        variantes_nan = ['nan', 'Nan', 'NaN', 'NAN']
        for variante in variantes_nan:
            self.df.replace(variante, np.nan, inplace=True)

        # Eliminar todos los Na por ''
        self.df.fillna('', inplace=True)

        current_app.logger.info('Finishing pandas')
        return self.df

    def export_products_pim_attributes(self,articulo_inicial=None,articulo_final=None,proveedor=None,temporada=None,subtemporada=None,
                       familia=None,subfamilia=None,genero=None,marca=None):
        self.carga_temporal(articulo_inicial,articulo_final,proveedor,temporada,subtemporada,familia,subfamilia,genero,marca)
        products_color_familia = self.db.execute("select a.fldIdArticulo, a.fldIdColor, b.fldIdFamilia from zzTempArticuloColor a, tbdarticulos0 b where a.fldIdArticulo = b.fldIdArticulo").fetchall()
        products_color_familia = list(set(tuple(product) for product in products_color_familia))

        families_ids = self.get_product_families([product[0] for product in products_color_familia])
        attributes_ids_names = self.db.execute("select a.fldIdAtributo,b.fldnombre, a.fldIdFamilia,b.fldmultiseleccionable from tbdatributosfamilias a, tbdatributos b where a.fldidatributo = b.fldidatributo and a.fldIdFamilia in ({})".format(','.join(['?' for _ in families_ids])),families_ids).fetchall()
        attributes_families = {attr[0]:[attr1[2] for attr1 in attributes_ids_names if attr1[0] == attr[0]] for attr in attributes_ids_names}
        attributes_ids_names = list(set([tuple((attribute[0],attribute[1],attribute[3])) for attribute in attributes_ids_names]))

        cols = ["fldIdArticulo","fldIdColor"]+[str(attribute[0])+'-'+ str(attribute[1])+'-'+['M' if attribute[2] else 'NM'][0] for attribute in attributes_ids_names]
        filas = []
        for product,color,idfamilia in products_color_familia:
            fila = {'fldIdArticulo':product,'fldIdColor':color}
            for idatributo,n_atributo,multi in attributes_ids_names:
                if idfamilia in attributes_families[idatributo]:
                    cats = Attribute.get_categories_by_product_color(self.db,product,color,idatributo)
                    if cats:
                        fila[str(idatributo)+'-'+str(n_atributo)+'-'+['M' if multi else 'NM'][0]] = ';'.join(cats)
                        continue
                    fila[str(idatributo)+'-'+str(n_atributo)+'-'+['M' if multi else 'NM'][0]] = ''
                    continue
                fila[str(idatributo)+'-'+str(n_atributo)+'-'+['M' if multi else 'NM'][0]] = '__NONE__'
                continue

            filas.append(fila)

        final = pd.DataFrame(filas,columns=cols)

        return final







    @staticmethod
    def send_ftp_file(file,host,username,password,remote_path,port:int=22):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host,username=username,password=password,port=port)
        sftp = ssh.open_sftp()

        sftp.put(file,remote_path)

        file = sftp.get(remote_path,file)
        if not file:
            return False

        sftp.close()
        ssh.close()
        return True

