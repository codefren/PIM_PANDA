from .model_db import Database

class Attribute():
    def __init__(self, db:Database, id:str, name:str=None, description:str=None, categories:list[str]=None, ismultiselectable:bool=None,
                 associated_marketplaces=None, associated_families:list[tuple[str]]=None,BulletPoint:bool=False):

        if categories is None:
            categories = []
        if associated_marketplaces is None:
            associated_marketplaces = []
        if associated_families is None:
            associated_families = []

        self._db = db
        self._id = id
        self._name = name
        self._description = description
        self._categories = list(set(categories))
        self._ismultiselectable = ismultiselectable
        self._associated_marketplaces = list(set(associated_marketplaces))
        self._associated_families = list(set(associated_families))
        self._BulletPoint = BulletPoint

        self._selected_categories = []
        self._candidates_categories = self._categories.copy()

        self._selected_color = None

    def is_product_associated(self,productid:str):
        q = 'select count(*) from tbdatributosarticuloscategorias where fldidarticulo = ? and fldidatributo = ?'
        self._db.execute(q,[productid,self._id])
        res = self._db.fetchone()[0]
        return res > 0

    def select_category(self,category:str):
        if category in self._candidates_categories:
            self._selected_categories.append(category)
            self._candidates_categories.remove(category)
            return self._selected_categories
        return self._selected_categories

    def select_categories(self,categories:list[str]=None,productid:str=None,colorid:str=None,colors:list[str]=None):
        if categories:
            for category in categories:
                self.select_category(category)
            return self._selected_categories

        elif productid:
            q = 'select fldcategoria from tbdatributosproductoscategorias ' \
                    'where fldidarticulo = ? and fldidatributo = ? and fldidcolor = ?'
            if colorid:
                self._db.execute(q,[productid,self._id,colorid])
                res = self._db.fetchall()
                for category in res:
                    self.select_category(category[0])
                return self._selected_categories
            
            if not colors:
                raise Exception('Colors parameters must be specified if colorid is None')
            all_colors_categories = []
            for color in colors:
                categories_res = set()
                self._db.execute(q,[productid,self._id,color])
                res = self._db.fetchall()
                for category in res:
                    categories_res.add(category[0])
                all_colors_categories.append(categories_res)
            intersect_categories = set.intersection(*all_colors_categories)
            for category in intersect_categories:
                 self.select_category(category)
            
        return self._selected_categories
    
    def get_selected_categories(self):
        return self._selected_categories

    def set_multiselectability(self,ismultiselectable:bool=None):
        if ismultiselectable:
            self._ismultiselectable = ismultiselectable
        else:
            q = 'select fldmultiseleccionable from tbdatributos where fldidatributo = ?'
            self._db.execute(q,[self._id])
            self._ismultiselectable = self._db.fetchone()[0]


    def set_name(self,name:str=None):
        if name:
            self._name = name
        else:
            q = 'select fldnombre from tbdatributos where fldidatributo = ?'
            self._db.execute(q,[self._id])
            self._name = self._db.fetchone()[0]


    def set_description(self,description:str=None):
        if description:
            self._description = description
        else:
            q = 'select flddescripcion from tbdatributos where fldidatributo = ?'
            self._db.execute(q,[self._id])
            self._description = self._db.fetchone()[0]


    def set_categories(self,categories:list[str]=None):
        if categories:
            self._categories = categories
        else:
            q = 'select fldcategoria from tbdatributoscategorias where fldidatributo = ?'
            self._db.execute(q,[self._id])
            self._categories = [category[0] for category in self._db.fetchall()]

        self._categories = list(set(self._categories))
        self._candidates_categories = self._categories.copy()

    def add_category(self,category:str):
        if category not in self._categories:
            self._categories.append(category)
            self._candidates_categories.append(category)

    def get_categories(self):
        return self._categories

    def set_associated_marketplaces(self,associated_marketplaces:list[str]=None):
        if associated_marketplaces:
            self._associated_marketplaces = associated_marketplaces
        else:
            q = 'select fldmarketplace from tbdatributosmarketplaces where fldidatributo = ?'
            self._db.execute(q,[self._id])
            self._associated_marketplaces = [marketplace[0] for marketplace in self._db.fetchall()]

        self._associated_marketplaces = list(set(self._associated_marketplaces))

    def add_marketplace(self,marketplace:str):
        if marketplace not in self._associated_marketplaces:
            self._associated_marketplaces.append(marketplace)


    def set_associated_families(self,associated_families:list=None):
        if associated_families:
            self._associated_families = list(set(associated_families))
        else:
            q = 'select a.fldidfamilia, b.flddescripcion from tbdatributosfamilias a ' \
                'join tbdfamilias b on a.fldidfamilia = b.fldidfamilia where fldidatributo = ?'
            self._db.execute(q,[self._id])
            self._associated_families = [(str(row[0]).strip(),str(row[1]).strip()) for row in self._db.fetchall()]

    def get_associated_families(self):
        return self._associated_families

    @staticmethod
    def process_bool(input):
        if type(input) is bool:
            return input
        if input in ('0',0,'False','false','FALSE',None,'NA','na','Na','None','none','NONE'):
            return False
        if input in ('1',1,'True','true','TRUE'):
            return True
        raise Exception(f'Input {input} is not a valid boolean value.')

    def set_BulletPoint(self,BulletPoint:bool=None):
        if BulletPoint:
            self._BulletPoint = BulletPoint
        else:
            q = 'select fldBulletPoint from tbdatributos where fldidatributo = ?'
            self._db.execute(q,[self._id])
            self._BulletPoint = Attribute.process_bool(self._db.fetchone()[0])

    def get_BulletPoint(self):
        return self._BulletPoint

    def set_selected_color(self,color:str):
        self._selected_color = str(color).strip().split(',')[0] if color else None

    def get_json_attribute(self):
        return {
            'id': str(self._id).strip(),
            'name': str(self._name).strip(),
            'description': str(self._description).strip(),
            'categories': [str(category).strip() for category in self._categories],
            'ismultiselectable': '1' if self._ismultiselectable else '0',
            'associated_marketplaces': [str(market).strip() for market in self._associated_marketplaces],
            'associated_families': [(str(family[0]).strip(),str(family[1]).strip()) for family in self._associated_families],
            'selected_categories': [str(category).strip() for category in self._selected_categories],
            'candidate_categories': [str(category).strip() for category in self._candidates_categories],
            'BulletPoint': self._BulletPoint
        }


    def update_attribute(self):
        if not self._name or not self._description or not self._categories or not self._associated_families:
            raise Exception('Attribute not ready to be saved. Inssuficient data.')

        # Update attribute, categories and marketplaces
        if self._db.exists('tbdatributos','fldidatributo',self._id):
            q = 'update tbdatributos set fldnombre = ?, flddescripcion = ?, fldmultiseleccionable = ?, fldBulletPoint = ? ' \
                'where fldidatributo = ?'
            self._db.execute(q, [self._name, self._description, self._description, self._ismultiselectable, self._BulletPoint,self._id])
            self._db.commit()
            
            q = "select fldidfamilia from tbdatributosfamilias where fldidatributo = ?"
            self._db.execute(1,[self._id])
            families_existent = []
            for row in self._db.fetchall(): families_existent.append(str(row[0]).lower().strip()) 

            q = 'insert into tbdatributosfamilias(fldidatributo,fldidfamilia) values(?,?)'
            for family in self._associated_families:
                if not str(family[0]).lower().strip() in families_existent:
                    self._db.execute(q,[self._id,family[0]])
                    self._db.commit()

            q = "select fldcategoria from tbdatributoscategorias where fldidatributo = ?"
            self._db.execute(q,[self._id])
            cats_existent = []
            for row in self._db.fetchall(): cats_existent.append(str(row[0]).lower().strip()) 

            for category in self._categories:
                if not str(category).lower().strip() in cats_existent:
                    q = 'insert into tbdatributoscategorias(fldcategoria,fldidatributo) values(?,?)'
                    self._db.execute(q,[category,self._id])
                    self._db.commit()
            
            q = "select fldmarketplace from tbdatributosmarketplaces where fldidatributo = ?"
            self._db.execute(q,[self._id])
            markets_existent = []
            for row in self._db.fetchall(): markets_existent.append(str(row[0]).lower().strip()) 

            for marketplace in self._associated_marketplaces:
                if not self._db.exists('tbdmarketplaces','fldmarketplace',marketplace):
                    q = 'insert into tbdmarketplaces values(?)'
                    self._db.execute(q,[marketplace])

                if not str(marketplace).lower().strip() in markets_existent:
                    q = 'insert into tbdatributosmarketplaces(fldmarketplace,fldidatributo) values(?,?)'
                    self._db.execute(q,[marketplace,self._id])
                    self._db.commit()
            return True

        raise Exception('Attribute not found in database.')


    def save_overwrite_attribute(self) -> bool:
        if not self._name or not self._description or not self._categories or not self._associated_families:
            raise Exception('Attribute not ready to be saved. Insuficient data.')

        # Update or insert attribute
        if self._db.exists('tbdatributos','fldidatributo',self._id):
            q = 'update tbdatributos set fldnombre = ?, flddescripcion = ?, fldmultiseleccionable = ?, fldBulletPoint = ? ' \
                'where fldidatributo = ?'
            self._db.execute(q,[self._name,self._description,self._ismultiselectable,self._BulletPoint, self._id])
            self._db.commit()
        else:
            q = 'insert into tbdatributos(fldidatributo,fldnombre,flddescripcion,fldmultiseleccionable,fldBulletPoint)' \
                ' values(?,?,?,?,?)'
            self._db.execute(q,[self._id,self._name,self._description,self._ismultiselectable.self._BulletPoint])
            self._db.commit()

        q = 'delete from tbdatributosfamilias where fldidatributo = ?'
        self._db.execute(q,[self._id])
        self._db.commit()
        q = 'insert into tbdatributosfamilias(fldidatributo,fldidfamilia) values(?,?)'
        for family in self._associated_families:
            self._db.execute(q,[self._id,family[0]])
            self._db.commit()

        # overwrite categories
        q = 'delete from tbdatributoscategorias where fldidatributo = ?'
        self._db.execute(q,[self._id])
        added = []
        for category in self._categories:
            if category not in added:
                q = 'insert into tbdatributoscategorias(fldcategoria,fldidatributo) values(?,?)'
                self._db.execute(q,[category,self._id])
                added.append(category)
        self._db.commit()

        # overwrite associated marketplaces
        q = 'delete from tbdatributosmarketplaces where fldidatributo = ?'
        self._db.execute(q,[self._id])
        added = []
        for marketplace in self._associated_marketplaces:
            if marketplace not in added:
                q = 'insert into tbdatributosmarketplaces(fldmarketplace,fldidatributo) values(?,?)'
                self._db.execute(q,[marketplace,self._id])
                added.append(marketplace)
        self._db.commit()

        return True


    def delete_attribute(self):
        if self._db.exists('tbdatributos', 'fldidatributo', self._id):
            q = 'delete from tbdatributosmarketplaces where fldidatributo = ?'
            self._db.execute(q, [self._id])

            q = 'delete from tbdatributoscategorias where fldidatributo = ?'
            self._db.execute(q, [self._id])

            q = 'delete from tbdatributosproductoscategorias where fldidatributo = ?'
            self._db.execute(q, [self._id])

            q = 'delete from tbdatributosfamilias where fldidatributo = ?'
            self._db.execute(q, [self._id])

            q = 'delete from tbdatributos where fldidatributo = ?'
            self._db.execute(q, [self._id])

            self._db.commit()
            return True
        raise Exception('Attribute not found in database.')

    def get_id(self) -> str:
        return self._id


    @staticmethod
    def obtain_valid_id(db:Database) -> str:
        q = 'select max(fldidatributo) from tbdatributos'
        db.execute(q)
        res = db.fetchone()
        if res:
            max_id = res[0]
            if max_id:
                return str(int(max_id) + 1).zfill(8)
            else:
                return '00000001'

        raise Exception('No valid id found for new attribute.')

    @staticmethod
    def get_attribute_by_id(db:Database,ide:str):
        if not db.exists('tbdatributos','fldidatributo',ide):
            return None
        attribute = Attribute(db,ide)
        attribute.set_name()
        attribute.set_description()
        attribute.set_categories()
        attribute.set_multiselectability()
        attribute.set_associated_marketplaces()
        attribute.set_associated_families()
        attribute.set_BulletPoint()
        return attribute

    def deselect_category(self,category:str,productid:str):
        if not self._db.exists('tbdatributos','fldidatributo',self._id) or \
                not self._db.exists('tbdarticulos0','fldidarticulo',productid):
            raise Exception('Attribute or product not found in database.')

        if category not in self._categories:
            self.set_categories()
            if category not in self._categories:
                raise Exception('Category not found in attribute.')
        if not self._selected_color:
            q = 'delete from tbdatributosproductoscategorias where fldidatributo = ? and fldidarticulo = ? and fldcategoria = ?'
            self._db.execute(q,[self._id,productid,category])
            self._db.commit()
        else:
            q = 'delete from tbdatributosproductoscategorias where fldidatributo = ? and fldidarticulo = ? and fldcategoria = ? and fldidcolor = ?'
            self._db.execute(q,[self._id,productid,category,self._selected_color])
            self._db.commit()

        return True

    def select_category_product(self,category:str,productid:str,colors:list[tuple[str,str]]):
        if not self._db.exists('tbdatributos','fldidatributo',self._id) or \
                not self._db.exists('tbdarticulos0','fldidarticulo',productid):
            raise Exception('Attribute or product not found in database.')
        if not colors:
            return {'status':'error','message':f'No colors found for the productid: {productid}.'}
        if category not in self._categories:
            self.set_categories()
            if category not in self._categories:
                raise Exception('Category not found in attribute.')
        if not self._selected_color:
            for color in colors:
                q = "select fldcategoria from tbdatributosproductoscategorias where fldidatributo = ? and fldidarticulo = ? and fldidcolor = ?"
                self._db.execute(q,[self._id,productid,color[0]])
                if self._db.fetchone() and not self._ismultiselectable:
                    q = "delete from tbdatributosproductoscategorias where fldidatributo = ? and fldidarticulo = ? and fldidcolor = ?"
                    self._db.execute(q,[self._id,productid,color[0]])
                    q = 'insert into tbdatributosproductoscategorias(fldidatributo,fldidarticulo,fldcategoria,fldidcolor) values(?,?,?,?)'
                    self._db.execute(q,[self._id,productid,category,color[0]])
                    self._db.commit()
                else:
                    q = 'insert into tbdatributosproductoscategorias(fldidatributo,fldidarticulo,fldcategoria,fldidcolor) values(?,?,?,?)'
                    self._db.execute(q,[self._id,productid,category,color[0]])
                    self._db.commit()
        else:
            q = "select fldcategoria from tbdatributosproductoscategorias where fldidatributo = ? and fldidarticulo = ? and fldcategoria = ? and fldidcolor = ?"
            self._db.execute(q, [self._id, productid, category,self._selected_color])
            if self._db.fetchone():
                return True


            q = 'insert into tbdatributosproductoscategorias(fldidatributo,fldidarticulo,fldcategoria,fldidcolor) values(?,?,?,?)'
            self._db.execute(q, [self._id, productid, category,self._selected_color])
            self._db.commit()

        return True

    @staticmethod
    def get_all_attributes(db:Database):
        q = 'select fldidatributo from tbdatributos'
        db.execute(q)
        return [Attribute.get_attribute_by_id(db,attribute[0]) for attribute in db.fetchall()]


    @staticmethod
    def get_attributes_by_marketplace(db:Database,marketplace:str):
        q = 'select fldidatributo from tbdatributosmarketplaces where fldmarketplace = ?'
        db.execute(q,[marketplace])
        return [Attribute.get_attribute_by_id(db,attribute[0]) for attribute in db.fetchall()]


    @staticmethod
    def get_attributes_by_familyid(db:Database,family:str):
        q = 'select fldidatributo from tbdatributosfamilias where fldidfamilia = ?'
        db.execute(q,[family])
        return [Attribute.get_attribute_by_id(db,attribute[0]) for attribute in db.fetchall()]
    
    @staticmethod
    def get_attributes_by_familyname(db:Database,family:str):
        q = "select fldidfamilia from tbdfamilias where LOWER(flddescripcion) = ?"
        db.execute(q,[str(family).lower()])
        res = db.fetchall()
        attributes = []
        for row in res:
            attributes = attributes + Attribute.get_attributes_by_familyid(db=db,family=row[0])
        
            return attributes

    @staticmethod
    def get_attributes_by_category(db:Database,category:str):
        q = 'select fldidatributo from tbdatributoscategorias where fldcategoria = ?'
        db.execute(q,[category])
        return [Attribute.get_attribute_by_id(db,attribute[0]) for attribute in db.fetchall()]


    @staticmethod
    def get_attributes_by_name(db:Database,name:str):
        q = 'select fldidatributo from tbdatributos where fldnombre = ?'
        db.execute(q,[name])
        return [Attribute.get_attribute_by_id(db,attribute[0]) for attribute in db.fetchall()]


    @staticmethod
    def get_attributes_by_product(db:Database,product:str):
        q = 'select distinct fldidatributo from tbdatributosproductoscategorias where fldidarticulo = ?'
        db.execute(q,[product])
        res = db.fetchall()
        res = [Attribute.get_attribute_by_id(db,attribute[0]) for attribute in res]
        return res

    @staticmethod
    def get_categories_by_product_color(db:Database,productid:str,colorid:str,attributeid:str):
        q = 'select distinct fldcategoria from tbdatributosproductoscategorias where fldidarticulo = ? and fldidcolor = ? and fldidatributo = ?'
        db.execute(q,[productid, colorid, attributeid])
        res = db.fetchall()
        return [category[0] for category in res]


    @staticmethod
    def get_attributes_batch_products_relation_from_db(db:Database,products_color:list[tuple[str,str]],BulletPoint:bool=False):
        """
        Given a list of product ids and its colors, find all the attributes that belong to any of this products and save them, create
        another dict with the product id and color id and all the attributes ids that belong to it.
        :param db: Database to extract attributes and products
        :param products: A list of tuples of product id, color id
        :return: Two dictionaries, one with all the attributes and another with the products and their attributes ids and the selected categories
        """
        if not BulletPoint:
            q = '''
            select b.fldidarticulo,b.fldidcolor,a.fldidatributo,a.fldnombre, STUFF(
            (SELECT ', ' + ac.fldCategoria
             FROM tbdAtributosProductosCategorias ac
             WHERE ac.fldidarticulo = b.fldidarticulo AND ac.fldidcolor = b.fldidcolor AND ac.fldidatributo = b.fldidatributo
             FOR XML PATH('')), 1, 2, '') AS categorias
            from tbdatributos a
            join tbdAtributosProductosCategorias b on a.fldidatributo = b.fldIdAtributo
            where fldidarticulo = ? and fldidcolor = ? and (a.fldBulletPoint = 0 or a.fldbulletpoint is null)
            group by b.fldidarticulo,b.fldidcolor,b.fldidatributo,a.fldnombre, a.fldidatributo
            '''
        else:
            q = '''
            select b.fldidarticulo,b.fldidcolor,a.fldidatributo,a.fldnombre, STUFF(
            (SELECT ', ' + ac.fldCategoria
             FROM tbdAtributosProductosCategorias ac
             WHERE ac.fldidarticulo = b.fldidarticulo AND ac.fldidcolor = b.fldidcolor AND ac.fldidatributo = b.fldidatributo
             FOR XML PATH('')), 1, 2, '') AS categorias
            from tbdatributos a
            join tbdAtributosProductosCategorias b on a.fldidatributo = b.fldIdAtributo
            where fldidarticulo = ? and fldidcolor = ? and a.fldBulletPoint = 1
            group by b.fldidarticulo,b.fldidcolor,b.fldidatributo,a.fldnombre, a.fldidatributo
            '''
        products_relation = {}
        attributes = {}
        for prod,color in products_color:

            db.execute(q,[prod,color])
            res = db.fetchall()

            products_relation[(prod,color)] = {r[2]:r[4] for r in res} if not products_relation.get((prod,color),None) \
                else products_relation[(prod,color)].update({r[2]:r[4] for r in res if r[2] not in products_relation[(prod,color)].keys()})

            attributes.update({r[2]:r[3] for r in res if r[2] not in attributes.keys()})

        return attributes,products_relation

    @staticmethod
    def get_attributes_batch_products_relation(db:Database,products_color:list[tuple[str,str]]):
        """
        Given a list of product ids and its colors, find all the attributes that belong to any of this products and save them, create
        another dict with the product id and color id and all the attributes ids that belong to it.
        :param db: Database to extract attributes and products
        :param products: A list of tuples of product id, color id
        :return: Two dictionaries, one with all the attributes and another with the products and their attributes ids and the selected categories
        """
        attributes = {}
        prods = {}
        for product,color in products_color:
            attrs = Attribute.get_attributes_by_product_and_color(db,product,color)

            if not (product,color) in prods.keys():
                prods[(product,color)] = []

            for attr in attrs:
                if attr.get_id() not in attributes.keys():
                    attributes[attr.get_id()] = attr

                prods[(product,color)].append( (attr.get_id(),Attribute.get_categories_by_product_color(db,product,color,attr.get_id())) )

        return attributes,prods

    @staticmethod
    def get_attribute_by_product_and_all_colors(db:Database,product:str,colors:list[str]):
        attributes = []
        attributes_ids = []
        for color in colors:
            res = Attribute.get_attributes_by_product_and_color(db,product,color)
            attributes.append(set(res))
            attributes_ids.append(set(attr.get_id() for attr in res))
        intersection = set.intersection(*attributes_ids)
        result = []
        for st in attributes:
            for attr in st:
                if attr.get_id() in intersection:
                    result.append(attr)
        print(result)
        return result



    @staticmethod
    def get_attributes_by_product_and_color(db:Database,product:str,colorid:str):
        q = 'select distinct fldidatributo from tbdatributosproductoscategorias where fldidarticulo = ? and fldidcolor = ?'
        db.execute(q,[product,colorid])
        res = db.fetchall()
        res = [Attribute.get_attribute_by_id(db,attribute[0]) for attribute in res]
        return res

    @staticmethod
    def get_families(db:Database):
        q = 'select distinct fldidfamilia,flddescripcion from tbdfamilias'
        db.execute(q)
        res = db.fetchall()
        if not res:
            print('No families obtained')
            return []
        return [(family[0],family[1]) for family in res]

    @staticmethod
    def get_marketplaces(db:Database):
        q = 'select distinct fldmarketplace from tbdmarketplaces'
        db.execute(q)
        res = db.fetchall()
        if not res:
            print('No markets obtained')
            return []
        return [marketplace[0] for marketplace in res]

    @staticmethod
    def get_main_stats(db:Database):
        q = f"select count(*) from tbdatributos"
        db.execute(q)
        res = db.fetchone()
        n_atributes = res[0] if res else 0

        q = "select count(*) from tbdatributos where fldmultiseleccionable = 1"
        db.execute(q)
        res = db.fetchone()
        n_multiselectables = res[0] if res else 0

        q = "select count(*) from tbdatributoscategorias"
        db.execute(q)
        res = db.fetchone()
        n_categories = res[0] if res else 0

        q = "select count(*) from tbdmarketplaces"
        db.execute(q)
        res = db.fetchone()
        n_marketplaces = res[0] if res else 0

        q = "select count(distinct fldidatributo), count(distinct fldidarticulo),count(distinct fldidatributo)/count(distinct fldidarticulo) from tbdatributosproductoscategorias"
        db.execute(q)
        res = db.fetchone()
        n_asigned_products = res[1] if res else 0
        n_asigned_atributes = res[0] if res else 0
        n_asigned_atributes_per_product = res[2] if res else 0

        return {'n_atributes':n_atributes,'n_multiselectables':n_multiselectables,'n_categories':n_categories,
                'n_marketplaces':n_marketplaces,'n_asigned_products':n_asigned_products,
                'n_asigned_atributes':n_asigned_atributes,'n_asigned_atributes_per_product':n_asigned_atributes_per_product}
