from .model_db import Database
from .model_attribute import Attribute

class Product():
    def __init__(self,db:Database,id:str,price:str=None,description:str=None,idfamily:str=None,gender:str=None):
        self._db = db
        self._id = id
        self._price = price
        self._description = description
        self._idfamily = idfamily
        self._family = self.set_family()
        self._colors = []
        self._sizes = []
        self._size_group = None
        self._images = []
        self._gender = gender

        self._attributes = []

    def remove_attribute(self,attribute:Attribute):
        attribute.set_categories()
        for cat in attribute.get_categories():
            attribute.deselect_category(category=cat,productid=self._id)

    def get_colors(self):
        return self._colors

    def get_family(self):
        return self._family
    
    def get_familyid(self):
        return self._idfamily
        
    def set_family(self,f:str=None):
        if f:
            self._family = f
        else:
            q = "select flddescripcion from tbdfamilias where fldidfamilia = ? "
            if self._idfamily:
                self._db.execute(q,[self._idfamily])
                res = self._db.fetchone()
                self._family = res[0] if res else None
            else:
                q = "select fldidfamilia, flddescripcion from tbdfamilias where fldidfamilia = (select top 1 fldidfamilia from tbdarticulos0 where fldidarticulo = ?)"
                self._db.execute(q,[self._id])
                res = self._db.fetchone()
                self._family = res[1] if res else None
                self._idfamily = res[0] if res else None
                
    def set_gender(self,g:str=None):
        if g:
            self._gender = g
        else:
            q = "select fldidsexo from tbdarticulos5 where fldidarticulo = ?"
            self._db.execute(q,[self._id])
            res = self._db.fetchone()
            self._gender = res[0] if res else None


    def set_attributes(self,attributes:list[Attribute]=None):
        if self._attributes:
            self._attributes = attributes
        else:
            self._attributes = Attribute.get_attributes_by_product(self._db,self._id)

    def get_attributes(self) -> list[Attribute]:
        return self._attributes



    def set_description(self,n:str=None):
        if n:
            self._description = n
        else:
            q = f"select flddescripcion from tbdarticulos0 where fldidarticulo = ?"
            self._db.execute(q,[self._id])
            self._description = self._db.fetchone()[0]

    def set_price(self,n:str=None):
        self._price = '0.0'
        '''
        if n:
            self._price = n
        else:
            q = f"select fldprecio from tbdarticulos0 where fldidarticulo = ?"
            self._db.execute(q,[self._id])
            self._price = self._db.fetchone()[0]
            self._db.close_cursor()
        '''
    def set_colors(self):
        q = "select a.fldidcolor, b.flddescripcion from tbdarticuloscolor a, tbdcolores b where a.fldidarticulo = ? and a.fldidcolor = b.fldidcolor and a.fldfechabaja = '' "
        self._db.execute(q,[self._id])
        self._colors = [tuple(color) for color in self._db.fetchall()]

    def set_sizes(self):
        q = '''
            select b.flddescripcion,b.fldtalla1,b.fldtalla2,b.fldtalla3,b.fldtalla4,b.fldtalla5,b.fldtalla6,b.fldtalla7
            ,b.fldtalla8,b.fldtalla9,b.fldtalla10,b.fldtalla11,b.fldtalla12,b.fldtalla13,b.fldtalla14,b.fldtalla15 
            from tbdarticulos0 a, tbdgrupostallas b where a.fldidarticulo = ? and a.fldidgrupotallas = b.fldidgrupotallas
        '''
        self._db.execute(q,[self._id])
        res = self._db.fetchone()
        self._size_group = res[0]
        i = 1
        while res[i] and i < 16:
            self._sizes.append(res[i])
            i += 1

    def get_json(self):
        return {
            'id':self._id,
            'price':self._price,
            'description':self._description,
            'idfamily':self._idfamily,
            'family':self._family,
            'colors':self._colors,
            'sizes':self._sizes,
            'size_group':self._size_group,
            'images':self._images,
            'gender':self._gender,
            'attributes':self._attributes
        }

    @staticmethod
    def get_by_id(db:Database, id:str):
        if not db.exists('tbdarticulos0','fldidarticulo',id):
            return None
        prod = Product(db,id)
        prod.set_description()
        prod.set_family()
        prod.set_gender()
        prod.set_price()
        prod.set_colors()
        prod.set_sizes()
        prod.set_attributes()
        return prod
