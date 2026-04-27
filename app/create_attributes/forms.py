import json

class CreateAttributeForm():
    def __init__(self,form:json):
        self.name = form.get('name')
        self.description = form.get('description')
        self.categories = form.get('categories')
        self.ismultiselectable = form.get('ismultiselectable')
        self.associated_families = form.get('associated_families')
        self.associated_marketplaces = form.get('associated_marketplaces')
        self.BulletPoint = form.get('BulletPoint')

    def get_name(self):
        return self.name
    def get_description(self):
        return self.description
    def get_categories(self):
        return self.categories
    def get_ismultiselectable(self):
        return self.ismultiselectable
    def get_associated_families(self):
        return self.associated_families
    def get_associated_marketplaces(self):
        return self.associated_marketplaces
    def get_BulletPoint(self):
        return self.BulletPoint


    def __str__(self) -> str:
        return f"name: {self.get_name()} \n description: {self.get_description()} \n categories: {self.get_categories()} \n " \
               f"ismultiselectable: {self.get_ismultiselectable()} \n families: {self.get_associated_families()} \n markets: {self.get_associated_marketplaces()} \n"

