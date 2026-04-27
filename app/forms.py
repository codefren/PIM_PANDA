import json


class LoginForm():
    def __init__(self, form:json):
        self.form = form
        self.username = form.get('usuario')
        self.password = form.get('password')

    def get_username(self):
        return self.username
    def get_password(self):
        return self.password
