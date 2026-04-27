from app import create_app

if __name__ == '__main__':
    from waitress import serve
    serve(create_app(),host='0.0.0.0',port=2026)
else:
    app = create_app()

