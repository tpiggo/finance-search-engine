from app.flask import create_app


def run_app():
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=False)


if __name__ == '__main__':
    run_app()
