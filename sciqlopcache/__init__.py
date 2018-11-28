from pyramid.config import Configurator
from .amda import CachedAMDA

__amda__ = None

def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)
    global __amda__
    __amda__ = CachedAMDA(data_folder=settings.get('amda_cache_folder','/tmp/amdacache'))
    config.include('pyramid_jinja2')
    config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_route('home', '/')
    config.add_route('auth', '/php/rest/auth.php')
    config.add_route('getParameter', '/php/rest/getParameter.php')
    config.add_route('data', 'data/*file')
    config.scan()
    return config.make_wsgi_app()
