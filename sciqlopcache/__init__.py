from pyramid.config import Configurator
from .cached_amda import CachedAMDA

import logging
log = logging.getLogger(__name__)


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)
    config.include('pyramid_jinja2')
    config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_route('home', '/')
    config.add_route('auth', '/php/rest/auth.php')
    config.add_route('getParameter', '/php/rest/getParameter.php')
    config.add_route('data', 'data/*file')
    config.scan()
    amda_cache_folder = settings.get('amda_cache_folder','/tmp/amdacache')
    log.debug(f'''amda_cache_folder is {amda_cache_folder}''')
    config.registry.amda = CachedAMDA(data_folder=amda_cache_folder)
    config.registry.tmp_files = []
    retval = config.make_wsgi_app()
    config.registry.amda._save()
    return retval
