import os
from tempfile import NamedTemporaryFile

from pyramid.view import view_config
from pyramid.response import Response, FileResponse
import uuid

@view_config(route_name='home', renderer='templates/mytemplate.jinja2')
def my_view(request):
    return {'project': 'sciqlopcache'}


@view_config(route_name='auth', renderer='json')
def auth(request):
    return Response(
        content_type="text/plain",
        body="{key}".format(key=uuid.uuid4())
    )

@view_config(route_name='getParameter', renderer='json')
def get_parameter(request):
    res = 4  # random data
    params = []
    for parameter in ("startTime", "stopTime", "parameterID"):
        value = request.params.get(parameter, None)
        if value is None:
            return Response(
                content_type="text/plain",
                body="Error: missing {name} parameter".format(name=parameter)
            )
        params.append(value)
    txt = request.registry.amda.get_parameter_as_txt(*params)
    with NamedTemporaryFile(delete=False, mode='w') as ofile:
        ofile.write(txt)
        return Response(
            content_type="text/plain",
            body='{{"success":true,"status":"done","dataFileURLs":"{host}/data/{result}"}}'.format(
                host=str(request.host_url),
                result=ofile.name)
        )


@view_config(route_name='data', renderer='json')
def data(request):
    datafile = '/'+'/'.join(request.matchdict['file'])
    if datafile != 'None' and os.path.exists(datafile):
        return FileResponse(datafile)
    else:
        return Response('Bad request.'+datafile)
