import asyncio
import aiohttp
import async_timeout
import atexit
import re
import json
from aiohttp_socks import SocksConnector
from .. import exception
from ..api import _methodurl, _which_pool, _fileurl, _guess_filename


_loop = asyncio.get_event_loop()

_pools = {
    'default': aiohttp.ClientSession(
                   connector=aiohttp.TCPConnector(limit=10),
                   loop=_loop)
}

_timeout = 30
_proxy = None  # (url, (username, password))
_proxy_auth = None

def set_proxy(url, auth=None):
    global _proxy
    global _proxy_auth
    if not url:
        _proxy = None
    else:
        type = url.split(':')[0];
        _proxy = (type, url, auth) if auth else (type, url,)

        _proxy_auth = aiohttp.BasicAuth
        if type != "http":
            if not auth:
                socks_conn = SocksConnector.from_url(url, rdns=True, limit=10)
            else:
                if len(auth) < 2:
                    auth += ('',)
                socks_conn = SocksConnector.from_url(url, rnds=True, username=auth[0],
                        password=auth[1], limit=10)

            _pools['default'] = aiohttp.ClientSession(connector=socks_conn, loop=_loop)

            _proxy_auth = {
                        "socks4": None, # as socks imped by aiohttp_socks and it handles auth internally
                        "socks5": None,
                }.get(type, None);
        



def _proxy_kwargs():
    if _proxy is None or len(_proxy) == 0 or\
        _proxy[0] == 'http':  # as currently only http support `proxy` and `proxy_auth` kwargs
        return {}
    try:
        kw = {'proxy': _proxy[1]}
        if len(_proxy) >= 2 and _proxy_auth:
            kw['proxy_auth'] = _proxy_auth(*_proxy[2])
    except:
        raise RuntimeError("_proxy has invalid length")
    finally:
        return kw

async def _close_pools():
    global _pools
    for s in _pools.values():
        await s.close()

atexit.register(lambda: _loop.create_task(_close_pools()))  # have to wrap async function

def _create_onetime_pool():
    if _proxy and _proxy[0] != "http":
        if len(_proxy) > 2:  # auth
            auth = _proxy[2]
            socks_conn = SocksConnector.from_url(_prox[1], rdns=True, username=auth[0], 
                    password=auth[1], limit=1, force_close=True)
        else:
            socks_conn = SocksConnector(_proxy[1], rdns=True, limit=1, force_close=True);
        return aiohttp.ClientSession(connector=socks_conn, request_class=ProxyClientRequest, loop=_loop)
    else:
        return aiohttp.ClientSession(
               connector=aiohttp.TCPConnector(limit=1, force_close=True),
               loop=_loop)

def _default_timeout(req, **user_kw):
    return _timeout

def _compose_timeout(req, **user_kw):
    token, method, params, files = req

    if method == 'getUpdates' and params and 'timeout' in params:
        # Ensure HTTP timeout is longer than getUpdates timeout
        return params['timeout'] + _default_timeout(req, **user_kw)
    elif files:
        # Disable timeout if uploading files. For some reason, the larger the file,
        # the longer it takes for the server to respond (after upload is finished).
        # It is unclear how long timeout should be.
        return None
    else:
        return _default_timeout(req, **user_kw)

def _compose_data(req, **user_kw):
    token, method, params, files = req

    data = aiohttp.FormData()

    if params:
        for key,value in params.items():
            data.add_field(key, str(value))

    if files:
        for key,f in files.items():
            if isinstance(f, tuple):
                if len(f) == 2:
                    filename, fileobj = f
                else:
                    raise ValueError('Tuple must have exactly 2 elements: filename, fileobj')
            else:
                filename, fileobj = _guess_filename(f) or key, f

            data.add_field(key, fileobj, filename=filename)

    return data

def _transform(req, **user_kw):
    timeout = _compose_timeout(req, **user_kw)

    data = _compose_data(req, **user_kw)

    url = _methodurl(req, **user_kw)

    name = _which_pool(req, **user_kw)

    if name is None:
        session = _create_onetime_pool()
        cleanup = session.close  # one-time session: remember to close
    else:
        session = _pools[name]
        cleanup = None  # reuse: do not close

    kwargs = {'data':data}
    kwargs.update(user_kw)

    return session.post, (url,), kwargs, timeout, cleanup

async def _parse(response):
    try:
        data = await response.json()
        if data is None:
            raise ValueError()
    except (ValueError, json.JSONDecodeError, aiohttp.ClientResponseError):
        text = await response.text()
        raise exception.BadHTTPResponse(response.status, text, response)

    if data['ok']:
        return data['result']
    else:
        description, error_code = data['description'], data['error_code']

        # Look for specific error ...
        for e in exception.TelegramError.__subclasses__():
            n = len(e.DESCRIPTION_PATTERNS)
            if any(map(re.search, e.DESCRIPTION_PATTERNS, n*[description], n*[re.IGNORECASE])):
                raise e(description, error_code, data)

        # ... or raise generic error
        raise exception.TelegramError(description, error_code, data)

async def request(req, **user_kw):
    fn, args, kwargs, timeout, cleanup = _transform(req, **user_kw)

    kwargs.update(_proxy_kwargs())
    try:
        if timeout is None:
            async with fn(*args, **kwargs) as r:
                return await _parse(r)
        else:
            try:
                with async_timeout.timeout(timeout):
                    async with fn(*args, **kwargs) as r:
                        return await _parse(r)

            except asyncio.TimeoutError:
                raise exception.TelegramError('Response timeout', 504, {})

    except aiohttp.ClientConnectionError:
        raise exception.TelegramError('Connection Error', 400, {})

    finally:
        if cleanup:  # e.g. closing one-time session
            if asyncio.iscoroutinefunction(cleanup):
                await cleanup()
            else:
                cleanup()

def download(req):
    session = _create_onetime_pool()

    kwargs = {}
    kwargs.update(_proxy_kwargs())

    return session, session.get(_fileurl(req), timeout=_timeout, **kwargs)
    # Caller should close session after download is complete
