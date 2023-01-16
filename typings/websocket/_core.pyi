"""
This type stub file was generated by pyright.
"""

from ._abnf import *
from ._exceptions import *
from ._handshake import *
from ._http import *
from ._logging import *
from ._socket import *
from ._ssl_compat import *
from ._utils import *

__all__ = ['WebSocket', 'create_connection']
class WebSocket:
    """
    Low level WebSocket interface.

    This class is based on the WebSocket protocol `draft-hixie-thewebsocketprotocol-76 <http://tools.ietf.org/html/draft-hixie-thewebsocketprotocol-76>`_

    We can connect to the websocket server and send/receive data.
    The following example is an echo client.

    >>> import websocket
    >>> ws = websocket.WebSocket()
    >>> ws.connect("ws://echo.websocket.events")
    >>> ws.recv()
    'echo.websocket.events sponsored by Lob.com'
    >>> ws.send("Hello, Server")
    19
    >>> ws.recv()
    'Hello, Server'
    >>> ws.close()

    Parameters
    ----------
    get_mask_key: func
        A callable function to get new mask keys, see the
        WebSocket.set_mask_key's docstring for more information.
    sockopt: tuple
        Values for socket.setsockopt.
        sockopt must be tuple and each element is argument of sock.setsockopt.
    sslopt: dict
        Optional dict object for ssl socket options. See FAQ for details.
    fire_cont_frame: bool
        Fire recv event for each cont frame. Default is False.
    enable_multithread: bool
        If set to True, lock send method.
    skip_utf8_validation: bool
        Skip utf8 validation.
    """
    def __init__(self, get_mask_key=..., sockopt=..., sslopt=..., fire_cont_frame=..., enable_multithread=..., skip_utf8_validation=..., **_) -> None:
        """
        Initialize WebSocket object.

        Parameters
        ----------
        sslopt: dict
            Optional dict object for ssl socket options. See FAQ for details.
        """
        ...
    
    def __iter__(self): # -> Generator[Unknown | Literal[''], None, None]:
        """
        Allow iteration over websocket, implying sequential `recv` executions.
        """
        ...
    
    def __next__(self): # -> Literal['']:
        ...
    
    def next(self): # -> Literal['']:
        ...
    
    def fileno(self):
        ...
    
    def set_mask_key(self, func): # -> None:
        """
        Set function to create mask key. You can customize mask key generator.
        Mainly, this is for testing purpose.

        Parameters
        ----------
        func: func
            callable object. the func takes 1 argument as integer.
            The argument means length of mask key.
            This func must return string(byte array),
            which length is argument specified.
        """
        ...
    
    def gettimeout(self): # -> None:
        """
        Get the websocket timeout (in seconds) as an int or float

        Returns
        ----------
        timeout: int or float
             returns timeout value (in seconds). This value could be either float/integer.
        """
        ...
    
    def settimeout(self, timeout): # -> None:
        """
        Set the timeout to the websocket.

        Parameters
        ----------
        timeout: int or float
            timeout time (in seconds). This value could be either float/integer.
        """
        ...
    
    timeout = ...
    def getsubprotocol(self): # -> None:
        """
        Get subprotocol
        """
        ...
    
    subprotocol = ...
    def getstatus(self): # -> int | None:
        """
        Get handshake status
        """
        ...
    
    status = ...
    def getheaders(self): # -> dict[Unknown, Unknown] | None:
        """
        Get handshake response header
        """
        ...
    
    def is_ssl(self): # -> bool:
        ...
    
    headers = ...
    def connect(self, url, **options):
        """
        Connect to url. url is websocket url scheme.
        ie. ws://host:port/resource
        You can customize using 'options'.
        If you set "header" list object, you can set your own custom header.

        >>> ws = WebSocket()
        >>> ws.connect("ws://echo.websocket.events",
                ...     header=["User-Agent: MyProgram",
                ...             "x-custom: header"])

        Parameters
        ----------
        header: list or dict
            Custom http header list or dict.
        cookie: str
            Cookie value.
        origin: str
            Custom origin url.
        connection: str
            Custom connection header value.
            Default value "Upgrade" set in _handshake.py
        suppress_origin: bool
            Suppress outputting origin header.
        host: str
            Custom host header string.
        timeout: int or float
            Socket timeout time. This value is an integer or float.
            If you set None for this value, it means "use default_timeout value"
        http_proxy_host: str
            HTTP proxy host name.
        http_proxy_port: str or int
            HTTP proxy port. Default is 80.
        http_no_proxy: list
            Whitelisted host names that don't use the proxy.
        http_proxy_auth: tuple
            HTTP proxy auth information. Tuple of username and password. Default is None.
        http_proxy_timeout: int or float
            HTTP proxy timeout, default is 60 sec as per python-socks.
        redirect_limit: int
            Number of redirects to follow.
        subprotocols: list
            List of available subprotocols. Default is None.
        socket: socket
            Pre-initialized stream socket.
        """
        ...
    
    def send(self, payload, opcode=...): # -> int:
        """
        Send the data as string.

        Parameters
        ----------
        payload: str
            Payload must be utf-8 string or unicode,
            If the opcode is OPCODE_TEXT.
            Otherwise, it must be string(byte array).
        opcode: int
            Operation code (opcode) to send.
        """
        ...
    
    def send_frame(self, frame): # -> int:
        """
        Send the data frame.

        >>> ws = create_connection("ws://echo.websocket.events")
        >>> frame = ABNF.create_frame("Hello", ABNF.OPCODE_TEXT)
        >>> ws.send_frame(frame)
        >>> cont_frame = ABNF.create_frame("My name is ", ABNF.OPCODE_CONT, 0)
        >>> ws.send_frame(frame)
        >>> cont_frame = ABNF.create_frame("Foo Bar", ABNF.OPCODE_CONT, 1)
        >>> ws.send_frame(frame)

        Parameters
        ----------
        frame: ABNF frame
            frame data created by ABNF.create_frame
        """
        ...
    
    def send_binary(self, payload): # -> int:
        """
        Send a binary message (OPCODE_BINARY).

        Parameters
        ----------
        payload: bytes
            payload of message to send.
        """
        ...
    
    def ping(self, payload=...): # -> None:
        """
        Send ping data.

        Parameters
        ----------
        payload: str
            data payload to send server.
        """
        ...
    
    def pong(self, payload=...): # -> None:
        """
        Send pong data.

        Parameters
        ----------
        payload: str
            data payload to send server.
        """
        ...
    
    def recv(self): # -> str | bytes:
        """
        Receive string data(byte array) from the server.

        Returns
        ----------
        data: string (byte array) value.
        """
        ...
    
    def recv_data(self, control_frame=...): # -> tuple[Unknown, Unknown]:
        """
        Receive data with operation code.

        Parameters
        ----------
        control_frame: bool
            a boolean flag indicating whether to return control frame
            data, defaults to False

        Returns
        -------
        opcode, frame.data: tuple
            tuple of operation code and string(byte array) value.
        """
        ...
    
    def recv_data_frame(self, control_frame=...):
        """
        Receive data with operation code.

        If a valid ping message is received, a pong response is sent.

        Parameters
        ----------
        control_frame: bool
            a boolean flag indicating whether to return control frame
            data, defaults to False

        Returns
        -------
        frame.opcode, frame: tuple
            tuple of operation code and string(byte array) value.
        """
        ...
    
    def recv_frame(self): # -> ABNF:
        """
        Receive data as frame from server.

        Returns
        -------
        self.frame_buffer.recv_frame(): ABNF frame object
        """
        ...
    
    def send_close(self, status=..., reason=...): # -> None:
        """
        Send close data to the server.

        Parameters
        ----------
        status: int
            Status code to send. See STATUS_XXX.
        reason: str or bytes
            The reason to close. This must be string or UTF-8 bytes.
        """
        ...
    
    def close(self, status=..., reason=..., timeout=...):
        """
        Close Websocket object

        Parameters
        ----------
        status: int
            Status code to send. See STATUS_XXX.
        reason: bytes
            The reason to close in UTF-8.
        timeout: int or float
            Timeout until receive a close frame.
            If None, it will wait forever until receive a close frame.
        """
        ...
    
    def abort(self): # -> None:
        """
        Low-level asynchronous abort, wakes up other threads that are waiting in recv_*
        """
        ...
    
    def shutdown(self): # -> None:
        """
        close socket, immediately.
        """
        ...
    


def create_connection(url, timeout=..., class_=..., **options): # -> WebSocket:
    """
    Connect to url and return websocket object.

    Connect to url and return the WebSocket object.
    Passing optional timeout parameter will set the timeout on the socket.
    If no timeout is supplied,
    the global default timeout setting returned by getdefaulttimeout() is used.
    You can customize using 'options'.
    If you set "header" list object, you can set your own custom header.

    >>> conn = create_connection("ws://echo.websocket.events",
         ...     header=["User-Agent: MyProgram",
         ...             "x-custom: header"])

    Parameters
    ----------
    class_: class
        class to instantiate when creating the connection. It has to implement
        settimeout and connect. It's __init__ should be compatible with
        WebSocket.__init__, i.e. accept all of it's kwargs.
    header: list or dict
        custom http header list or dict.
    cookie: str
        Cookie value.
    origin: str
        custom origin url.
    suppress_origin: bool
        suppress outputting origin header.
    host: str
        custom host header string.
    timeout: int or float
        socket timeout time. This value could be either float/integer.
        If set to None, it uses the default_timeout value.
    http_proxy_host: str
        HTTP proxy host name.
    http_proxy_port: str or int
        HTTP proxy port. If not set, set to 80.
    http_no_proxy: list
        Whitelisted host names that don't use the proxy.
    http_proxy_auth: tuple
        HTTP proxy auth information. tuple of username and password. Default is None.
    http_proxy_timeout: int or float
        HTTP proxy timeout, default is 60 sec as per python-socks.
    enable_multithread: bool
        Enable lock for multithread.
    redirect_limit: int
        Number of redirects to follow.
    sockopt: tuple
        Values for socket.setsockopt.
        sockopt must be a tuple and each element is an argument of sock.setsockopt.
    sslopt: dict
        Optional dict object for ssl socket options. See FAQ for details.
    subprotocols: list
        List of available subprotocols. Default is None.
    skip_utf8_validation: bool
        Skip utf8 validation.
    socket: socket
        Pre-initialized stream socket.
    """
    ...
