from ..config import Config
from .ipc_proto import IPCEndpointSpecification
from .tcp_proto import TCPEndpointSpecification


if Config.socket.protocol == IPCEndpointSpecification.ENDPOINT_PREFIX:
    EndpointSpecification = IPCEndpointSpecification
elif Config.socket.protocol == TCPEndpointSpecification.ENDPOINT_PREFIX:
    EndpointSpecification = TCPEndpointSpecification
else:
    err = f"{Config.socket.protocol} is not a valid transport protocol."
    raise ValueError(err)
