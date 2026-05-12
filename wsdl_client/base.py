from zeep import Client, Settings
from zeep.transports import Transport
import os
import requests
from .config import WSDL_URL

WSDL_LOAD_TIMEOUT_SECONDS = int(os.getenv("WSDL_LOAD_TIMEOUT_SECONDS", "10"))
WSDL_OPERATION_TIMEOUT_SECONDS = int(os.getenv("WSDL_OPERATION_TIMEOUT_SECONDS", "15"))


def _build_transport():
    session = requests.Session()
    return Transport(
        session=session,
        timeout=WSDL_LOAD_TIMEOUT_SECONDS,
        operation_timeout=WSDL_OPERATION_TIMEOUT_SECONDS,
    )


def get_soap_client(service_name=None):
    settings = Settings(strict=False, xml_huge_tree=True)
    transport = _build_transport()
    if service_name:
        return Client(WSDL_URL, port_name=service_name, settings=settings, transport=transport)
    return Client(WSDL_URL, settings=settings, transport=transport)

def find_client_with_operation(operation_name):
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        base_client = Client(WSDL_URL, settings=settings, transport=_build_transport())
        for service in base_client.wsdl.services.values():
            for port in service.ports.values():
                try:
                    if operation_name in port.binding.port_type.operations:
                        return Client(WSDL_URL, port_name=port.name,
                                      settings=settings, transport=_build_transport())
                except Exception:
                    continue
        return None
    except Exception:
        return None
