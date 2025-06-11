import logging
import pytest
pytestmark = pytest.mark.integration


cache = {}


def test_client_initialization(vcom_client):
    """#1: client can be instantiated"""
    assert vcom_client is not None


def test_connectivity(vcom_client):
    """#2: API connectivity check"""
    assert vcom_client.test_connectivity()


def test_session_info(vcom_client):
    """#3: retrieve session info"""
    session = vcom_client.get_session()
    cache['session'] = session
    assert 'user' in session.get('data', {})


def test_systems_list(vcom_client):
    """#4: systems listing"""
    systems = vcom_client.get_systems()
    cache['systems'] = systems
    assert isinstance(systems, list)


def test_system_details(vcom_client):
    """#5: system details"""
    systems = cache.get('systems') or vcom_client.get_systems()
    assert systems, "No systems available"
    details = vcom_client.get_system_details(systems[0]['key'])
    cache['system_details'] = details
    assert isinstance(details, dict)


def test_technical_data(vcom_client):
    """#6: technical data extraction"""
    systems = cache.get('systems') or vcom_client.get_systems()
    assert systems, "No systems available"
    data = vcom_client.get_technical_data(systems[0]['key'])
    cache['technical_data'] = data
    assert isinstance(data, dict)


def test_inverters_list(vcom_client):
    """#7: list inverters"""
    systems = cache.get('systems') or vcom_client.get_systems()
    assert systems, "No systems available"
    inverters = vcom_client.get_inverters(systems[0]['key'])
    cache['inverters'] = inverters
    assert isinstance(inverters, list)


def test_inverter_details(vcom_client):
    """#8: inverter details"""
    systems = cache.get('systems') or vcom_client.get_systems()
    inverters = cache.get('inverters') or vcom_client.get_inverters(systems[0]['key'])
    if not inverters:
        pytest.skip("No inverter available for details test")
    details = vcom_client.get_inverter_details(systems[0]['key'], inverters[0]['id'])
    cache['inverter_details'] = details
    assert isinstance(details, dict)


def test_tickets_list(vcom_client):
    """#9: list tickets"""
    tickets = vcom_client.get_tickets()
    cache['tickets'] = tickets
    assert isinstance(tickets, list)


def test_priority_tickets(vcom_client):
    """#10: high priority tickets"""
    tickets = vcom_client.get_tickets(priority="high,urgent")
    cache['priority_tickets'] = tickets
    assert isinstance(tickets, list)


def test_ticket_details(vcom_client):
    """#11: ticket details"""
    tickets = cache.get('tickets') or vcom_client.get_tickets()
    if not tickets:
        pytest.skip("No ticket for details test")
    details = vcom_client.get_ticket_details(tickets[0]['id'])
    cache['ticket_details'] = details
    assert isinstance(details, dict)


def test_rate_limiting(vcom_client, caplog):
    """#12: rate limiting status"""
    caplog.set_level(logging.INFO)
    status = vcom_client.get_rate_limit_status()
    assert 'remaining_minute' in status
