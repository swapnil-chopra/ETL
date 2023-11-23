from src.utils.connectors.postgres import PostgreConnector
from src.utils.validators.component_validator import validate_system_configuration

connector = PostgreConnector()
BOOTSTRAP_SERVERS = ['192.168.56.101:9092']
SOURCE_SYSTEM_CONFIG = connector.get_data_from_component(
    'comp_source_system_config')

TARGET_SYSTEM_CONFIG = connector.get_data_from_component(
    'comp_target_system_config')

SOURCE_TO_TARGET_MAPPING = connector.get_data_from_component(
    'comp_source_to_target_field_mapping')


validation = validate_system_configuration(SOURCE_SYSTEM_CONFIG, 'source')
if validation.get('message') == 'Success':
    validation = validate_system_configuration(TARGET_SYSTEM_CONFIG, 'target')
else:
    raise Exception('Invalid configuration provided')
