from azure.storage.blob import ContainerClient, ContentSettings
import yaml

class AzureContainer:

    def load_config(self) -> dict:
        """Loads the config file"""

        with open('azure_storage/azure_config.yaml', 'r') as yaml_file:
            config = yaml.load(yaml_file, Loader=yaml.FullLoader)
        return config

    def get_container_client(self) -> ContainerClient:
        """Gets the container client"""

        azure_config = self.load_config()
        container_client = ContainerClient.from_connection_string(
            azure_config['azure_storage_connectionstring'], azure_config['dataset_nyc_container_name'])
        return container_client
    
    def get_azure_storage_path(self) -> str:
        """Gets the azure storage path"""

        azure_config = self.load_config()
        azure_storage_path = azure_config['azure_storage_path']
        return azure_storage_path
    
    def get_azure_content_settings(self) -> ContentSettings:
        """Gets the azure content settings"""

        azure_config = self.load_config()
        content_settings = ContentSettings(
            content_type=azure_config['content_type'])
        return content_settings
