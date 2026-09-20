# infrastructure/main.tf

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# 1. Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-fintech-data-prod"
  location = "East US" # Change to your preferred Azure region
}

# 2. ADLS Gen2 Storage Account
resource "azurerm_storage_account" "adls" {
  name                     = "stfintechdatalake" # Must be globally unique and lowercase
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Critical: Enables Hierarchical Namespace for ADLS Gen2
}

# 3. Databricks Workspace
resource "azurerm_databricks_workspace" "databricks" {
  name                        = "dbw-fintech-workspace"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = "premium" # Required for advanced networking and Unity Catalog
  managed_resource_group_name = "rg-fintech-databricks-managed"
}
