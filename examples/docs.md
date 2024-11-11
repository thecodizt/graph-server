# Docs

::: mermaid
graph TD
    %% Define node styles
    classDef staticNode fill:#f9f,stroke:#333,stroke-width:2px
    classDef dynamicNode fill:#9ff,stroke:#333,stroke-width:2px
    
    %% Add nodes
    BusinessUnit[BusinessUnit]:::staticNode
    ProductFamily[ProductFamily]:::staticNode
    ProductOffering[ProductOffering]:::staticNode
    Facility[Facility]:::dynamicNode
    Parts[Parts]:::dynamicNode
    Warehouse[Warehouse]:::dynamicNode
    Supplier[Supplier]:::staticNode
    
    %% Add relationships
    ProductFamily -->|ProductFamiliesToBusinessUnit| BusinessUnit
    ProductOffering -->|ProductOfferingsToProductFamilies| ProductFamily
    Facility -->|FacilityToProductOfferings| ProductOffering
    Parts -->|PartsToFacility| Facility
    Facility -->|FacilityToParts| Parts
    Warehouse -->|WarehouseToParts| Parts
    Warehouse -->|WarehouseToSubassembly| Parts
    Warehouse -->|WarehouseToProductOfferings| ProductOffering
    Supplier -->|SupplierToWarehouse| Warehouse
:::