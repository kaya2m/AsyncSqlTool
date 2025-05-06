# Async SQL Tool

A robust application for synchronizing data between different database systems and SQL Server, with a focus on data mapping, transformation, and scheduling capabilities.

## Overview

Async SQL Tool is a Blazor-based web application designed to help database administrators, developers, and data analysts efficiently manage data transfer operations between various database systems and SQL Server. The application provides a user-friendly interface for executing queries, mapping column data types, and scheduling automated data synchronization tasks.

## Features

### Database Connection Management
- Connect to multiple database sources including SQL Server, SAP HANA, and others
- Secure connection string storage
- Connection testing capabilities
- Active/inactive status tracking

### Query Management
- Create, save, and edit SQL queries
- Preview query results before execution
- Execute queries with different result set limits
- Export query results to Excel

### Advanced Column Mapping
- Define precise column mappings between source and target databases
- Specify exact SQL data types including length, precision, and scale parameters
- Support for primary key definition
- Nullable column configuration

### Target Table Operations
- Automatic table creation based on query results and mappings
- Table schema updates when structure changes
- View, truncate, or drop target tables
- Inspect table contents directly from the UI

### Data Synchronization
- Merge data based on primary key columns
- Bulk insert capabilities for efficient data loading
- Transaction support for data integrity

### User Interface
- Modern, responsive design
- Detailed feedback on operations
- Clear error reporting
- Interactive data grid with filtering and sorting capabilities

## Technical Details

### Architecture
- Built with ASP.NET Core and Blazor Server
- Entity Framework Core for data access
- DevExpress UI components for rich user interface
- Service-based architecture for maintainability

### Data Type Handling
- Precise mapping between different database system data types
- Support for preserving column properties (length, precision, scale)
- Automatic data conversion for compatible types

### Security
- SQL injection prevention
- Secure connection string management
- Input validation for table and column names

## Getting Started

### Prerequisites
- .NET 6.0 or higher
- SQL Server (for the application database)
- Appropriate database drivers for source systems

### Configuration
1. Configure the connection string for the application database in `appsettings.json`
2. Set up initial database connections through the UI
3. Create queries and column mappings based on your data requirements

### Best Practices
- Test connections before creating queries
- Preview data before executing to SQL Server
- Use column mappings to ensure proper data type conversion
- Set primary keys for efficient merge operations
- Add descriptive names and descriptions for saved queries

## Usage Examples

### Basic Data Transfer
1. Select a database connection
2. Write a query to extract data
3. Specify a target table name
4. Define column mappings (optional)
5. Execute the query to transfer data

### Scheduled Synchronization
1. Save a query with appropriate mappings
2. Enable scheduling for the query
3. Define the schedule expression
4. The system will automatically execute the query based on the schedule

### Schema Management
1. Use the column mapping UI to define precise SQL data types
2. Specify length, precision, and scale as needed
3. Mark primary key columns and nullable properties
4. Execute to create or update the target table structure

## Troubleshooting

### Common Issues
- **Connection errors**: Verify connection strings and network access
- **Data type conversion issues**: Use column mappings to define explicit type conversions
- **Performance problems**: Consider using key columns for merge operations with large datasets

### Logging
The application logs detailed information about operations, which can be reviewed for troubleshooting purposes.

## Contributing

Contributions to the Async SQL Tool are welcome. Please ensure your code follows the existing patterns and includes appropriate tests.
