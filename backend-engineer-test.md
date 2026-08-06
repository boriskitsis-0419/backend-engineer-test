# Backend Engineer Take-Home Test: Data Processing & ETL Pipeline

## Overview

This take-home test evaluates your skills in designing and implementing a backend system that handles large-scale data processing, focusing on PostgreSQL, Python ETL, GraphQL API development, and workflow orchestration with Flyte.

**Time Expectation:** 4-6 hours (you may take up to 3 days to submit)

## Project Description

You are tasked with building a data processing pipeline for an e-commerce analytics platform. The system needs to:

1. Process large volumes of raw sales data (provided as CSV files)
2. Transform and load the data into a PostgreSQL database
3. Create a GraphQL API to query the processed data
4. Orchestrate the ETL workflow using Flyte

## Requirements

### 1. Database Design (PostgreSQL)

- Design a normalized schema for storing e-commerce sales data
- Include tables for: products, customers, orders, order_items, and product_categories
- Implement appropriate indexes for performance with large datasets
- Write SQL migrations to create this schema
- Design partitioning strategy for the orders table (assume millions of records)

### 2. ETL Pipeline (Python)

- Create a Python ETL pipeline that:
  - Reads the provided CSV files
  - Validates and cleans the data
  - Transforms according to business rules (described below)
  - Efficiently loads data into PostgreSQL using bulk operations
- Implement error handling and logging
- Use pandas or PySpark for data processing
- Write unit tests for critical components

### 3. GraphQL API

- Implement a GraphQL API with the following queries:
  - Get product sales by time period
  - Get customer purchase history
  - Get top-selling products by category
  - Get sales trends over time
- Include filtering, sorting, and pagination
- Implement at least one mutation (e.g., update product information)
- Consider query performance with large datasets

### 4. Workflow Orchestration (Flyte)

- Create a Flyte workflow that:
  - Orchestrates the ETL pipeline
  - Handles incremental data loads
  - Includes data quality checks
  - Provides monitoring and error handling
- Document how this workflow would be scheduled and monitored in production

## Data Transformation Rules

The raw data needs the following transformations:

1. Join product information with categories
2. Calculate revenue per order (price × quantity - discount)
3. Enrich customer data with derived attributes (e.g., total lifetime value)
4. Aggregate daily sales data by product and category
5. Generate dimension tables for time-based analysis

## Dataset

We provide the following CSV files (note: these are simulated datasets, approximately 1GB total):

- `products.csv` (100k records)
- `customers.csv` (1M records)
- `orders.csv` (5M records)
- `order_items.csv` (20M records)
- `product_categories.csv` (500 records)

Sample files with fewer records are provided for development purposes.

## Deliverables

Please provide:

1. A GitHub repository containing:
   - All code with documentation
   - SQL migrations
   - Docker Compose setup for local testing
   - README with setup and execution instructions

2. A brief technical document (max 2 pages) covering:
   - Your approach to database design
   - ETL pipeline architecture decisions
   - Query optimization strategies 
   - Scaling considerations
   - Production deployment considerations

3. A working demo with:
   - ETL pipeline processing the sample data
   - GraphQL endpoint with working queries
   - Simple Flyte workflow definition

## Evaluation Criteria

You will be evaluated on:

1. **Code quality and organization**
   - Clean, readable, and maintainable code
   - Proper error handling and testing
   - Documentation quality

2. **System design**
   - Database schema design and optimization
   - ETL pipeline efficiency and reliability
   - API design and query performance
   - Workflow orchestration approach

3. **Performance considerations**
   - Handling of large datasets
   - Query optimization techniques
   - Bulk loading strategies
   - Resource utilization

4. **Production readiness**
   - Error handling and logging
   - Deployment considerations
   - Monitoring approach
   - Incremental processing design

## Technical Specifications

- PostgreSQL 14+
- Python 3.9+
- GraphQL library of your choice (Apollo Server, Ariadne, etc.)
- Flyte for workflow orchestration
- Docker for containerization

## Bonus Points

- Implementation of a simple dashboard or visualization of the data
- Advanced PostgreSQL features (partitioning, materialized views)
- Performance benchmarks
- CI/CD pipeline configuration
- Comprehensive test coverage

## Submission

Please email a link to your GitHub repository to bob@vindecahealth.com and massy@vindecahealth.com along with your technical document.

Good luck!
