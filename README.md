# Azure-Application-Insights-Data-Handling
Worked end to end on this project for Diageo using Azure to extract data from KQL Database and move it to production

The Big Picture
Built a system that:

Takes application usage data (from Application Insights/KQL DB)

Processes 3 years of this data (handling large volumes by processing day-by-day)

Stores the final results in a structured format

Step-by-Step Process
Data Collection (Azure Data Factory Pipeline)

Since there's too much data to process all at once (3 years worth), it processes one day at a time in a loop

**Data Landing Zone**

The raw data first lands in a temporary storage area called "/Landed/AppInsights/"

Think of this like a loading dock where packages (data) arrive before being sorted

Data Transformation (VSCode Notebook)
The notebook does the heavy lifting with these steps:

a) Setting Up the Workshop

Creates a Spark session (this is like starting up a powerful data processing engine)

Imports tools needed for the job (like getting the right wrenches from a toolbox)

b) Reading the Raw Data

Opens the JSON files from the landing zone

The data comes in nested packages (like Russian dolls) so it:

Unpacks the outer layers (explode function)

Extracts the column names and rows

Flattens everything into a neat table format

c) Transforming the Data

Adds a "Source" label to all records (marking them as "AppInsights")

Removes unnecessary columns (like throwing away packing materials)

Handles two scenarios:

Full Load: Takes all the new data as-is

Incremental Load: Carefully merges new data with existing data, keeping only what's needed

d) Quality Control

Runs validation checks (through run_actions)

Makes sure all data fits the expected format (using fact_appinsights_output)

Final Storage

The cleaned data gets stored in "/Presented/AppInsights/schema_version=1"

Uses Delta format (a special way to store data that keeps history and allows easy updates)

Organizes data by "Source" (like filing documents in labeled folders)

Can either completely replace old data or intelligently merge with it

Key Features Highlight
Handles Huge Data: Processes 3 years of data safely by working day-by-day

Flexible Loading: Can do complete refreshes or just add new data

Data Quality: Checks and enforces data standards

Tracking: Keeps track of where data came from ("AppInsights" source tag)

Imagine this like a sophisticated mail room system that:

Receives packages (raw data)

Unpacks and inspects contents

Throws away unnecessary packaging

Labels and organizes the important contents

Files them in the right cabinets (storage)

Can either replace old files or update them as needed

The system is designed to handle massive amounts of application usage data efficiently while ensuring data quality and organization.

