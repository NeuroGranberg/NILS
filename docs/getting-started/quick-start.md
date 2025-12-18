# Quick Start

This guide walks you through importing and processing your first dataset with NILS.

## Overview

A typical NILS workflow consists of:

1. **Import** - Load DICOM data into the system
2. **Classify** - Automatically identify sequence types
3. **Sort** - Organize files into a structured hierarchy
4. **Anonymize** - De-identify patient information
5. **Export** - Convert to BIDS format (optional)

## Step 1: Access the Dashboard

After starting NILS, navigate to `http://localhost:5173`. You'll see the main dashboard:

![Dashboard](../assets/dashboard-placeholder.png)

## Step 2: Import DICOM Data

1. Click **Import** in the sidebar
2. Specify the path to your DICOM folder
3. Click **Start Import**

NILS will scan the folder and extract metadata from all DICOM files.

!!! tip "Supported Formats"
    NILS supports standard DICOM files (.dcm) and files without extensions.

## Step 3: Review Classification

After import, NILS automatically classifies each series:

- Navigate to **Database** to view all imported series
- Each series shows its detected sequence type (T1w, T2w, FLAIR, etc.)
- Review and correct any misclassifications if needed

## Step 4: Create a Cohort

Cohorts help organize subjects for processing:

1. Go to **Cohorts** → **New Cohort**
2. Enter a name and description
3. Add subjects from the database
4. Save the cohort

## Step 5: Run Sorting Pipeline

1. Select your cohort
2. Click **Sort** in the pipeline steps
3. Configure output directory
4. Start the sorting job

Monitor progress in the **Jobs** tab.

## Step 6: Anonymization (Optional)

To de-identify data:

1. Select sorted data
2. Click **Anonymize**
3. Review the anonymization profile
4. Run the anonymization job

## Step 7: BIDS Export (Optional)

Export to BIDS format:

1. Select anonymized data
2. Click **BIDS Export**
3. Configure BIDS metadata
4. Export

## Next Steps

- Learn about [Classification](../classification/index.md)
- Configure [Anonymization](../cohort/anonymization.md)
- Explore the [Sorting Workflow](../cohort/sorting.md)
