# QuickSight Backup Process Flow Diagram

## Overview Flow Diagram

```mermaid
flowchart TD
    A[User Executes CLI Command] --> B[Load Configuration File]
    B --> C{Configuration Valid?}
    C -->|No| D[Exit with Error]
    C -->|Yes| E[Initialize AWS Clients]
    E --> F{AWS Credentials Valid?}
    F -->|No| G[Exit with Credentials Error]
    F -->|Yes| H{Dry Run Mode?}
    H -->|Yes| I[Validate Configuration & Exit]
    H -->|No| J{Select Backup Mode}
    
    J -->|users-only| K[Execute User/Group Backup]
    J -->|assets-only| L[Execute Asset Bundle Backup]
    J -->|full| M[Execute Full Backup]
    
    M --> K
    K --> N[Retrieve QuickSight Users]
    N --> O[Retrieve QuickSight Groups]
    O --> P[Store Users in DynamoDB]
    P --> Q[Store Groups in DynamoDB]
    Q -->|full mode| L
    Q -->|users-only| AA[Generate Reports]
    
    L --> R[Discover QuickSight Assets]
    R --> S{Filter FILE Datasets?}
    S -->|Yes| T[Exclude FILE Datasets from List]
    S -->|No| U[Keep All Assets]
    T --> V[Group Assets by Type]
    U --> V
    V --> W[Start Asset Bundle Export Job]
    W --> X{Export Job Status}
    X -->|IN_PROGRESS| Y[Wait & Poll Status]
    Y --> X
    X -->|SUCCESSFUL| Z[Download Asset Bundle]
    X -->|FAILED| BB{Retry Available?}
    Z --> AA1[Upload to S3 with Date Prefix]
    AA1 --> AA
    
    BB -->|Yes| CC[Increment Retry Counter]
    CC --> W
    BB -->|No| DD[Log Error & Continue]
    DD --> AA
    
    AA[Generate Backup Manifest]
    AA --> EE[Generate Backup Report]
    EE --> FF[Save Output Files]
    FF --> GG[Display Summary]
    GG --> HH[Exit with Status Code]

    style A fill:#e1f5fe
    style D fill:#ffebee
    style G fill:#ffebee
    style I fill:#f3e5f5
    style BB fill:#fff3e0
    style DD fill:#ffebee
    style HH fill:#e8f5e8
```

## Detailed Error Handling Flow

```mermaid
flowchart TD
    A[Operation Starts] --> B{Error Occurs?}
    B -->|No| C[Operation Successful]
    B -->|Yes| D[Capture Error Details]
    D --> E{Error Type}
    
    E -->|Rate Limiting| F[Apply Exponential Backoff]
    E -->|Permission Error| G[Log Detailed Error & Stop]
    E -->|Network Error| H[Check Retry Count]
    E -->|Resource Not Found| I[Log Warning & Continue]
    E -->|Configuration Error| J[Exit with Config Error]
    
    F --> K{Retry Count < 3?}
    H --> K
    K -->|Yes| L[Wait for Backoff Period]
    K -->|No| M[Log Final Error]
    L --> N[Increment Retry Count]
    N --> O[Retry Operation]
    O --> B
    
    G --> P[Update Error Report]
    I --> P
    M --> P
    P --> Q[Continue with Next Resource]
    
    C --> R[Update Success Report]
    R --> S[Continue Processing]
    
    style A fill:#e1f5fe
    style C fill:#e8f5e8
    style G fill:#ffebee
    style J fill:#ffebee
    style M fill:#ffebee
    style P fill:#fff3e0
```

## Asset Discovery and Filtering Flow

```mermaid
flowchart TD
    A[Start Asset Discovery] --> B[List All DataSources]
    B --> C[List All DataSets]
    C --> D[List All Analyses]
    D --> E[List All Dashboards]
    
    E --> F{Filter FILE DataSets?}
    F -->|Yes| G[Identify FILE DataSets]
    F -->|No| H[Keep All DataSets]
    
    G --> I[Remove FILE DataSets from List]
    I --> J[Log Excluded DataSets]
    J --> K[Group Remaining Assets by Type]
    H --> K
    
    K --> L{Assets Found?}
    L -->|No| M[Log No Assets Warning]
    L -->|Yes| N[Create Export Job Request]
    
    M --> O[Skip Asset Export]
    N --> P[Validate Asset Dependencies]
    P --> Q{Dependencies Valid?}
    Q -->|No| R[Log Dependency Warning]
    Q -->|Yes| S[Proceed with Export]
    R --> S
    
    S --> T[Submit Export Job]
    O --> U[Continue with Next Asset Type]
    T --> V[Return Job ID]
    
    style A fill:#e1f5fe
    style M fill:#fff3e0
    style R fill:#fff3e0
    style V fill:#e8f5e8
```

## S3 Upload Process Flow

```mermaid
flowchart TD
    A[Asset Bundle Ready] --> B[Generate S3 Key with Date Prefix]
    B --> C[Calculate File Size]
    C --> D{File Size > 100MB?}
    D -->|Yes| E[Use Multipart Upload]
    D -->|No| F[Use Standard Upload]
    
    E --> G[Initialize Multipart Upload]
    G --> H[Upload Parts in Parallel]
    H --> I{All Parts Uploaded?}
    I -->|No| J[Retry Failed Parts]
    J --> I
    I -->|Yes| K[Complete Multipart Upload]
    
    F --> L[Upload File to S3]
    L --> M{Upload Successful?}
    M -->|No| N{Retry Count < 3?}
    N -->|Yes| O[Wait & Retry]
    O --> F
    N -->|No| P[Log Upload Error]
    
    K --> Q[Verify Upload]
    M -->|Yes| Q
    Q --> R{Verification Successful?}
    R -->|Yes| S[Log Success & Update Manifest]
    R -->|No| T[Log Verification Error]
    
    P --> U[Update Error Report]
    T --> U
    S --> V[Continue with Next Asset]
    U --> V
    
    style A fill:#e1f5fe
    style S fill:#e8f5e8
    style P fill:#ffebee
    style T fill:#ffebee
```

## DynamoDB Storage Flow

```mermaid
flowchart TD
    A[User/Group Data Retrieved] --> B{Table Exists?}
    B -->|No| C[Create DynamoDB Table]
    B -->|Yes| D[Validate Table Schema]
    
    C --> E{Table Creation Successful?}
    E -->|No| F[Log Table Creation Error]
    E -->|Yes| G[Wait for Table Active]
    
    D --> H{Schema Valid?}
    H -->|No| I[Log Schema Warning]
    H -->|Yes| J[Prepare Batch Write Items]
    
    G --> J
    I --> J
    J --> K[Execute Batch Write]
    K --> L{Write Successful?}
    L -->|No| M{Retry Available?}
    M -->|Yes| N[Apply Backoff & Retry]
    N --> K
    M -->|No| O[Log Write Error]
    
    L -->|Yes| P[Verify Data Written]
    P --> Q{Verification Successful?}
    Q -->|Yes| R[Log Success & Update Count]
    Q -->|No| S[Log Verification Warning]
    
    F --> T[Update Error Report]
    O --> T
    S --> T
    R --> U[Continue Processing]
    T --> U
    
    style A fill:#e1f5fe
    style R fill:#e8f5e8
    style F fill:#ffebee
    style O fill:#ffebee
```

## Key Decision Points

### 1. Backup Mode Selection
- **Full**: Execute both user/group and asset bundle backups
- **Users-only**: Execute only user/group backup to DynamoDB
- **Assets-only**: Execute only asset bundle backup to S3

### 2. Asset Filtering
- **FILE Datasets**: Always excluded from asset bundle exports per requirements
- **Dependency Validation**: Check for circular dependencies before export
- **Permission Validation**: Verify access to assets before including in export

### 3. Error Handling Strategies
- **Rate Limiting**: Exponential backoff with jitter (max 3 retries)
- **Network Errors**: Retry with backoff (max 3 retries)
- **Permission Errors**: Log and continue with accessible resources
- **Resource Not Found**: Log warning and continue

### 4. Retry Logic
- **API Calls**: Maximum 3 retries with exponential backoff
- **File Uploads**: Maximum 3 retries for network-related failures
- **Database Operations**: Maximum 3 retries for throttling errors

### 5. Output Generation
- **Manifest File**: Always generated (JSON format)
- **Report File**: Always generated (human-readable text)
- **Log Files**: Generated based on configuration
- **Progress Indicators**: Disabled in batch mode (`--no-progress`)

## Flow Execution Times

| Operation | Typical Duration | Factors Affecting Duration |
|-----------|------------------|---------------------------|
| Configuration Loading | < 1 second | File size, validation complexity |
| AWS Client Initialization | 1-5 seconds | Network latency, credential validation |
| User/Group Retrieval | 5-30 seconds | Number of users/groups, API rate limits |
| Asset Discovery | 10-60 seconds | Number of assets, API rate limits |
| Asset Bundle Export | 2-15 minutes | Asset complexity, dependencies |
| DynamoDB Storage | 5-30 seconds | Data volume, table throughput |
| S3 Upload | 30 seconds - 5 minutes | File size, network speed |
| Report Generation | < 5 seconds | Number of resources processed |

## Error Recovery Scenarios

### Scenario 1: Partial Backup Failure
- **Trigger**: Some assets fail to export
- **Response**: Continue with successful assets, log failures
- **Output**: Partial success report with detailed error information

### Scenario 2: Network Interruption
- **Trigger**: Network connectivity lost during operation
- **Response**: Retry with exponential backoff
- **Fallback**: Save progress and allow resume (future enhancement)

### Scenario 3: Permission Changes
- **Trigger**: Permissions revoked during backup
- **Response**: Log detailed error with remediation steps
- **Output**: Clear error message with required permissions

### Scenario 4: Storage Quota Exceeded
- **Trigger**: DynamoDB or S3 storage limits reached
- **Response**: Log storage error with usage information
- **Recommendation**: Suggest cleanup or quota increase