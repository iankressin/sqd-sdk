# RFC: Pipeline Architecture for Forkable Data Processing

## Problem

Traditional data pipeline architectures assume linear data flow, making them unsuitable for non-linear, forkable data systems like blockchain indexing, Git branch processing, and distributed ledgers. These systems require handling data that can branch (fork) and later converge, with automatic recovery capabilities when conflicts are detected.

## Duration

January 2025 - Ongoing development

## Current State

**Approved and Implemented** - Core architecture complete, ready for integration

## Proposers

- Development Team (sqd-sdk pipeline system)

## Detail

### Architectural Requirements

1. **Fork-Aware Data Flow**: Support branching and merging data streams
2. **Automatic Recovery**: Transparent handling of fork detection and rollback
3. **Reference-Based Ordering**: Track data lineage through cryptographic or logical references
4. **Composable Operations**: Modular pipeline components that can be chained
5. **Transparent Complexity**: Simple interface hiding complex fork mechanics

## Proposal

### Core Data Types

```typescript
// Base data interface - each item has content and a reference
export interface Data<I = unknown, R = unknown> {
    item: I    // The actual data content
    ref: R     // Reference for ordering/comparison
}

// Batch of data items with metadata
export interface DataBatch<D extends Data = Data> {
    readonly data: DataItem<D>[]           // Array of data items
    readonly finalizedHead?: DataRef<D>    // Last finalized reference
    readonly head: DataRef<D>              // Current head reference  
    readonly ref: DataRefer<D>             // Reference comparison utility
}

// Fork information when divergence is detected
export interface DataFork<D extends Data = Data> {
    readonly heads: DataRef<D>[]           // All competing head references
    readonly ref: DataRefer<D>             // Reference comparison utility
}

// Reference comparison interface
export interface DataRefer<D extends Data = Data> {
    get(ref: DataItem<D>): DataRef<D>                           // Get ref from item
    compare(a: DataRef<D>, b: DataRef<D>): 'ls' | 'eq' | 'gt' | 'fk'  // Compare refs
}
```

### Core Interfaces

```typescript
// Data source - provides data, handles offset management internally
export interface DataSource<T extends Data> {
    read(offset?: DataRef<T>): Promise<DataBatch<T> | null>  // Read next batch
    close(): Promise<void>                                   // Clean up resources
    pipeThrough<U extends Data>(operation: Pipeline<T, U>): DataSource<U>  // Chain operations
    pipeTo(target: DataTarget<T>): Promise<void>             // Execute to target
}

// Data target - receives data, handles forks
export interface DataTarget<T extends Data> {
    write(batch: DataBatch<T>): Promise<void>                      // Process batch
    fork(fork: DataFork<T>): Promise<DataRef<T> | undefined>       // Handle fork
    close(): Promise<void>                                         // Clean up
}

// Pipeline operation - transforms one source into another
export interface Pipeline<T extends Data, U extends Data> {
    run(source: DataSource<T>): DataSource<U>                     // Apply transformation
}
```

### Core Reader and Writer Interfaces

```typescript
// Data reader interface - provides data with lifecycle management
export interface DataReader<T extends Data> {
    read(): Promise<DataBatch<T> | null>
    close(): Promise<void>
}

// Data writer interface - consumes data with lifecycle management
export interface DataWriter<T extends Data> {
    write(batch: DataBatch<T>): Promise<void>
    fork(fork: DataFork<T>): Promise<DataRef<T> | undefined>
    close(): Promise<void>
}
```

### Factory Functions

```typescript
// Create a data source from a reader factory
export function source<T extends Data>(
    reader: (offset?: DataRef<T>) => DataReader<T>
): DataSource<T>

// Create a data target from a writer factory
export function target<T extends Data>(
    writer: () => DataWriter<T>
): DataTarget<T>

// Factory utilities
export const DataReader = {
    fromAsync: <T extends Data>(iterator: AsyncIterator<DataBatch<T>>) => DataReader<T>
}
```

### Pipeline Operations

```typescript
// Transform data batches
export function transform<T extends Data, U extends Data>(
    fn: (batch: DataBatch<T>) => Promise<DataBatch<U>>
): Pipeline<T, U>

// Finalize data (handle partial batch accumulation)
export function finalize<T extends Data>(
    options?: { throwOnFork?: boolean }
): Pipeline<T, T>

// Execute pipeline with automatic fork handling
export async function pipe<T extends Data>(
    source: DataSource<T>, 
    target: DataTarget<T>
): Promise<void>
```

## Architecture Overview

The pipeline architecture consists of three core components that work together to handle forkable data streams:

### Component Separation

1. **DataSource**: Provides data with internal reader management
2. **DataTarget**: Consumes data and handles fork resolution  
3. **Pipeline**: Transforms data between sources and targets

### Data Flow Model

```
DataSource → Pipeline Operations → DataTarget
     ↑              ↓                ↓
Reader Factory   Transform        Fork Handler
     ↑              ↓                ↓  
Offset Mgmt    Batch Processing   Recovery Logic
```

### Fork Handling Strategy

The architecture uses an exception-based approach for fork detection:
- Normal processing continues until a fork is detected
- `ForkException` interrupts the flow with recovery information  
- Target handles fork resolution and provides restart offset
- Source automatically resumes from the recovery point

## Architectural Patterns

### Source-Transform-Target Pattern
```typescript
source(readerFactory)
  .pipeThrough(transform(processor))
  .pipeThrough(finalize())
  .pipeTo(target(writerFactory))
```

### Multi-Output Architecture
```typescript
// Fan-out pattern for multiple consumers
const processedSource = source(reader).pipeThrough(transform(processor))

await Promise.all([
  processedSource.pipeTo(primaryTarget),
  processedSource.pipeTo(analyticsTarget), 
  processedSource.pipeTo(auditTarget)
])
```

### Fork Recovery Flow
```
1. Source reads data → Target processes
2. Fork detected → ForkException thrown
3. Target resolves fork → Returns recovery offset
4. Source restarts → Processing continues
```

## Architectural Principles

### 1. **Separation of Concerns**
- **DataSource**: Handles data provision and offset management
- **DataTarget**: Manages data consumption and fork resolution
- **Pipeline**: Focuses purely on data transformation

### 2. **Exception-Driven Fork Detection**
- Normal processing flow until fork detected
- Exceptions carry recovery context
- Automatic restart from recovery points

### 3. **Transparent Complexity**
- Simple public APIs hide internal state management
- Offset handling abstracted from user code
- Reader lifecycle managed internally

### 4. **Factory-Based Construction**
- Consistent creation patterns for all components
- Dependency injection through factory functions
- Clean separation of configuration and runtime

## Key Architectural Decisions

### 1. **Three-Component Architecture**
- Separate DataSource, DataTarget, and Pipeline concerns
- Clear interfaces with minimal surface area
- Composable through method chaining

### 2. **Reference-Based Data Model**
- Each data item carries a comparable reference
- References enable fork detection and ordering
- Support for both finalized and tentative data

### 3. **Exception-Driven Fork Handling**  
- Exceptions interrupt normal flow when forks detected
- Carry sufficient context for recovery decisions
- Enable automatic restart from resolved points

### 4. **Internal State Management**
- Offset and reader lifecycle hidden from users
- Factory functions encapsulate complexity
- Simple public APIs with transparent behavior

---

This RFC defines the core architecture for processing forkable data streams in distributed systems, prioritizing simplicity and automatic fork handling for blockchain and similar applications. 