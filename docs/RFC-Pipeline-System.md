# Pipeline System Architecture

## Overview

The pipeline system provides a composable architecture for processing forkable data streams, designed specifically for blockchain indexing and similar applications where data can branch and require rollback capabilities.

## Core Concepts

### Data Model

All data flows through the system as typed batches with references for ordering and fork detection:

```typescript
// Base data interface
export interface Data<I = unknown, R = unknown> {
    item: I    // The actual data content
    ref: R     // Reference for ordering/comparison
}

// Batch of data items with metadata
export interface DataBatch<D extends Data = Data> {
    readonly data: DataItem<D>[]           // Array of data items
    readonly finalizedHead?: DataRef<D>    // Last finalized reference
    readonly head: DataRef<D>              // Current head reference  
}

// Fork information when divergence is detected
export interface DataFork<D extends Data = Data> {
    readonly heads: DataRef<D>[]           // All competing head references
}

// Reference comparison interface
export interface DataRefer<D extends Data = Data> {
    get(ref: DataItem<D>): DataRef<D>                           // Get ref from item
    compare(a: DataRef<D>, b: DataRef<D>): 'ls' | 'eq' | 'gt' | 'fk'  // Compare refs
}
```

### Component Architecture

The system consists of three main components:

1. **DataSource**: Provides data with internal reader management
2. **DataTarget**: Consumes data and handles fork resolution  
3. **DataDuplex**: Combines target and source for transformations

## Core Interfaces

```typescript
// Data source - provides data, handles offset management internally
export interface DataSource<T extends Data> {
    ref: DataRefer<T>                                        // Reference system
    read(offset?: DataRef<T>): Promise<DataBatch<T> | null>  // Read next batch
    close(): Promise<void>                                   // Clean up resources
    pipeThrough<U extends Data>(duplex: DataDuplex<T, U>): DataSource<U>  // Chain operations
    pipeTo(target: DataTarget<T>): Promise<void>             // Execute to target
}

// Data target - receives data, handles forks
export interface DataTarget<T extends Data> {
    head(): Promise<DataRef<T> | undefined>                       // Get current head
    write(batch: DataBatch<T>, ref: DataRefer<T>): Promise<void>  // Process batch
    fork(fork: DataFork<T>, ref: DataRefer<T>): Promise<DataRef<T> | undefined>  // Handle fork
    close(): Promise<void>                                        // Clean up
}

// Data duplex - combines target and source for transformations
export interface DataDuplex<T extends Data, U extends Data> {
    target: DataTarget<T>                                         // Input target
    source: DataSource<U>                                         // Output source
}
```

## Reader and Writer Interfaces

```typescript
// Data reader interface - provides data with lifecycle management
export interface DataReader<T extends Data> {
    read(): Promise<DataBatch<T> | null>
    close(): Promise<void>
}

// Data writer interface - consumes data with lifecycle management
export interface DataWriter<T extends Data> {
    offset?: DataRef<T>
    write(batch: DataBatch<T>, ref: DataRefer<T>): Promise<void>
    fork(fork: DataFork<T>, ref: DataRefer<T>): Promise<DataRef<T> | undefined>
    close(): Promise<void>
}
```

## Factory Functions

```typescript
// Create a data source from options
export function source<T extends Data, F extends boolean>(
    options: DataSourceOptions<T, F>
): DataSource<T>

// Create a data target from options  
export function target<T extends Data, F extends boolean>(
    options: DataTargetOptions<T, F>
): DataTarget<T>

// Options interfaces
export interface DataSourceOptions<T extends Data, F extends boolean> {
    reader: (offset?: DataRef<T>) => DataReader<T>
    ref: DataRefer<T>
    finalized?: F
}

export interface DataTargetOptions<T extends Data, F extends boolean> {
    writer: () => DataWriter<T>
    finalized?: F
}

// Factory utilities
export const DataReader = {
    fromAsync: <T extends Data>(iterator: AsyncIterator<DataBatch<T>>) => DataReader<T>
}
```

## Pipeline Operations

```typescript
// Transform data batches
export function transformer<T extends Data, U extends Data>(
    fn: (batch: DataBatch<T>) => Promise<DataBatch<U>>,
    ref: DataRefer<U>
): DataDuplex<T, U>

// Finalize data (handle partial batch accumulation)  
export function finalizer<T extends Data>(
    ref: DataRefer<T>,
    options?: { throwOnFork?: boolean }
): DataDuplex<T, T>

// Execute pipeline with automatic fork handling
export async function pipe<T extends Data>(
    source: DataSource<T>, 
    target: DataTarget<T>
): Promise<void>
```

## Usage Patterns

### Basic Pipeline Construction

```typescript
// Create source with reader factory
const dataSource = source({
  reader: (offset?) => createReader(offset),
  ref: blockRef
})

// Create target with writer factory  
const dataTarget = target({
  writer: () => createWriter()
})

// Execute pipeline
await pipe(dataSource, dataTarget)
```

### Transformation Pipeline

```typescript
source({
  reader: readerFactory,
  ref: inputRef
})
  .pipeThrough(transformer(processor, outputRef))
  .pipeThrough(finalizer(outputRef))
  .pipeTo(target({
    writer: writerFactory
  }))
```

### Multi-Output Fanout

```typescript
// Fan-out pattern for multiple consumers
const processedSource = source({
  reader,
  ref: inputRef
}).pipeThrough(transformer(processor, outputRef))

await Promise.all([
  processedSource.pipeTo(target({ writer: primaryWriter })),
  processedSource.pipeTo(target({ writer: analyticsWriter })), 
  processedSource.pipeTo(target({ writer: auditWriter }))
])
```

## Fork Handling

The system uses exception-based fork detection:

1. Normal processing continues until a fork is detected
2. `ForkException` interrupts the flow with recovery information  
3. Target handles fork resolution and provides restart offset
4. Source automatically resumes from the recovery point

```typescript
// Fork recovery flow
1. Source reads data → Target processes
2. Fork detected → ForkException thrown
3. Target resolves fork → Returns recovery offset
4. Source restarts → Processing continues
```

## Implementation Details

### State Management

- **DataSource**: Manages reader lifecycle and offset tracking internally
- **DataTarget**: Tracks current head position and manages writer state
- **Transformations**: Use buffer-based approach with proper cleanup

### Reference Flow

- Each operation receives its input ref from the previous operation
- Each operation only needs to specify its output ref  
- References flow naturally from source to target through the pipeline

### Error Handling

- Fork exceptions carry sufficient context for recovery decisions
- Automatic restart from resolved points
- Proper resource cleanup on errors

## Best Practices

1. **Use Options Pattern**: Always use structured options for factory functions
2. **Manage References**: Let references flow naturally, only specify output refs in transformations
3. **Resource Cleanup**: Always implement proper close() methods
4. **State Isolation**: Keep transformation state isolated and cleanable
5. **Error Boundaries**: Handle fork exceptions at appropriate pipeline boundaries

## Performance Considerations

- Reader and writer factories are called as needed for lifecycle management
- Internal buffering in transformations should be minimal
- Offset management is optimized for restart scenarios
- Resource cleanup prevents memory leaks in long-running pipelines